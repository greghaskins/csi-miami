//go:build smoke

package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/observe"
	"github.com/greghaskins/csi-miami/internal/transport"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// The node command against the real kernel: stage and publish a local volume
// through the CSI socket with real binds, scrape /metrics, then take it down.
// hack/smoke.sh runs this as root under `unshare --mount --propagation
// private`; it fails, never skips, when it is not.

const smokeVolume = "pvc-00000000-0000-4000-8000-0000000000a1"

func scrape(t *testing.T, addr string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/metrics", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func requireLines(t *testing.T, body string, lines ...string) {
	t.Helper()
	for _, want := range lines {
		if !strings.Contains(body, want+"\n") {
			t.Errorf("/metrics lacks the line %q", want)
		}
	}
}

func TestSmokeNodeStagesPublishesAndReportsMetrics(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Fatal("this test mounts: run it as root, through the script that calls it")
	}
	self, err := os.Readlink("/proc/self/ns/mnt")
	if err != nil {
		t.Fatal(err)
	}
	if parent := os.Getenv("SMOKE_PARENT_MNT_NS"); parent == "" || parent == self {
		t.Fatalf("not in a mount namespace of our own (%s, from %s): run under `unshare --mount --propagation private`, as the script does", self, parent)
	}

	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for range 20 {
			ms, err := observe.ReadMountinfo()
			if err != nil {
				return
			}
			under := observe.Table(ms).Under(base)
			if len(under) == 0 {
				return
			}
			for _, m := range under {
				_ = unix.Unmount(m.MountPoint, unix.MNT_DETACH)
			}
		}
	})
	host := filepath.Join(base, "export", "data")
	if err := os.MkdirAll(host, 0o755); err != nil {
		t.Fatal(err)
	}
	sock := filepath.Join(base, "csi.sock")
	cfg := nodeConfig{
		endpoint: "unix://" + sock, nodeID: "smoke-node", kubeletDir: filepath.Join(base, "kubelet"),
		transportsRoot: filepath.Join(base, "miami"), metricsAddr: "127.0.0.1:0",
		interval: time.Hour, deadline: 10 * time.Second,
	}.withDefaults()

	ctx, cancel := context.WithCancel(context.Background())
	var logs bytes.Buffer
	done := make(chan error, 1)
	ready := make(chan string, 1)
	go func() {
		done <- serveNodeWith(ctx, cfg, mount.New(cfg.deadline), nil, slog.New(slog.NewTextHandler(&logs, nil)), func(a string) { ready <- a })
	}()
	var addr string
	select {
	case addr = <-ready:
	case err := <-done:
		t.Fatalf("the node stopped: %v\n%s", err, logs.String())
	case <-time.After(30 * time.Second):
		t.Fatal("the node did not start")
	}
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("the node stopped with %v", err)
		}
	}()

	conn, err := grpc.NewClient("unix:"+sock, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	node := csi.NewNodeClient(conn)
	callCtx, callCancel := context.WithTimeout(ctx, 60*time.Second)
	defer callCancel()

	spec := transport.Spec{Kind: transport.KindLocal, Path: host}
	vc := volume.Context{Kind: volume.KindDirectory, Transport: spec, Subdir: "team"}.Encode()
	volID := "directory:" + smokeVolume
	stage := filepath.Join(cfg.stageRoot, "x", "globalmount")
	target := filepath.Join(base, "kubelet", "pods", "p1", "volumes", "mount")
	capability := &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER},
	}

	if _, err := node.NodeStageVolume(callCtx, &csi.NodeStageVolumeRequest{
		VolumeId: volID, StagingTargetPath: stage, VolumeCapability: capability, VolumeContext: vc,
	}, grpc.WaitForReady(true)); err != nil {
		t.Fatalf("NodeStageVolume: %v\n%s", err, logs.String())
	}
	if _, err := node.NodePublishVolume(callCtx, &csi.NodePublishVolumeRequest{
		VolumeId: volID, StagingTargetPath: stage, TargetPath: target, VolumeCapability: capability,
	}); err != nil {
		t.Fatalf("NodePublishVolume: %v\n%s", err, logs.String())
	}

	// The node made the volume's directory, and a write through the publish
	// lands in it.
	if err := os.WriteFile(filepath.Join(target, "data"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(host, "team", smokeVolume, "data")); err != nil {
		t.Errorf("the write did not reach the volume's directory: %v", err)
	}
	stats, err := node.NodeGetVolumeStats(callCtx, &csi.NodeGetVolumeStatsRequest{VolumeId: volID, VolumePath: target})
	if err != nil || len(stats.GetUsage()) != 2 || stats.GetUsage()[0].GetTotal() <= 0 {
		t.Errorf("NodeGetVolumeStats = %v, %v", stats, err)
	}

	body := scrape(t, addr)
	tr := `miami_transports{id="` + spec.ID() + `",kind="local"} 1`
	requireLines(t, body,
		`miami_intents{kind="directory"} 1`,
		tr,
		`miami_csi_requests_total{code="OK",method="NodeStageVolume"} 1`,
		`miami_csi_requests_total{code="OK",method="NodePublishVolume"} 1`,
		`miami_csi_requests_total{code="OK",method="NodeGetVolumeStats"} 1`,
		`miami_intent_load_failures 0`,
		`miami_garbage_unmounts_total 0`,
	)
	for _, name := range []string{
		"miami_reconcile_duration_seconds_count", "miami_csi_request_duration_seconds_count",
		`miami_reconcile_errors_total{step="item"} 0`, `miami_observation_timeouts_total{op="transport"} 0`,
		"go_goroutines", "process_resident_memory_bytes",
	} {
		if !strings.Contains(body, name) {
			t.Errorf("/metrics lacks %q", name)
		}
	}
	if resp, err := http.Get("http://" + addr + "/healthz"); err != nil || resp.StatusCode != http.StatusOK { //nolint:noctx // a loopback probe
		t.Errorf("/healthz: %v, %v", resp, err)
	} else {
		_ = resp.Body.Close()
	}

	if _, err := node.NodeUnpublishVolume(callCtx, &csi.NodeUnpublishVolumeRequest{VolumeId: volID, TargetPath: target}); err != nil {
		t.Fatalf("NodeUnpublishVolume: %v", err)
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Errorf("the target directory is still there: %v", err)
	}
	if _, err := node.NodeUnstageVolume(callCtx, &csi.NodeUnstageVolumeRequest{VolumeId: volID, StagingTargetPath: stage}); err != nil {
		t.Fatalf("NodeUnstageVolume: %v", err)
	}
	ms, err := observe.ReadMountinfo()
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{stage, target} {
		if _, ok := observe.Table(ms).At(p); ok {
			t.Errorf("%s is still mounted", p)
		}
	}

	// The intent gauge fell with the volume; the transport stays mounted for
	// the next volume (the idle timeout is off), and says so.
	body = scrape(t, addr)
	requireLines(t, body,
		`miami_intents{kind="directory"} 0`,
		tr,
		`miami_csi_requests_total{code="OK",method="NodeUnpublishVolume"} 1`,
		`miami_csi_requests_total{code="OK",method="NodeUnstageVolume"} 1`,
	)
	// The directory and its data stay: unstage never deletes a volume.
	if _, err := os.Stat(filepath.Join(host, "team", smokeVolume, "data")); err != nil {
		t.Errorf("the volume's data is gone: %v", err)
	}
}
