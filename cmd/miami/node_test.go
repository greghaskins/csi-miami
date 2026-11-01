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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/greghaskins/csi-miami/internal/mount/fake"
)

func TestRunNodeFlags(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		args       []string
		wantCode   int
		wantStdout []string
		wantStderr []string
	}{
		{
			name:     "help lists every flag",
			args:     []string{"-h"},
			wantCode: 0,
			wantStdout: []string{
				"Usage of miami node:", "-endpoint", "unix:///csi/csi.sock", "-node-id", "-kubelet-dir", "/var/lib/kubelet",
				"-transports-root", "plugins/miami.csi.greghaskins.com/transports", "-state-dir", "-stage-root", "-metrics-addr", ":9810",
				"-reconcile-interval", "-deadline", "-transport-idle", "-kubeconfig", "-no-events",
			},
		},
		{name: "unknown flag", args: []string{"-nope"}, wantCode: 2, wantStderr: []string{"flag provided but not defined: -nope"}},
		{name: "extra argument", args: []string{"x"}, wantCode: 2, wantStderr: []string{`unexpected argument "x"`}},
		{name: "no node ID", args: nil, wantCode: 2, wantStderr: []string{"-node-id is required"}},
		{name: "zero deadline", args: []string{"-node-id=n", "-deadline=0s"}, wantCode: 2, wantStderr: []string{"-deadline must be positive"}},
		{name: "zero interval", args: []string{"-node-id=n", "-reconcile-interval=0s"}, wantCode: 2, wantStderr: []string{"-reconcile-interval must be positive"}},
		{name: "negative idle", args: []string{"-node-id=n", "-transport-idle=-1s"}, wantCode: 2, wantStderr: []string{"-transport-idle must not be negative"}},
		{name: "relative kubelet dir", args: []string{"-node-id=n", "-kubelet-dir=kubelet"}, wantCode: 2, wantStderr: []string{"-kubelet-dir must be an absolute path"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var stdout, stderr bytes.Buffer
			code := runNode(tt.args, &stdout, &stderr)
			if code != tt.wantCode {
				t.Errorf("exit code = %d, want %d", code, tt.wantCode)
			}
			for _, want := range tt.wantStdout {
				if !strings.Contains(stdout.String(), want) {
					t.Errorf("stdout lacks %q:\n%s", want, stdout.String())
				}
			}
			for _, want := range tt.wantStderr {
				if !strings.Contains(stderr.String(), want) {
					t.Errorf("stderr lacks %q:\n%s", want, stderr.String())
				}
			}
		})
	}
}

// Without --no-events the node needs a cluster, and says so before it has made
// a socket.
func TestRunNodeWithoutAClusterFails(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	dir := t.TempDir()
	sock := filepath.Join(dir, "csi.sock")
	var stderr bytes.Buffer
	code := runNode([]string{"-node-id=n", "-endpoint", "unix://" + sock, "-kubelet-dir", dir, "-transports-root", filepath.Join(dir, "t")}, io.Discard, &stderr)
	if code != 1 || !strings.Contains(stderr.String(), "no kubeconfig") {
		t.Errorf("exit code = %d, stderr = %q", code, stderr.String())
	}
	if _, err := os.Lstat(sock); err == nil {
		t.Error("a socket was made")
	}
}

func TestNodeConfigDefaultsFollowTheKubeletDir(t *testing.T) {
	t.Parallel()
	c := nodeConfig{kubeletDir: "/k"}.withDefaults()
	if c.stateDir != "/k/plugins/miami.csi.greghaskins.com/intents" || c.stageRoot != "/k/plugins/kubernetes.io/csi/miami.csi.greghaskins.com" {
		t.Errorf("defaults: %q, %q", c.stateDir, c.stageRoot)
	}
	if c.transportsRoot != "/k/plugins/miami.csi.greghaskins.com/transports" {
		t.Errorf("transports root default: %q", c.transportsRoot)
	}
	c = nodeConfig{kubeletDir: "/k", stateDir: "/s", stageRoot: "/r", transportsRoot: "/t"}.withDefaults()
	if c.stateDir != "/s" || c.stageRoot != "/r" || c.transportsRoot != "/t" {
		t.Errorf("explicit values changed: %q, %q, %q", c.stateDir, c.stageRoot, c.transportsRoot)
	}
}

func TestServeNodeWithServesUntilTheContextEnds(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	sock := filepath.Join(dir, "csi.sock")
	cfg := nodeConfig{
		endpoint: "unix://" + sock, nodeID: "node-a", kubeletDir: dir,
		transportsRoot: filepath.Join(dir, "transports"), metricsAddr: "127.0.0.1:0",
		interval: time.Hour, deadline: time.Second,
	}.withDefaults()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	addr := make(chan string, 1)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	go func() {
		done <- serveNodeWith(ctx, cfg, &fake.Mounter{}, nil, log, func(a string) { addr <- a })
	}()

	var metricsAddr string
	select {
	case metricsAddr = <-addr:
	case err := <-done:
		t.Fatalf("serveNodeWith returned %v before it was ready", err)
	case <-time.After(10 * time.Second):
		t.Fatal("not ready")
	}

	conn, err := grpc.NewClient("unix:"+sock, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	callCtx, callCancel := context.WithTimeout(ctx, 10*time.Second)
	defer callCancel()
	info, err := csi.NewNodeClient(conn).NodeGetInfo(callCtx, &csi.NodeGetInfoRequest{}, grpc.WaitForReady(true))
	if err != nil || info.GetNodeId() != "node-a" {
		t.Fatalf("NodeGetInfo = %v, %v", info, err)
	}
	if pi, err := csi.NewIdentityClient(conn).GetPluginInfo(callCtx, &csi.GetPluginInfoRequest{}); err != nil || pi.GetName() != "miami.csi.greghaskins.com" {
		t.Errorf("GetPluginInfo = %v, %v", pi, err)
	}
	caps, err := csi.NewNodeClient(conn).NodeGetCapabilities(callCtx, &csi.NodeGetCapabilitiesRequest{})
	if err != nil || len(caps.GetCapabilities()) != 2 {
		t.Errorf("NodeGetCapabilities = %v, %v", caps, err)
	}

	// The call was counted, and the reconciler's gauges are there.
	req, _ := http.NewRequestWithContext(callCtx, http.MethodGet, "http://"+metricsAddr+"/metrics", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	for _, want := range []string{`miami_csi_requests_total{code="OK",method="NodeGetInfo"} 1`, "miami_reconcile_duration_seconds_count", "go_goroutines"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("/metrics lacks %q", want)
		}
	}

	cancel()
	if err := <-done; err != nil {
		t.Errorf("serveNodeWith returned %v after a requested stop", err)
	}
	for _, d := range []string{cfg.stateDir, cfg.stageRoot, cfg.transportsRoot} {
		if _, err := os.Stat(d); err != nil {
			t.Errorf("%s was not made: %v", d, err)
		}
	}
}

func TestServeNodeWithErrors(t *testing.T) {
	t.Parallel()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	dir := t.TempDir()
	file := filepath.Join(dir, "file")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	base := nodeConfig{
		endpoint: "unix://" + filepath.Join(dir, "a.sock"), nodeID: "n", kubeletDir: dir,
		transportsRoot: filepath.Join(dir, "t"), deadline: time.Second, interval: time.Hour,
	}
	with := func(f func(*nodeConfig)) nodeConfig { c := base; f(&c); return c.withDefaults() }
	tests := []struct {
		name string
		cfg  nodeConfig
	}{
		{"transports root under a file", with(func(c *nodeConfig) { c.transportsRoot = filepath.Join(file, "t") })},
		{"stage root under a file", with(func(c *nodeConfig) { c.stageRoot = filepath.Join(file, "s") })},
		{"state dir that is a file", with(func(c *nodeConfig) { c.stateDir = file })},
		{"overlapping roots", with(func(c *nodeConfig) { c.stageRoot = filepath.Join(c.transportsRoot, "s") })},
		{"endpoint that is not a unix socket", with(func(c *nodeConfig) { c.endpoint = "tcp://192.0.2.1:1" })},
		{"a metrics address that is taken or bad", with(func(c *nodeConfig) { c.metricsAddr = "256.0.0.1:1" })},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if err := serveNodeWith(context.Background(), tt.cfg, &fake.Mounter{}, nil, log, nil); err == nil {
				t.Error("no error")
			}
		})
	}
}

func TestRunDispatchesNode(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	if code := run([]string{"node", "-h"}, &stdout, &stderr); code != 0 || !strings.HasPrefix(stdout.String(), "Usage of miami node:") {
		t.Errorf("exit code = %d, stdout = %q", code, stdout.String())
	}
}
