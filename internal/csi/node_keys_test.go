package csi

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"

	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/mount/fake"
	"github.com/greghaskins/csi-miami/internal/reconcile"
)

// Key values the tests use; unusual enough to be found by a substring search.
const (
	testKey      = "k3y-0f-the-t3st-volume-aaaaaaaaaaaaaaaa"
	testPrevious = "pr3vious-k3y-bbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func TestNodeStageReadsKeyFromSecrets(t *testing.T) {
	t.Parallel()
	rec := &fakeRec{}
	n := newTestNode(t, rec, nil)
	req := stageReq()
	req.Secrets = map[string]string{"key": testKey, "previousKey": testPrevious}
	if _, err := n.NodeStageVolume(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if len(rec.stageKeys) != 1 || string(rec.stageKeys[0].key) != testKey || string(rec.stageKeys[0].previous) != testPrevious {
		t.Errorf("stage keys = %q", rec.stageKeys)
	}

	// No Secret, no keys: a plain volume stages as before.
	rec2 := &fakeRec{}
	if _, err := newTestNode(t, rec2, nil).NodeStageVolume(context.Background(), stageReq()); err != nil {
		t.Fatal(err)
	}
	if k := rec2.stageKeys[0]; k.key != nil || k.previous != nil {
		t.Errorf("keys without a Secret: %v", k)
	}
}

func TestNodePublishCarriesKeyForRestage(t *testing.T) {
	t.Parallel()
	rec := &fakeRec{record: stagedRecord()}
	n := newTestNode(t, rec, nil)
	req := publishReq()
	req.Secrets = map[string]string{"key": testKey}
	if _, err := n.NodePublishVolume(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if len(rec.publishKeys) != 1 || string(rec.publishKeys[0].key) != testKey || rec.publishKeys[0].previous != nil {
		t.Errorf("publish keys = %q", rec.publishKeys)
	}
}

// keyedBackend keeps the key slices the handler passed on, to see that they
// were wiped after the call.
type keyedBackend struct {
	NodeBackend
	got []reconcile.Keys
}

func (b *keyedBackend) StageWith(ctx context.Context, in intent.Intent, k reconcile.Keys) error {
	b.got = append(b.got, k)
	return b.NodeBackend.StageWith(ctx, in, k)
}

func TestKeyNeverInLogsOrIntent(t *testing.T) {
	t.Parallel()
	dir := resolved(t, t.TempDir())
	for _, d := range []string{"nodes", "stage", "host"} {
		if err := os.Mkdir(filepath.Join(dir, d), 0o750); err != nil {
			t.Fatal(err)
		}
	}
	logs := &syncBuffer{}
	log := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	store, err := intent.Open(filepath.Join(dir, "intents"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	rec, err := reconcile.New(store, &fake.Mounter{}, reconcile.Options{
		TransportsRoot: filepath.Join(dir, "nodes"), StageRoot: filepath.Join(dir, "stage"), Interval: time.Hour, Logger: log,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rec.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("reconciler: %v", err)
		}
	}()
	backend := &keyedBackend{NodeBackend: NodeBackend{Reconciler: rec, Store: store}}
	n, err := NewNode(NodeOptions{NodeID: "node-a", Reconciler: backend, Mounter: &fakeStatfs{}, Log: log})
	if err != nil {
		t.Fatal(err)
	}

	stagePath := filepath.Join(dir, "stage", "x", "globalmount")
	vc := localCtx
	vc.Transport.Path = filepath.Join(dir, "host")
	secrets := map[string]string{"key": testKey, "previousKey": testPrevious}
	sreq := stageReq()
	sreq.StagingTargetPath = stagePath
	sreq.VolumeContext = vc.Encode()
	sreq.Secrets = secrets
	preq := publishReq()
	preq.StagingTargetPath = stagePath
	preq.TargetPath = filepath.Join(dir, "pod", "mount")
	preq.Secrets = secrets

	logging := LoggingInterceptor(log)
	call := func(method string, req any, h func(context.Context, any) (any, error)) (any, error) {
		return logging(context.Background(), req, &grpc.UnaryServerInfo{FullMethod: method}, h)
	}
	// Whether the calls succeed does not matter (the mounter is a fake); what
	// the node wrote, logged and replied does, errors included.
	stageResp, stageErr := call("/csi.v1.Node/NodeStageVolume", sreq, func(c context.Context, r any) (any, error) {
		return n.NodeStageVolume(c, r.(*csi.NodeStageVolumeRequest))
	})
	pubResp, pubErr := call("/csi.v1.Node/NodePublishVolume", preq, func(c context.Context, r any) (any, error) {
		return n.NodePublishVolume(c, r.(*csi.NodePublishVolumeRequest))
	})
	if _, ok, err := store.Get(nodeVol); err != nil || !ok {
		t.Fatalf("the intent record was not written (ok=%v, err=%v): the test would pass for nothing", ok, err)
	}

	said := map[string]string{"logs": logs.String(), "stage reply": fmt.Sprint(stageResp, stageErr), "publish reply": fmt.Sprint(pubResp, pubErr)}
	err = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, rerr := os.ReadFile(p) //nolint:gosec // a path of the test's own temporary directory
		said["file "+filepath.Base(p)] = string(b)
		return rerr
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(said["logs"], "rpc") {
		t.Fatalf("the interceptor logged nothing: %q", said["logs"])
	}
	for where, text := range said {
		for _, k := range []string{testKey, testPrevious} {
			if strings.Contains(text, k) {
				t.Errorf("a key is in the %s", where)
			}
		}
	}

	// The handler's copies of the keys were wiped when the call returned.
	if len(backend.got) == 0 {
		t.Fatal("the stage did not reach the reconciler")
	}
	for _, k := range backend.got {
		for _, b := range [][]byte{k.Key, k.PreviousKey} {
			if bytes.ContainsAny(b, "k3y") && len(bytes.Trim(b, "\x00")) > 0 {
				t.Errorf("a key was not wiped after the call: %q", b)
			}
		}
	}
}
