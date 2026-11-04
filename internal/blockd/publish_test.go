package blockd

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/greghaskins/csi-miami/internal/chunk"
)

// prepareBoundary writes a full chunk and sets the boundary, ready to publish.
func prepareBoundary(t *testing.T, s *server, seed byte) {
	t.Helper()
	writeDevice(t, s, 0, pattern(seed, testChunk), false)
	if err := s.boundary(); err != nil {
		t.Fatalf("boundary: %v", err)
	}
}

func TestPublishOrder(t *testing.T) {
	cfg, _ := testConfig(t)
	s := newTestServer(t, cfg)
	rs := &recordingStore{storer: s.store}
	s.store = rs

	prepareBoundary(t, s, 1)
	reply, err := s.publish(context.Background(), "request")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if reply.Name == "" || reply.Uploaded == 0 {
		t.Fatalf("publish reply: %+v", reply)
	}

	ops := rs.calls()
	mi, ci, puts := -1, -1, 0
	for k, op := range ops {
		switch op {
		case "put":
			puts++
			if mi >= 0 {
				t.Fatalf("a chunk was put after the manifest: %v", ops)
			}
		case "manifest":
			mi = k
		case "current":
			ci = k
		}
	}
	if puts == 0 {
		t.Fatalf("no chunk was uploaded: %v", ops)
	}
	if mi < 0 || ci < 0 || ci < mi {
		t.Fatalf("order wrong (manifest %d, current %d): %v", mi, ci, ops)
	}
}

func TestPublishRefusedWhenHolderNotMine(t *testing.T) {
	cfg, chk := testConfig(t)
	s := newTestServer(t, cfg)
	chk.set(false, nil)

	prepareBoundary(t, s, 2)
	reply, err := s.publish(context.Background(), "request")
	if !errors.Is(err, ErrRefused) {
		t.Fatalf("publish err = %v, want ErrRefused", err)
	}
	if !reply.Refused {
		t.Fatal("reply.Refused is false")
	}
	if names := manifestNames(t, openStore(t, cfg)); len(names) != 0 {
		t.Fatalf("a manifest was written on refusal: %v", names)
	}
	if idx := s.back.cache.Index(); len(idx.Dirty) != 1 || idx.Dirty[0] != 0 {
		t.Fatalf("dirty set not kept on refusal: %v", idx.Dirty)
	}
	if !s.status().Refused {
		t.Fatal("status does not report Refused")
	}
}

func TestPublishRefusedWhenCurrentHasHigherEpoch(t *testing.T) {
	cfg, _ := testConfig(t)
	store := openStore(t, cfg)
	h0 := putChunk(t, store, pattern(3, testChunk))
	h1 := putChunk(t, store, pattern(4, testChunk))
	// current is at epoch 5, above our epoch 1.
	writeCurrent(t, store, 5, 1, []chunk.Hash{h0, h1})

	s := newTestServer(t, cfg)
	prepareBoundary(t, s, 7)
	reply, err := s.publish(context.Background(), "request")
	if !errors.Is(err, ErrRefused) {
		t.Fatalf("publish err = %v, want ErrRefused", err)
	}
	if !reply.Refused {
		t.Fatal("reply.Refused is false")
	}
	if names := manifestNames(t, store); len(names) != 1 || names[0] != "5.1" {
		t.Fatalf("manifests changed on refusal: %v", names)
	}
	if idx := s.back.cache.Index(); len(idx.Dirty) != 1 {
		t.Fatalf("dirty set not kept: %v", idx.Dirty)
	}
}

func TestPublishNothingWhenClean(t *testing.T) {
	cfg, _ := testConfig(t)
	s := newTestServer(t, cfg)

	if err := s.boundary(); err != nil {
		t.Fatalf("boundary on a clean volume: %v", err)
	}
	reply, err := s.publish(context.Background(), "request")
	if err != nil {
		t.Fatalf("publish on a clean volume: %v", err)
	}
	if reply.Name != "" || reply.Uploaded != 0 {
		t.Fatalf("publish on a clean volume produced %+v", reply)
	}
	if names := manifestNames(t, openStore(t, cfg)); len(names) != 0 {
		t.Fatalf("a manifest was written for an empty boundary: %v", names)
	}
}

func TestPublishNoBoundary(t *testing.T) {
	cfg, _ := testConfig(t)
	s := newTestServer(t, cfg)
	if _, err := s.publish(context.Background(), "request"); !errors.Is(err, ErrNoBoundary) {
		t.Fatalf("publish without a boundary = %v, want ErrNoBoundary", err)
	}
}

func TestPruneAfterPublish(t *testing.T) {
	cfg, _ := testConfig(t)
	s := newTestServer(t, cfg)
	ctx := context.Background()

	for i := range byte(generationsKeep + 1) { // one more than we keep
		prepareBoundary(t, s, 10+i)
		if _, err := s.publish(ctx, "request"); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	store := openStore(t, cfg)
	names := manifestNames(t, store)
	if len(names) != generationsKeep {
		t.Fatalf("after %d publishes, kept %v, want %d", generationsKeep+1, names, generationsKeep)
	}
	cur, ok, err := store.Current(ctx)
	if err != nil || !ok {
		t.Fatalf("current: %v ok=%v", err, ok)
	}
	want := fmt.Sprintf("1.%d", generationsKeep+1)
	if cur != want {
		t.Fatalf("current = %q, want %q", cur, want)
	}
}

func TestStatusReportsBinaryHash(t *testing.T) {
	cfg, _ := testConfig(t)
	s := newTestServer(t, cfg)
	want, err := binaryHash()
	if err != nil {
		t.Fatalf("binaryHash: %v", err)
	}
	if want == "" {
		t.Fatal("binary hash is empty")
	}
	if got := s.status().BinaryHash; got != want {
		t.Fatalf("status binary hash = %q, want %q", got, want)
	}
}
