package blockd

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"sync"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/chunk"
)

// failingStore fails the first failPublish calls to Publish (the current
// rename), leaving an orphan manifest behind, as a crashed publish would.
type failingStore struct {
	storer
	mu          sync.Mutex
	failPublish int
}

func (f *failingStore) Publish(ctx context.Context, name string) error {
	f.mu.Lock()
	fail := f.failPublish > 0
	if fail {
		f.failPublish--
	}
	f.mu.Unlock()
	if fail {
		return errors.New("store publish failed")
	}
	return f.storer.Publish(ctx, name)
}

// retransmitStore writes the manifest but then reports fs.ErrExist, as a
// non-idempotent NFSv3 LINK retransmit does.
type retransmitStore struct {
	storer
	armed bool
}

func (r *retransmitStore) WriteManifest(ctx context.Context, m chunk.Manifest) error {
	if err := r.storer.WriteManifest(ctx, m); err != nil {
		return err
	}
	if r.armed {
		r.armed = false
		return fs.ErrExist
	}
	return nil
}

// C1a: a publish that fails at the store leaves an orphan manifest; the next
// publish derives a higher sequence from the store and current advances,
// instead of colliding on the same name forever.
func TestPublishSequenceAdvancesAfterFailure(t *testing.T) {
	cfg, _ := testConfig(t)
	s := newTestServer(t, cfg)
	s.store = &failingStore{storer: s.store, failPublish: 1}
	ctx := context.Background()

	prepareBoundary(t, s, 1)
	if _, err := s.publish(ctx, "request"); err == nil {
		t.Fatal("expected the first publish to fail at the store")
	}
	// The orphan manifest 1.1 now exists but current was not advanced.
	store := openStore(t, cfg)
	if names := manifestNames(t, store); len(names) != 1 || names[0] != "1.1" {
		t.Fatalf("after the failed publish, manifests = %v, want [1.1]", names)
	}

	prepareBoundary(t, s, 2)
	reply, err := s.publish(ctx, "request")
	if err != nil {
		t.Fatalf("second publish: %v", err)
	}
	if reply.Name != "1.2" {
		t.Fatalf("second publish name = %q, want 1.2", reply.Name)
	}
	if cur, ok, _ := store.Current(ctx); !ok || cur != "1.2" {
		t.Fatalf("current = %q (ok=%v), want 1.2", cur, ok)
	}
}

// C1b: an orphan manifest left by a crash between the manifest link and the
// current rename is counted, so the next publish does not collide with it.
func TestPublishSkipsOrphanManifest(t *testing.T) {
	cfg, _ := testConfig(t)
	store := openStore(t, cfg)
	// Seed an orphan manifest at 1.1 with no current (a crash before Publish).
	h0 := putChunk(t, store, pattern(5, testChunk))
	m := chunk.Manifest{Volume: testVol, Epoch: 1, Sequence: 1, Size: testSize, Chunk: testChunk, Chunks: []chunk.Hash{h0, chunk.ZeroHash(testChunk)}}
	if err := store.WriteManifest(context.Background(), m); err != nil {
		t.Fatalf("seed orphan: %v", err)
	}

	s := newTestServer(t, cfg)
	prepareBoundary(t, s, 6)
	reply, err := s.publish(context.Background(), "request")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if reply.Name != "1.2" {
		t.Fatalf("publish name = %q, want 1.2 (past the orphan 1.1)", reply.Name)
	}
}

// C1 retransmit: a WriteManifest that reports EEXIST although our bytes are
// already there is accepted, not treated as a collision.
func TestWriteManifestAcceptsRetransmit(t *testing.T) {
	cfg, _ := testConfig(t)
	s := newTestServer(t, cfg)
	s.store = &retransmitStore{storer: s.store, armed: true}
	prepareBoundary(t, s, 7)
	reply, err := s.publish(context.Background(), "request")
	if err != nil {
		t.Fatalf("publish with a retransmit: %v", err)
	}
	if reply.Name != "1.1" {
		t.Fatalf("publish name = %q, want 1.1 (the retransmit accepted, not bumped)", reply.Name)
	}
}

// C3: a crash with a boundary active must not wedge later generations. On
// resume the boundary is cleared (chunks kept dirty), so Boundary and Publish
// work again.
func TestResumeWithActiveBoundaryRecovers(t *testing.T) {
	cfg, _ := testConfig(t)
	s1 := newServerForTest(t, cfg)
	writeDevice(t, s1, 0, pattern(8, testChunk), false)
	if err := s1.boundary(); err != nil { // leaves an active boundary in the index
		t.Fatalf("boundary: %v", err)
	}
	if err := s1.back.cache.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := s1.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2 := newTestServer(t, cfg) // resume: ClearBoundary(false)
	if idx := s2.back.cache.Index(); idx.Boundary != nil {
		t.Fatalf("resume left a boundary active: %v", idx.Boundary)
	}
	if err := s2.boundary(); err != nil {
		t.Fatalf("boundary after resume: %v", err)
	}
	reply, err := s2.publish(context.Background(), "request")
	if err != nil {
		t.Fatalf("publish after resume: %v", err)
	}
	if reply.Name == "" {
		t.Fatal("publish after resume produced no manifest")
	}
}

// I3: tgidOf maps a thread id to its process.
func TestTgidOfSelf(t *testing.T) {
	tid := unix.Gettid()
	if g := tgidOf(tid); g != os.Getpid() {
		t.Fatalf("tgidOf(%d) = %d, want this process %d", tid, g, os.Getpid())
	}
}

// I7: Status reports the last successful publish (from the base manifest on
// resume), counts no bytes for trimmed chunks, and counts clean cache copies.
func TestStatusPublishTimeAndCounts(t *testing.T) {
	cfg, _ := testConfig(t)
	store := openStore(t, cfg)
	h0 := putChunk(t, store, pattern(9, testChunk))
	m := writeCurrent(t, store, 1, 1, []chunk.Hash{h0, chunk.ZeroHash(testChunk)})
	reRead, err := store.ReadManifest(context.Background(), m.Name())
	if err != nil {
		t.Fatal(err)
	}

	s := newTestServer(t, cfg)
	if st := s.status(); !st.LastPublish.Equal(reRead.Time) {
		t.Fatalf("LastPublish = %v, want the base manifest's time %v", st.LastPublish, reRead.Time)
	}

	// Reading chunk 0 fetches and caches a clean copy.
	readDevice(t, s, 0, testChunk)
	if c := s.status().Cached; c < testChunk {
		t.Fatalf("Cached = %d, want at least one chunk (%d)", c, testChunk)
	}

	// Writing chunk 0 makes it dirty; trimming chunk 1 adds no dirty bytes.
	writeDevice(t, s, 0, pattern(10, testChunk), false)
	if d := s.status().Dirty; d != testChunk {
		t.Fatalf("Dirty = %d, want one chunk (%d)", d, testChunk)
	}
	if err := s.back.Trim(testChunk, testChunk); err != nil {
		t.Fatal(err)
	}
	if d := s.status().Dirty; d != testChunk {
		t.Fatalf("Dirty after trim = %d, want one chunk (trimmed chunks hold no bytes)", d)
	}

	// A publish attempt moves LastAttempt and advances LastPublish.
	if err := s.boundary(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.publish(context.Background(), "request"); err != nil {
		t.Fatal(err)
	}
	st := s.status()
	if st.LastAttempt.IsZero() {
		t.Fatal("LastAttempt not set after a publish")
	}
	if !st.LastPublish.After(reRead.Time) {
		t.Fatalf("LastPublish %v did not advance past the base %v", st.LastPublish, reRead.Time)
	}
}
