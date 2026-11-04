package blockd

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/chunk"
)

const (
	testChunk = 1 << 20 // 1 MiB
	testSize  = 2 << 20 // 2 chunks
	testBound = 4 << 20 // room for growth
	testVol   = "pvc-test"
	testNode  = "node-a"
)

func testLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// fakeChecker is a controllable holder.Checker.
type fakeChecker struct {
	mu   sync.Mutex
	mine bool
	err  error
}

func (f *fakeChecker) Mine(context.Context) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mine, f.err
}

func (f *fakeChecker) set(mine bool, err error) {
	f.mu.Lock()
	f.mine, f.err = mine, err
	f.mu.Unlock()
}

// fakeDevice is a non-kernel device.
type fakeDevice struct {
	idx          int
	size         int64
	reconfigErr  error
	disconnected bool
}

func (d *fakeDevice) index() int { return d.idx }
func (d *fakeDevice) reconfigure(_ context.Context, size int64) error {
	d.size = size
	return d.reconfigErr
}
func (d *fakeDevice) disconnect(context.Context) error { d.disconnected = true; return nil }

// recordingStore records the order of the mutating store calls so a test can
// assert the publish order.
type recordingStore struct {
	storer
	mu  sync.Mutex
	ops []string
}

func (r *recordingStore) record(op string) {
	r.mu.Lock()
	r.ops = append(r.ops, op)
	r.mu.Unlock()
}

func (r *recordingStore) calls() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.ops...)
}

func (r *recordingStore) Put(ctx context.Context, h chunk.Hash, rd io.Reader, n int64) error {
	r.record("put")
	return r.storer.Put(ctx, h, rd, n)
}

func (r *recordingStore) WriteManifest(ctx context.Context, m chunk.Manifest) error {
	r.record("manifest")
	return r.storer.WriteManifest(ctx, m)
}

func (r *recordingStore) Publish(ctx context.Context, name string) error {
	r.record("current")
	return r.storer.Publish(ctx, name)
}

// testConfig builds a config over fresh temp directories with a holder that is
// ours.
func testConfig(t testing.TB) (Config, *fakeChecker) {
	t.Helper()
	base := t.TempDir()
	store := filepath.Join(base, testVol)
	if err := os.MkdirAll(store, 0o750); err != nil {
		t.Fatalf("mkdir store: %v", err)
	}
	if _, err := chunk.Open(store); err != nil { // creates chunks/ and manifests/
		t.Fatalf("open store: %v", err)
	}
	chk := &fakeChecker{mine: true}
	cfg := Config{
		Volume:        testVol,
		StoreDir:      store,
		CacheDir:      filepath.Join(t.TempDir(), "cache"),
		ControlSocket: filepath.Join(t.TempDir(), "control.sock"),
		Size:          testSize,
		Chunk:         testChunk,
		CacheBound:    testBound,
		Epoch:         1,
		Node:          testNode,
		Holder:        chk,
	}
	return cfg, chk
}

// newTestServer opens a server over cfg with a fake device.
func newTestServer(t *testing.T, cfg Config) *server {
	t.Helper()
	s, err := newServer(context.Background(), cfg, testLog())
	if err != nil {
		t.Fatalf("newServer: %v", err)
	}
	t.Cleanup(func() { _ = s.close() })
	s.setDevice(&fakeDevice{idx: 7})
	return s
}

// openStore opens the store at cfg.StoreDir for test setup.
func openStore(t *testing.T, cfg Config) *chunk.Store {
	t.Helper()
	store, err := chunk.Open(cfg.StoreDir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return store
}

// pattern is n bytes of a repeating, seed-dependent pattern.
func pattern(seed byte, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = seed + byte((i*31)&0xff) //nolint:gosec // deliberate byte wraparound for a test pattern
	}
	return b
}

// putChunk stores data and returns its hash.
func putChunk(t *testing.T, store *chunk.Store, data []byte) chunk.Hash {
	t.Helper()
	h := chunk.Sum(data)
	if err := store.Put(context.Background(), h, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatalf("put chunk: %v", err)
	}
	return h
}

// writeCurrent writes a manifest and makes it current.
func writeCurrent(t *testing.T, store *chunk.Store, epoch, seq uint64, chunks []chunk.Hash) chunk.Manifest {
	t.Helper()
	m := chunk.Manifest{
		Volume:   testVol,
		Epoch:    epoch,
		Sequence: seq,
		Size:     int64(len(chunks)) * testChunk,
		Chunk:    testChunk,
		Time:     time.Now().UTC(),
		Chunks:   chunks,
	}
	ctx := context.Background()
	if err := store.WriteManifest(ctx, m); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if err := store.Publish(ctx, m.Name()); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	return m
}

// readDevice reads n bytes at off through the backend.
func readDevice(t *testing.T, s *server, off int64, n int) []byte {
	t.Helper()
	p := make([]byte, n)
	got, err := s.back.ReadAt(p, off)
	if err != nil {
		t.Fatalf("ReadAt(%d,%d): %v", off, n, err)
	}
	if got != n {
		t.Fatalf("ReadAt short: %d of %d", got, n)
	}
	return p
}

// writeDevice writes p at off through the backend.
func writeDevice(t *testing.T, s *server, off int64, p []byte, fua bool) {
	t.Helper()
	n, err := s.back.WriteAt(p, off, fua)
	if err != nil {
		t.Fatalf("WriteAt(%d): %v", off, err)
	}
	if n != len(p) {
		t.Fatalf("WriteAt short: %d of %d", n, len(p))
	}
}

func manifestNames(t *testing.T, store *chunk.Store) []string {
	t.Helper()
	names, err := store.Manifests(context.Background())
	if err != nil {
		t.Fatalf("manifests: %v", err)
	}
	return names
}

func hasChunk(s *server, i uint32) bool { return s.back.cache.Has(i) }
