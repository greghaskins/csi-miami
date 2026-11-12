package blockd

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"

	"github.com/greghaskins/csi-miami/internal/chunk"
)

func TestReadMissFetchesAndInserts(t *testing.T) {
	cfg, _ := testConfig(t)
	store := openStore(t, cfg)
	data0 := pattern(1, testChunk)
	h0 := putChunk(t, store, data0)
	writeCurrent(t, store, 1, 1, []chunk.Hash{h0, chunk.ZeroHash(testChunk)})

	s := newTestServer(t, cfg)
	if hasChunk(s, 0) {
		t.Fatal("chunk 0 should not be cached before the first read")
	}
	got := readDevice(t, s, 0, testChunk)
	if !bytes.Equal(got, data0) {
		t.Fatal("read did not return the stored chunk")
	}
	if !hasChunk(s, 0) {
		t.Fatal("chunk 0 should be cached after a miss fetch")
	}
}

func TestReadOfZeroChunkNeedsNoFetch(t *testing.T) {
	cfg, _ := testConfig(t)
	store := openStore(t, cfg)
	data0 := pattern(2, testChunk)
	h0 := putChunk(t, store, data0)
	// Chunk 1 is the zero hash; the zero chunk is deliberately NOT stored, so a
	// fetch of it would fail. A correct read serves zeros without fetching.
	writeCurrent(t, store, 1, 1, []chunk.Hash{h0, chunk.ZeroHash(testChunk)})

	s := newTestServer(t, cfg)
	got := readDevice(t, s, testChunk, testChunk)
	if !bytes.Equal(got, make([]byte, testChunk)) {
		t.Fatal("zero chunk did not read as zeros")
	}
}

func TestWriteDirtiesAndFlushSyncs(t *testing.T) {
	cfg, _ := testConfig(t)
	s := newTestServer(t, cfg)

	payload := pattern(3, 4096)
	writeDevice(t, s, 0, payload, false)

	idx := s.back.cache.Index()
	if len(idx.Dirty) != 1 || idx.Dirty[0] != 0 {
		t.Fatalf("dirty set after write: %v", idx.Dirty)
	}
	if err := s.back.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	got := readDevice(t, s, 0, testChunk)
	if !bytes.Equal(got[:4096], payload) {
		t.Fatal("the written bytes read back wrong")
	}
	if !bytes.Equal(got[4096:], make([]byte, testChunk-4096)) {
		t.Fatal("the rest of the chunk is not zero")
	}
}

func TestTrimMarksZero(t *testing.T) {
	cfg, _ := testConfig(t)
	s := newTestServer(t, cfg)
	writeDevice(t, s, 0, pattern(4, testChunk), false)
	writeDevice(t, s, testChunk, pattern(5, testChunk), false)

	if err := s.back.Trim(testChunk, testChunk); err != nil {
		t.Fatalf("Trim: %v", err)
	}
	idx := s.back.cache.Index()
	if len(idx.Zero) != 1 || idx.Zero[0] != 1 {
		t.Fatalf("zero set after trim: %v", idx.Zero)
	}
	if got := readDevice(t, s, testChunk, testChunk); !bytes.Equal(got, make([]byte, testChunk)) {
		t.Fatal("trimmed chunk did not read as zeros")
	}
	if got := readDevice(t, s, 0, testChunk); !bytes.Equal(got, pattern(4, testChunk)) {
		t.Fatal("trim reached a chunk outside its range")
	}
}

func TestGrowExtendsWithZeroChunks(t *testing.T) {
	cfg, _ := testConfig(t)
	s := newTestServer(t, cfg)
	dev := &fakeDevice{idx: 3}
	s.setDevice(dev)

	const newSize = 4 << 20 // 4 chunks
	if err := s.growTo(context.Background(), newSize); err != nil {
		t.Fatalf("growTo: %v", err)
	}
	if s.back.Size() != newSize {
		t.Fatalf("size after grow: %d", s.back.Size())
	}
	if dev.size != newSize {
		t.Fatalf("device not reconfigured: %d", dev.size)
	}
	if got := readDevice(t, s, testSize, testChunk); !bytes.Equal(got, make([]byte, testChunk)) {
		t.Fatal("a grown chunk did not read as zeros")
	}
}

func TestCreateFromCurrentManifest(t *testing.T) {
	cfg, _ := testConfig(t)
	store := openStore(t, cfg)
	d0, d1 := pattern(6, testChunk), pattern(7, testChunk)
	h0, h1 := putChunk(t, store, d0), putChunk(t, store, d1)
	m := writeCurrent(t, store, 1, 1, []chunk.Hash{h0, h1})

	s := newTestServer(t, cfg)
	if s.base != m.Name() {
		t.Fatalf("base = %q, want %q", s.base, m.Name())
	}
	s.back.mu.RLock()
	gotBase := append([]chunk.Hash(nil), s.back.manifest...)
	s.back.mu.RUnlock()
	if len(gotBase) != 2 || gotBase[0] != h0 || gotBase[1] != h1 {
		t.Fatalf("base manifest chunks wrong: %v", gotBase)
	}
	if !bytes.Equal(readDevice(t, s, 0, testChunk), d0) {
		t.Fatal("chunk 0 read wrong")
	}
	if !bytes.Equal(readDevice(t, s, testChunk, testChunk), d1) {
		t.Fatal("chunk 1 read wrong")
	}
}

func TestResumeFromIndexKeepsDirty(t *testing.T) {
	cfg, _ := testConfig(t)
	s1 := newServerForTest(t, cfg)
	payload := pattern(8, testChunk)
	writeDevice(t, s1, 0, payload, false)
	if err := s1.back.cache.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := s1.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen on the same cache directory: the dirty set survives.
	s2 := newTestServer(t, cfg)
	idx := s2.back.cache.Index()
	if len(idx.Dirty) != 1 || idx.Dirty[0] != 0 {
		t.Fatalf("dirty set after resume: %v", idx.Dirty)
	}
	if got := readDevice(t, s2, 0, testChunk); !bytes.Equal(got, payload) {
		t.Fatal("resumed dirty chunk read wrong")
	}
}

// newServerForTest opens a server without registering a t.Cleanup close, so the
// test can close it itself (to then reopen the same cache).
func newServerForTest(t *testing.T, cfg Config) *server {
	t.Helper()
	s, err := newServer(context.Background(), cfg, testLog())
	if err != nil {
		t.Fatalf("newServer: %v", err)
	}
	s.setDevice(&fakeDevice{idx: 7})
	return s
}

func TestCorruptChunkFailsThatReadOnly(t *testing.T) {
	cfg, _ := testConfig(t)
	store := openStore(t, cfg)
	good := pattern(9, testChunk)
	bad := pattern(10, testChunk)
	hGood := putChunk(t, store, good)
	hBad := putChunk(t, store, bad)
	// Corrupt chunk 1's stored bytes so Get returns ErrCorrupt.
	if err := os.WriteFile(store.ChunkPath(hBad), pattern(99, testChunk), 0o600); err != nil {
		t.Fatalf("corrupt chunk: %v", err)
	}
	writeCurrent(t, store, 1, 1, []chunk.Hash{hGood, hBad})

	s := newTestServer(t, cfg)
	// The corrupt chunk fails its own read.
	if _, err := s.back.ReadAt(make([]byte, testChunk), testChunk); !errors.Is(err, chunk.ErrCorrupt) {
		t.Fatalf("corrupt read error = %v, want ErrCorrupt", err)
	}
	// The device is not wedged: the good chunk still reads.
	if got := readDevice(t, s, 0, testChunk); !bytes.Equal(got, good) {
		t.Fatal("good chunk read wrong after a corrupt read")
	}
}

// TestReadZeroPathZeroAllocs checks the backend's own per-request machinery
// allocates nothing: a 4 KiB read of a chunk the cache serves as zeros (no
// store access, no file open) touches only the pooled aligned buffer and a
// copy. A read of a resident data chunk inherits the cache's per-read file
// open; that cost is the cache's, measured honestly by BenchmarkReadHit.
func TestReadZeroPathZeroAllocs(t *testing.T) {
	cfg, _ := testConfig(t)
	s := newTestServer(t, cfg)
	if err := s.back.cache.MarkZero(0); err != nil {
		t.Fatalf("MarkZero: %v", err)
	}
	p := make([]byte, 4096)
	allocs := testing.AllocsPerRun(200, func() {
		if _, err := s.back.ReadAt(p, 0); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("backend read machinery allocated %.0f/op, want 0", allocs)
	}
}

// BenchmarkReadHit measures a 4 KiB read of a RESIDENT (written, synced) chunk:
// the honest warm-hit path, which opens, preads and closes the chunk's O_DIRECT
// file in the cache. It is not zero-alloc, and the number reported is the real
// one (a per-chunk fd cache could drop the open/close later if G1 needs it).
func BenchmarkReadHit(b *testing.B) {
	cfg, _ := testConfig(b)
	s, err := newServer(context.Background(), cfg, testLog())
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = s.close() }()
	s.setDevice(&fakeDevice{idx: 1})
	// Make chunk 0 a resident, durable data chunk.
	full := make([]byte, testChunk)
	for i := range full {
		full[i] = byte(i)
	}
	if _, err := s.back.WriteAt(full, 0, true); err != nil {
		b.Fatal(err)
	}
	p := make([]byte, 4096)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := s.back.ReadAt(p, 0); err != nil {
			b.Fatal(err)
		}
	}
}

// A write whose connection dies between two segments stops there: the segment
// it had finished stays, the next does nothing, and the write is reported as
// failed (not done), with no FUA sync, so the kernel resends all of it.
func TestAWriteOfADeadConnectionStopsAtTheNextSegmentAndFails(t *testing.T) {
	cfg, _ := testConfig(t)
	s := newTestServer(t, cfg)
	calls := 0
	alive := func() bool { calls++; return calls == 1 } // alive for chunk 0's segment only
	p := pattern(7, 8192)
	n, err := s.back.writeAt(p, testChunk-4096, true, alive)
	if !errors.Is(err, errConnDead) || n != 4096 {
		t.Fatalf("writeAt = %d, %v; want 4096, errConnDead", n, err)
	}
	if idx := s.back.cache.Index(); len(idx.Dirty) != 1 || idx.Dirty[0] != 0 {
		t.Fatalf("dirty after the refused segment: %v, want chunk 0 only", idx.Dirty)
	}
	if err := s.back.trim(testChunk, testChunk, func() bool { return false }); !errors.Is(err, errConnDead) {
		t.Fatalf("trim of a dead connection = %v, want errConnDead", err)
	}
	if hasChunk(s, 1) {
		t.Fatal("a refused trim or segment touched chunk 1")
	}
}
