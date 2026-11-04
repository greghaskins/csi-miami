package cache

import (
	"bytes"
	"errors"
	"testing"
)

const (
	srChunk = 1 << 20
	srSize  = 4 << 20 // 4 chunks
)

func newSubCache(t testing.TB, bound int64) *Cache {
	t.Helper()
	c, err := Create(t.TempDir(), Index{Volume: "v", Size: srSize, Chunk: srChunk}, bound)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func filled(n int, b byte) []byte {
	p := AlignedBuffer(n)
	for i := range p {
		p[i] = b
	}
	return p
}

func TestWriteAtReadAtSubRange(t *testing.T) {
	c := newSubCache(t, srSize)
	want := filled(8192, 0xA7)
	if err := c.WriteAt(0, 4096, want); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	got := AlignedBuffer(8192)
	if err := c.ReadAt(0, 4096, got); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("sub-range read did not return what was written")
	}
	// The gaps before and after read as zeros.
	gap := AlignedBuffer(4096)
	if err := c.ReadAt(0, 0, gap); err != nil || !bytes.Equal(gap, make([]byte, 4096)) {
		t.Fatalf("gap before the write is not zero: %v", err)
	}
	if err := c.ReadAt(0, 12288, gap); err != nil || !bytes.Equal(gap, make([]byte, 4096)) {
		t.Fatalf("gap after the write is not zero: %v", err)
	}
}

func TestWriteAtWholeChunk(t *testing.T) {
	c := newSubCache(t, srSize)
	want := filled(srChunk, 0x5c)
	if err := c.WriteAt(1, 0, want); err != nil {
		t.Fatalf("WriteAt whole chunk: %v", err)
	}
	got := AlignedBuffer(srChunk)
	if err := c.ReadAt(1, 0, got); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("whole-chunk read mismatch")
	}
	if idx := c.Index(); len(idx.Dirty) != 1 || idx.Dirty[0] != 1 {
		t.Fatalf("dirty set: %v", idx.Dirty)
	}
}

func TestReadAtMissAndZero(t *testing.T) {
	c := newSubCache(t, srSize)
	// An untouched chunk is a miss.
	if err := c.ReadAt(2, 0, AlignedBuffer(4096)); !errors.Is(err, ErrMiss) {
		t.Fatalf("ReadAt untouched = %v, want ErrMiss", err)
	}
	// A trimmed chunk reads as zeros.
	if err := c.MarkZero(2); err != nil {
		t.Fatalf("MarkZero: %v", err)
	}
	z := filled(4096, 0xff)
	if err := c.ReadAt(2, 0, z); err != nil {
		t.Fatalf("ReadAt zero chunk: %v", err)
	}
	if !bytes.Equal(z, make([]byte, 4096)) {
		t.Fatal("trimmed chunk did not read as zeros")
	}
}

func TestWriteAtEvictsCleanKeepsDirty(t *testing.T) {
	c := newSubCache(t, 2*srChunk) // room for two chunks
	// A dirty, partially-written chunk is pinned.
	if err := c.WriteAt(0, 0, filled(4096, 0x11)); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if err := c.Insert(1, filled(srChunk, 0x22)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	// Inserting a third chunk must evict the clean one, not the dirty one.
	if err := c.Insert(2, filled(srChunk, 0x33)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if !c.Has(0) {
		t.Fatal("the dirty partially-written chunk was evicted")
	}
	if c.Has(1) {
		t.Fatal("the least-recently-used clean chunk was not evicted")
	}
	got := AlignedBuffer(4096)
	if err := c.ReadAt(0, 0, got); err != nil || got[0] != 0x11 {
		t.Fatalf("the dirty chunk's data is gone: %v", err)
	}
}

func TestWriteAtBoundaryCopiesWholeChunk(t *testing.T) {
	c := newSubCache(t, srSize)
	base := filled(srChunk, 0xAA)
	if err := c.WriteAt(0, 0, base); err != nil {
		t.Fatalf("WriteAt base: %v", err)
	}
	if _, err := c.SetBoundary(); err != nil {
		t.Fatalf("SetBoundary: %v", err)
	}
	// A sub-range write to a boundary chunk must copy the whole pre-write chunk
	// first, so the frozen image is unchanged.
	if err := c.WriteAt(0, 0, filled(4096, 0xBB)); err != nil {
		t.Fatalf("WriteAt sub-range: %v", err)
	}
	frozen := AlignedBuffer(srChunk)
	if err := c.BoundaryChunk(0, frozen); err != nil {
		t.Fatalf("BoundaryChunk: %v", err)
	}
	if !bytes.Equal(frozen, base) {
		t.Fatal("the frozen boundary copy changed under a sub-range write")
	}
	// The current image reflects the new write.
	cur := AlignedBuffer(4096)
	if err := c.ReadAt(0, 0, cur); err != nil || cur[0] != 0xBB {
		t.Fatalf("current image did not take the sub-range write: %v", err)
	}
}

// TestWriteAtDurabilityAcrossConcurrentSync proves a FLUSH landing while a
// WriteAt's pwrite is in flight cannot drop the write's durability obligation.
// With the mark-before-pwrite ordering the gap Sync cleared unsynced[0] and
// WriteAt returned (the write ACKed) with the obligation gone; a later FLUSH
// would never fdatasync the new data, which a node power loss then loses. With
// the mark-after-pwrite ordering unsynced[0] is set when WriteAt returns, so
// the next Sync covers it.
func TestWriteAtDurabilityAcrossConcurrentSync(t *testing.T) {
	c := newSubCache(t, srSize)
	// Chunk 0 is a resident dirty chunk whose data is already durable.
	if err := c.WriteAt(0, 0, filled(srChunk, 0x11)); err != nil {
		t.Fatal(err)
	}
	if err := c.Sync(); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	_, pendingBefore := c.unsynced[0]
	c.mu.Unlock()
	if pendingBefore {
		t.Fatal("precondition: chunk 0 should be synced before the test write")
	}

	// A FLUSH lands in the window between WriteAt's lock release and its pwrite.
	c.gapHook = func() {
		if err := c.Sync(); err != nil {
			t.Errorf("gap Sync: %v", err)
		}
	}
	if err := c.WriteAt(0, 0, filled(srChunk, 0x22)); err != nil {
		t.Fatal(err)
	}
	c.gapHook = nil

	// The write is ACKed (WriteAt returned); chunk 0 must still be unsynced so a
	// later FLUSH fdatasyncs the new data.
	c.mu.Lock()
	_, pending := c.unsynced[0]
	c.mu.Unlock()
	if !pending {
		t.Fatal("a FLUSH during WriteAt dropped the write's durability obligation")
	}
}

func TestWriteAtFullCacheRefuses(t *testing.T) {
	c := newSubCache(t, srChunk) // one chunk of dirty capacity
	if err := c.WriteAt(0, 0, filled(srChunk, 1)); err != nil {
		t.Fatalf("first WriteAt: %v", err)
	}
	// A second new dirty chunk cannot fit.
	if err := c.WriteAt(1, 0, filled(4096, 2)); !errors.Is(err, ErrFull) {
		t.Fatalf("WriteAt into a full cache = %v, want ErrFull", err)
	}
}
