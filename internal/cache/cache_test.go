package cache

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const testChunk = 8192

func fill(b byte) []byte {
	buf := AlignedBuffer(testChunk)
	for i := range buf {
		buf[i] = b
	}
	return buf
}

func tmpCache(t *testing.T, bound int64) (*Cache, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "cache")
	c, err := Create(dir, Index{Volume: "pvc-1", Size: 16 * testChunk, Chunk: testChunk}, bound)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, dir
}

func mustRead(t *testing.T, c *Cache, i uint32) []byte {
	t.Helper()
	buf := AlignedBuffer(testChunk)
	if err := c.Read(i, buf); err != nil {
		t.Fatalf("Read(%d): %v", i, err)
	}
	return buf
}

func mustWrite(t *testing.T, c *Cache, i uint32, b byte) {
	t.Helper()
	if err := c.Write(i, fill(b)); err != nil {
		t.Fatalf("Write(%d): %v", i, err)
	}
}

func equalU32(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] { //nolint:gosec // lengths are equal
			return false
		}
	}
	return true
}

func TestCreateThenOpenRoundTripsIndex(t *testing.T) {
	c, dir := tmpCache(t, 1<<30)
	mustWrite(t, c, 3, 3)
	mustWrite(t, c, 1, 1)
	if _, err := c.SetBoundary(); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, c, 1, 9) // copy-on-write
	if err := c.MarkZero(5); err != nil {
		t.Fatal(err)
	}
	want := c.Index()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	d, err := Open(dir, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	got := d.Index()
	if got.Volume != "pvc-1" || got.Size != 16*testChunk || got.Chunk != testChunk {
		t.Fatalf("index fields: %+v", got)
	}
	if !equalU32(got.Dirty, want.Dirty) || !equalU32(got.Boundary, want.Boundary) || !equalU32(got.Zero, want.Zero) {
		t.Fatalf("got %+v want %+v", got, want)
	}
	if !bytes.Equal(mustRead(t, d, 1), fill(9)) || !bytes.Equal(mustRead(t, d, 3), fill(3)) {
		t.Fatal("chunk contents changed across reopen")
	}
	// the frozen copy survives the reopen too
	b := AlignedBuffer(testChunk)
	if err := d.BoundaryChunk(1, b); err != nil || !bytes.Equal(b, fill(1)) {
		t.Fatalf("boundary chunk after reopen: %v", err)
	}
}

func TestWriteMarksDirty(t *testing.T) {
	c, _ := tmpCache(t, 1<<30)
	if c.Has(2) {
		t.Fatal("empty cache has chunk")
	}
	mustWrite(t, c, 2, 7)
	mustWrite(t, c, 2, 8)
	if !c.Has(2) || !equalU32(c.Index().Dirty, []uint32{2}) {
		t.Fatalf("dirty = %v", c.Index().Dirty)
	}
	if !bytes.Equal(mustRead(t, c, 2), fill(8)) {
		t.Fatal("read back differs")
	}
	if err := c.Write(99, fill(1)); err == nil {
		t.Fatal("write past the end of the volume succeeded")
	}
	if err := c.Write(2, make([]byte, 10)); err == nil {
		t.Fatal("short buffer accepted")
	}
}

func TestBoundaryCopiesOnWrite(t *testing.T) {
	c, dir := tmpCache(t, 1<<30)
	mustWrite(t, c, 1, 1)
	mustWrite(t, c, 4, 4)
	set, err := c.SetBoundary()
	if err != nil {
		t.Fatal(err)
	}
	if !equalU32(set, []uint32{1, 4}) {
		t.Fatalf("boundary = %v", set)
	}
	if _, err := c.SetBoundary(); !errors.Is(err, ErrBoundaryActive) {
		t.Fatalf("second SetBoundary: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "b", "1")); !os.IsNotExist(err) {
		t.Fatalf("b/1 exists before any write: %v", err)
	}
	mustWrite(t, c, 1, 2)
	mustWrite(t, c, 1, 3) // second write must not re-copy
	if _, err := os.Stat(filepath.Join(dir, "b", "1")); err != nil {
		t.Fatalf("b/1 missing after write: %v", err)
	}
	b := AlignedBuffer(testChunk)
	if err := c.BoundaryChunk(1, b); err != nil || !bytes.Equal(b, fill(1)) {
		t.Fatalf("frozen chunk changed: %v", err)
	}
	if !bytes.Equal(mustRead(t, c, 1), fill(3)) {
		t.Fatal("live chunk not updated")
	}
	if err := c.BoundaryChunk(4, b); err != nil || !bytes.Equal(b, fill(4)) {
		t.Fatalf("unmodified frozen chunk: %v", err)
	}
	// a chunk dirty only after the boundary is not copied
	mustWrite(t, c, 7, 7)
	if _, err := os.Stat(filepath.Join(dir, "b", "7")); !os.IsNotExist(err) {
		t.Fatal("chunk outside the boundary was copied")
	}
	if err := c.BoundaryChunk(7, b); err == nil {
		t.Fatal("BoundaryChunk of a non-member succeeded")
	}
}

func TestBoundaryOfZeroChunkReadsZeros(t *testing.T) {
	c, _ := tmpCache(t, 1<<30)
	mustWrite(t, c, 1, 1)
	if err := c.MarkZero(1); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetBoundary(); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, c, 1, 5)
	b := fill(0xff)
	if err := c.BoundaryChunk(1, b); err != nil || !bytes.Equal(b, make([]byte, testChunk)) {
		t.Fatalf("frozen zero chunk: %v", err)
	}
	// and the reverse: trimming after the boundary keeps the frozen bytes
	mustWrite(t, c, 2, 2)
	c.ClearBoundary(true)
	if _, err := c.SetBoundary(); err != nil {
		t.Fatal(err)
	}
	if err := c.MarkZero(2); err != nil {
		t.Fatal(err)
	}
	if err := c.BoundaryChunk(2, b); err != nil || !bytes.Equal(b, fill(2)) {
		t.Fatalf("frozen chunk after trim: %v", err)
	}
}

func TestClearBoundaryPublishedDropsDirty(t *testing.T) {
	c, dir := tmpCache(t, 1<<30)
	mustWrite(t, c, 1, 1)
	mustWrite(t, c, 4, 4)
	if _, err := c.SetBoundary(); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, c, 1, 2) // written again since the boundary
	c.ClearBoundary(true)
	idx := c.Index()
	if !equalU32(idx.Dirty, []uint32{1}) || idx.Boundary != nil {
		t.Fatalf("index after publish: %+v", idx)
	}
	if _, err := os.Stat(filepath.Join(dir, "b", "1")); !os.IsNotExist(err) {
		t.Fatal("b/ not cleared")
	}
	if !c.Has(4) || !bytes.Equal(mustRead(t, c, 4), fill(4)) {
		t.Fatal("published chunk should stay as a clean cache entry")
	}
	if !bytes.Equal(mustRead(t, c, 1), fill(2)) {
		t.Fatal("rewritten chunk lost")
	}
	if _, err := c.SetBoundary(); err != nil {
		t.Fatalf("SetBoundary after clear: %v", err)
	}
}

func TestClearBoundaryUnpublishedMergesBack(t *testing.T) {
	c, dir := tmpCache(t, 1<<30)
	mustWrite(t, c, 1, 1)
	mustWrite(t, c, 4, 4)
	if _, err := c.SetBoundary(); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, c, 1, 2)
	mustWrite(t, c, 6, 6)
	c.ClearBoundary(false)
	idx := c.Index()
	if !equalU32(idx.Dirty, []uint32{1, 4, 6}) || idx.Boundary != nil {
		t.Fatalf("index after abandon: %+v", idx)
	}
	if _, err := os.Stat(filepath.Join(dir, "b", "1")); !os.IsNotExist(err) {
		t.Fatal("b/ not cleared")
	}
	if !bytes.Equal(mustRead(t, c, 1), fill(2)) {
		t.Fatal("latest bytes lost")
	}
}

func TestInsertEvictsCleanLRU(t *testing.T) {
	c, _ := tmpCache(t, 3*testChunk)
	for i := uint32(0); i < 3; i++ {
		if err := c.Insert(i, fill(byte(i+1))); err != nil {
			t.Fatal(err)
		}
	}
	mustRead(t, c, 0) // 0 is now most recent; 1 is least
	if err := c.Insert(3, fill(4)); err != nil {
		t.Fatal(err)
	}
	if c.Has(1) || !c.Has(0) || !c.Has(2) || !c.Has(3) {
		t.Fatalf("resident: 0=%v 1=%v 2=%v 3=%v", c.Has(0), c.Has(1), c.Has(2), c.Has(3))
	}
	buf := AlignedBuffer(testChunk)
	if err := c.Read(1, buf); !errors.Is(err, ErrMiss) {
		t.Fatalf("evicted chunk: %v", err)
	}
	// a dirty chunk is pinned: it displaces clean ones, never the reverse
	mustWrite(t, c, 8, 8)
	mustWrite(t, c, 9, 9)
	if !c.Has(8) || !c.Has(9) {
		t.Fatal("dirty chunk lost")
	}
	live := 0
	for i := uint32(0); i < 4; i++ {
		if c.Has(i) {
			live++
		}
	}
	if live != 1 {
		t.Fatalf("clean chunks resident = %d, want 1", live)
	}
}

func TestInsertFullWhenDirtyExceedsBound(t *testing.T) {
	c, dir := tmpCache(t, 2*testChunk)
	mustWrite(t, c, 1, 1)
	mustWrite(t, c, 2, 2)
	mustWrite(t, c, 2, 3) // not counted twice
	if err := c.Insert(5, fill(5)); !errors.Is(err, ErrFull) {
		t.Fatalf("Insert = %v, want ErrFull", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "c", "5")); !os.IsNotExist(err) {
		t.Fatal("ErrFull wrote the chunk")
	}
	if c.Has(5) {
		t.Fatal("ErrFull left the chunk resident")
	}
	if err := c.Write(6, fill(6)); !errors.Is(err, ErrFull) {
		t.Fatalf("Write of a new chunk = %v, want ErrFull", err)
	}
	mustWrite(t, c, 1, 4) // already dirty: always allowed
}

func TestMarkZero(t *testing.T) {
	c, dir := tmpCache(t, 1<<30)
	mustWrite(t, c, 2, 2)
	if err := c.MarkZero(2); err != nil {
		t.Fatal(err)
	}
	if err := c.Sync(); err != nil { // the unlink follows the next index flush
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "c", "2")); !os.IsNotExist(err) {
		t.Fatal("zero chunk kept its file")
	}
	idx := c.Index()
	if !equalU32(idx.Dirty, []uint32{2}) || !equalU32(idx.Zero, []uint32{2}) {
		t.Fatalf("index: %+v", idx)
	}
	if !bytes.Equal(mustRead(t, c, 2), make([]byte, testChunk)) {
		t.Fatal("zero chunk not zero")
	}
	mustWrite(t, c, 2, 9)
	if len(c.Index().Zero) != 0 || !bytes.Equal(mustRead(t, c, 2), fill(9)) {
		t.Fatal("write did not clear the zero flag")
	}
	if err := c.MarkZero(99); err == nil {
		t.Fatal("MarkZero out of range succeeded")
	}
}

func TestGrow(t *testing.T) {
	c, dir := tmpCache(t, 1<<30)
	if err := c.Grow(8 * testChunk); err == nil {
		t.Fatal("shrinking accepted")
	}
	if err := c.Grow(20 * testChunk); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, c, 19, 1)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	d, err := Open(dir, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	if d.Index().Size != 20*testChunk {
		t.Fatalf("size = %d", d.Index().Size)
	}
	buf := AlignedBuffer(testChunk)
	if err := d.Read(18, buf); !errors.Is(err, ErrMiss) {
		t.Fatalf("grown chunk: %v", err)
	}
}

func TestOpenWithoutIndexIsErrNoIndex(t *testing.T) {
	if _, err := Open(filepath.Join(t.TempDir(), "nothing"), 1); !errors.Is(err, ErrNoIndex) {
		t.Fatalf("missing dir: %v", err)
	}
	if _, err := Open(t.TempDir(), 1); !errors.Is(err, ErrNoIndex) {
		t.Fatalf("empty dir: %v", err)
	}
}

func TestCorruptIndexIsRejected(t *testing.T) {
	cases := map[string]string{
		"garbage":        "not json",
		"zero chunk":     `{"Volume":"v","Size":8192,"Chunk":0}`,
		"unaligned":      `{"Volume":"v","Size":8192,"Chunk":100}`,
		"dirty past end": `{"Volume":"v","Size":8192,"Chunk":4096,"Dirty":[7]}`,
		"unsorted":       `{"Volume":"v","Size":81920,"Chunk":4096,"Dirty":[3,1]}`,
		"zero not dirty": `{"Volume":"v","Size":81920,"Chunk":4096,"Zero":[1]}`,
		"dirty no file":  `{"Volume":"v","Size":81920,"Chunk":4096,"Dirty":[1]}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.Mkdir(filepath.Join(dir, "c"), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "index.json"), []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := Open(dir, 1<<20)
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("Open = %v, want ErrCorrupt", err)
			}
		})
	}
}

func TestReadMissWhenAbsent(t *testing.T) {
	c, _ := tmpCache(t, 1<<30)
	if err := c.Read(3, AlignedBuffer(testChunk)); !errors.Is(err, ErrMiss) {
		t.Fatalf("Read = %v", err)
	}
}

func TestResumeAfterCrash(t *testing.T) {
	c, dir := tmpCache(t, 1<<30)
	mustWrite(t, c, 1, 1)
	mustWrite(t, c, 2, 2)
	if err := c.Insert(3, fill(3)); err != nil {
		t.Fatal(err)
	}
	if err := c.MarkZero(4); err != nil {
		t.Fatal(err)
	}
	if err := c.Sync(); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash: the process dies after Sync without Close, leaving a
	// torn temporary file behind.
	if err := os.WriteFile(filepath.Join(dir, "c", ".9.tmp"), []byte("torn"), 0o600); err != nil {
		t.Fatal(err)
	}
	d, err := Open(dir, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	if !equalU32(d.Index().Dirty, []uint32{1, 2, 4}) || !equalU32(d.Index().Zero, []uint32{4}) {
		t.Fatalf("index: %+v", d.Index())
	}
	if !bytes.Equal(mustRead(t, d, 2), fill(2)) {
		t.Fatal("dirty chunk not resumed")
	}
	if d.Has(3) {
		t.Fatal("a resumed cache must start cold for clean data")
	}
	if _, err := os.Stat(filepath.Join(dir, "c", ".9.tmp")); !os.IsNotExist(err) {
		t.Fatal("temporary file survived Open")
	}
}

func TestIndexFlushesWithoutSync(t *testing.T) {
	c, dir := tmpCache(t, 1<<30)
	mustWrite(t, c, 1, 1)
	deadlineWait(t, func() bool {
		b, err := os.ReadFile(filepath.Join(dir, "index.json")) //nolint:gosec // test path
		return err == nil && bytes.Contains(b, []byte(`"Dirty"`)) && !bytes.Contains(b, []byte(`"Dirty":null`)) && !bytes.Contains(b, []byte(`"Dirty": null`))
	})
}

func TestDirectIOAlignment(t *testing.T) {
	// A directory on the worktree's own filesystem; tmpfs refuses O_DIRECT.
	dir, err := os.MkdirTemp(".", "direct-") // ignored by .gitignore
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	c, err := Create(filepath.Join(dir, "cache"), Index{Volume: "v", Size: 4 * testChunk, Chunk: testChunk}, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if !c.DirectIO {
		t.Skip("filesystem refuses O_DIRECT; the direct-I/O case is skipped here (run on a real disk)")
	}
	mustWrite(t, c, 1, 1)
	if !bytes.Equal(mustRead(t, c, 1), fill(1)) {
		t.Fatal("direct round trip differs")
	}
}

func TestAlignmentIsEnforced(t *testing.T) {
	c, _ := tmpCache(t, 1<<30)
	raw := make([]byte, testChunk+4096)
	off := 0
	for addr(raw[off:])%4096 == 0 {
		off++ // find a misaligned start
	}
	mis := raw[off : off+testChunk]
	if err := c.Write(1, mis); err == nil {
		t.Fatal("misaligned write accepted")
	}
	if err := c.Read(1, mis); err == nil {
		t.Fatal("misaligned read accepted")
	}
	if err := c.Insert(1, mis); err == nil {
		t.Fatal("misaligned insert accepted")
	}
	if a := AlignedBuffer(10000); len(a) != 10000 || addr(a)%4096 != 0 {
		t.Fatal("AlignedBuffer not aligned")
	}
}

func TestRemoveDeletesEverything(t *testing.T) {
	c, dir := tmpCache(t, 1<<30)
	mustWrite(t, c, 1, 1)
	if _, err := c.SetBoundary(); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, c, 1, 2)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := Remove(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("directory remains")
	}
}

func TestRemoveRefusesAForeignDirectory(t *testing.T) {
	dir := t.TempDir()
	keep := filepath.Join(dir, "precious")
	if err := os.WriteFile(keep, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := Remove(dir)
	if err == nil || !bytes.Contains([]byte(err.Error()), []byte(dir)) {
		t.Fatalf("Remove = %v, want an error naming %s", err, dir)
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatal("foreign file removed")
	}
	if err := Remove(filepath.Join(dir, "absent")); err == nil {
		t.Fatal("Remove of a missing directory succeeded")
	}
}

func TestUseAfterClose(t *testing.T) {
	c, _ := tmpCache(t, 1<<30)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Write(1, fill(1)); err == nil {
		t.Fatal("write after close")
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestCreateRefusesExisting(t *testing.T) {
	_, dir := tmpCache(t, 1<<30)
	if _, err := Create(dir, Index{Volume: "v", Size: testChunk, Chunk: testChunk}, 1); err == nil {
		t.Fatal("Create over an existing cache succeeded")
	}
	if _, err := Create(filepath.Join(t.TempDir(), "x"), Index{Volume: "v", Size: testChunk, Chunk: 1000}, 1); err == nil {
		t.Fatal("bad chunk size accepted")
	}
}

func TestSetBase(t *testing.T) {
	c, dir := tmpCache(t, 1<<30)
	c.SetBase("1.2", 1)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	d, err := Open(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	if i := d.Index(); i.Base != "1.2" || i.Epoch != 1 {
		t.Fatalf("%+v", i)
	}
}

// crashCopy snapshots dir as a crash would leave it: whatever is on disk now,
// with no Close, Sync or pending timer applied.
func crashCopy(t *testing.T, dir string) string {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "crashed")
	err := filepath.Walk(dir, func(p string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dir, p)
		if fi.IsDir() {
			return os.MkdirAll(filepath.Join(dst, rel), 0o700)
		}
		b, err := os.ReadFile(p) //nolint:gosec // test path
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dst, rel), b, 0o600) //nolint:gosec // test path
	})
	if err != nil {
		t.Fatal(err)
	}
	return dst
}

func holdFlush(t *testing.T) {
	t.Helper()
	old := flushDelay
	flushDelay = time.Hour
	t.Cleanup(func() { flushDelay = old })
}

func TestCrashBeforeIndexFlushLosesWriteNotStaleClean(t *testing.T) {
	holdFlush(t)
	c, dir := tmpCache(t, 1<<30)
	if err := c.Insert(3, fill(3)); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, c, 3, 9) // clean-resident chunk rewritten; index not yet flushed
	d, err := Open(crashCopy(t, dir), 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	if d.Has(3) {
		t.Fatal("unindexed chunk file adopted as clean")
	}
	if err := d.Read(3, AlignedBuffer(testChunk)); !errors.Is(err, ErrMiss) {
		t.Fatalf("Read = %v, want ErrMiss", err)
	}
}

func TestCrashAfterMarkZeroReopens(t *testing.T) {
	holdFlush(t)
	c, dir := tmpCache(t, 1<<30)
	mustWrite(t, c, 2, 2)
	if err := c.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := c.MarkZero(2); err != nil {
		t.Fatal(err)
	}
	// Crash before any flush: the trim is lost, but the cache must open.
	d, err := Open(crashCopy(t, dir), 1<<30)
	if err != nil {
		t.Fatalf("Open after crash: %v", err)
	}
	_ = d.Close()
}

func TestCrashBetweenFlushAndDeferredUnlinkReadsZeros(t *testing.T) {
	holdFlush(t)
	c, dir := tmpCache(t, 1<<30)
	mustWrite(t, c, 2, 2)
	if err := c.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := c.MarkZero(2); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "c", "2")); err != nil {
		t.Fatalf("unlink was not deferred: %v", err)
	}
	// The index recording Zero reaches disk; the process dies before the unlink.
	c.mu.Lock()
	err := writeIndex(dir, c.snapshot())
	c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	d, err := Open(crashCopy(t, dir), 1<<30)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	if !bytes.Equal(mustRead(t, d, 2), make([]byte, testChunk)) {
		t.Fatal("trimmed chunk not zero after crash")
	}
}

func TestWriteCancelsPendingUnlink(t *testing.T) {
	holdFlush(t)
	c, dir := tmpCache(t, 1<<30)
	mustWrite(t, c, 2, 2)
	if err := c.MarkZero(2); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, c, 2, 5)
	if err := c.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "c", "2")); err != nil {
		t.Fatalf("rewritten chunk was unlinked: %v", err)
	}
	if !bytes.Equal(mustRead(t, c, 2), fill(5)) {
		t.Fatal("rewritten chunk lost")
	}
}

func TestMarkZeroBatchesIndexWrites(t *testing.T) {
	holdFlush(t)
	const n = 10000
	dir := filepath.Join(t.TempDir(), "cache")
	c, err := Create(dir, Index{Volume: "v", Size: n * ioAlign, Chunk: ioAlign}, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	for i := uint32(0); i < n; i++ {
		if err := c.MarkZero(i); err != nil {
			t.Fatal(err)
		}
	}
	if c.indexWrites != 0 {
		t.Fatalf("index written %d times during %d trims, want 0", c.indexWrites, n)
	}
	if err := c.Sync(); err != nil {
		t.Fatal(err)
	}
	if c.indexWrites != 1 || len(c.Index().Zero) != n {
		t.Fatalf("after Sync: %d index writes, %d zero chunks", c.indexWrites, len(c.Index().Zero))
	}
}

func TestPublishedChunkKeepsFsyncObligation(t *testing.T) {
	c, _ := tmpCache(t, 1<<30)
	mustWrite(t, c, 1, 1)
	if _, err := c.SetBoundary(); err != nil {
		t.Fatal(err)
	}
	c.ClearBoundary(true)
	if _, ok := c.unsynced[1]; !ok {
		t.Fatal("published chunk dropped from the unsynced set before its bytes were fsynced")
	}
	if err := c.Sync(); err != nil {
		t.Fatal(err)
	}
	if len(c.unsynced) != 0 {
		t.Fatal("Sync left obligations")
	}
}

func TestCreateIgnoresLeftoverFiles(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "cache")
	for _, d := range []string{"c", "b"} {
		if err := os.MkdirAll(filepath.Join(dir, d), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, d, "5"), fill(7), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	c, err := Create(dir, Index{Volume: "v", Size: 16 * testChunk, Chunk: testChunk}, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "c", "5")); !os.IsNotExist(err) {
		t.Fatalf("Create kept the leftover c/5: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	d, err := Open(dir, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	if d.Has(5) {
		t.Fatal("leftover chunk adopted")
	}
	if err := d.Read(5, AlignedBuffer(testChunk)); !errors.Is(err, ErrMiss) {
		t.Fatalf("Read = %v", err)
	}
}
