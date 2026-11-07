package cache

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

// These tests pin the cost and the ordering of the durable-flush path: what a
// FLUSH or a FUA write must put on stable storage before it is acknowledged,
// and, just as much, what it must not bother with. They replace the
// process-wide fdatasync and syncDir seams, so they do not run in parallel.

// syncLog records every durability syscall the cache issues, in order, as
// "data:<chunk>", "dir:c" or "dir:root" (the last is the end of an index
// write, which fsyncs the cache root after the rename). hook, when set, runs
// before each is performed, which is where a crash is simulated.
type syncLog struct {
	events []string
	hook   func(ev string)
}

func watchSyncs(t *testing.T) *syncLog {
	t.Helper()
	l := &syncLog{}
	oldData, oldDir := fdatasync, syncDir
	fdatasync = func(fd int) error {
		// The chunk number is not known to the seam; name it by its file.
		name, _ := filepath.EvalSymlinks(fmt.Sprintf("/proc/self/fd/%d", fd))
		ev := "data:" + filepath.Base(name)
		l.events = append(l.events, ev)
		if l.hook != nil {
			l.hook(ev)
		}
		return oldData(fd)
	}
	syncDir = func(dir string) error {
		ev := "dir:root"
		if filepath.Base(dir) == "c" {
			ev = "dir:c"
		} else if filepath.Base(dir) == "b" {
			ev = "dir:b"
		}
		l.events = append(l.events, ev)
		if l.hook != nil {
			l.hook(ev)
		}
		return oldDir(dir)
	}
	t.Cleanup(func() { fdatasync, syncDir = oldData, oldDir })
	return l
}

func (l *syncLog) reset()         { l.events = nil }
func (l *syncLog) String() string { return strings.Join(l.events, " ") }

func (l *syncLog) index(ev string) int {
	for k, e := range l.events {
		if e == ev {
			return k
		}
	}
	return -1
}

const fl4k = 4096

func wr(t *testing.T, c *Cache, i uint32, b byte) {
	t.Helper()
	if err := c.WriteAt(i, 0, filled(fl4k, b), nil, true); err != nil {
		t.Fatalf("WriteAt(%d): %v", i, err)
	}
}

// flCache is a cache of testChunk-sized chunks with the index timer held back,
// so only Sync, SyncChunks and the boundary calls write the index.
func flCache(t *testing.T) (*Cache, string) {
	t.Helper()
	holdFlush(t)
	return tmpCache(t, 1<<30)
}

func TestOverwriteOfDirtyChunkDoesNotRewriteIndex(t *testing.T) {
	c, _ := flCache(t)
	wr(t, c, 1, 1)
	if err := c.Sync(); err != nil {
		t.Fatal(err)
	}
	l := watchSyncs(t)
	writes := c.indexWrites
	wr(t, c, 1, 2) // chunk 1 is already dirty and already in the index on disk
	if err := c.Sync(); err != nil {
		t.Fatal(err)
	}
	if c.indexWrites != writes {
		t.Fatalf("overwrite of a dirty chunk rewrote the index (%d -> %d)", writes, c.indexWrites)
	}
	if got := l.String(); got != "data:1" {
		t.Fatalf("syncs = %q, want only the chunk's fdatasync", got)
	}
}

func TestNewlyDirtyChunkWritesIndexAfterItsData(t *testing.T) {
	c, dir := flCache(t)
	l := watchSyncs(t)
	wr(t, c, 2, 7)
	if err := c.Sync(); err != nil {
		t.Fatal(err)
	}
	// The chunk's bytes and its directory entry are durable before the index
	// that names it, so no index on disk lists a chunk that is not fully there.
	d, dc, dr := l.index("data:2"), l.index("dir:c"), l.index("dir:root")
	if d < 0 || dc < 0 || dr < 0 || d > dc || dc > dr {
		t.Fatalf("order = %q, want data:2, dir:c, then the index (dir:root)", l)
	}
	e, err := Open(crashCopy(t, dir), 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()
	if !equalU32(e.Index().Dirty, []uint32{2}) {
		t.Fatalf("resumed dirty = %v, want [2]", e.Index().Dirty)
	}
}

func TestEmptySyncIssuesNoSyscalls(t *testing.T) {
	c, _ := flCache(t)
	wr(t, c, 1, 1)
	if err := c.Sync(); err != nil {
		t.Fatal(err)
	}
	l := watchSyncs(t)
	writes := c.indexWrites
	for range 3 {
		if err := c.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := c.SyncChunks(1); err != nil {
			t.Fatal(err)
		}
	}
	if len(l.events) != 0 || c.indexWrites != writes {
		t.Fatalf("a FLUSH with nothing new issued %q and %d index writes", l, c.indexWrites-writes)
	}
}

func TestSyncChunksSyncsOnlyItsChunkAndKeepsOthersObligations(t *testing.T) {
	c, dir := flCache(t)
	wr(t, c, 1, 1)
	wr(t, c, 2, 2)
	if err := c.Sync(); err != nil {
		t.Fatal(err)
	}
	wr(t, c, 1, 3)
	wr(t, c, 2, 4)
	l := watchSyncs(t)
	writes := c.indexWrites
	if err := c.SyncChunks(1); err != nil {
		t.Fatal(err)
	}
	if got := l.String(); got != "data:1" || c.indexWrites != writes {
		t.Fatalf("FUA on chunk 1 issued %q and %d index writes, want only its fdatasync", got, c.indexWrites-writes)
	}
	if _, ok := c.unsynced[2]; !ok {
		t.Fatal("FUA on chunk 1 dropped chunk 2's fsync obligation, which a later FLUSH must honour")
	}
	l.reset()
	if err := c.Sync(); err != nil {
		t.Fatal(err)
	}
	if got := l.String(); got != "data:2" {
		t.Fatalf("FLUSH after the FUA issued %q, want chunk 2's fdatasync only", got)
	}
	e, err := Open(crashCopy(t, dir), 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()
	if !bytes.Equal(mustRead(t, e, 2)[:fl4k], filled(fl4k, 4)) {
		t.Fatal("chunk 2's flushed bytes lost")
	}
}

func TestSyncChunksOfNewlyDirtyChunkRecordsItInTheIndex(t *testing.T) {
	c, dir := flCache(t)
	wr(t, c, 3, 9) // never in an index on disk
	l := watchSyncs(t)
	if err := c.SyncChunks(3); err != nil {
		t.Fatal(err)
	}
	d, dc, dr := l.index("data:3"), l.index("dir:c"), l.index("dir:root")
	if d < 0 || dc < 0 || dr < 0 || d > dc || dc > dr {
		t.Fatalf("order = %q, want data:3, dir:c, index", l)
	}
	e, err := Open(crashCopy(t, dir), 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()
	if !equalU32(e.Index().Dirty, []uint32{3}) {
		t.Fatalf("resumed dirty = %v, want [3]", e.Index().Dirty)
	}
}

// A chunk whose index entry the timer already wrote, but whose directory
// entry and bytes are not yet durable, still needs both at a FUA.
func TestSyncChunksSyncsDirectoryEntryTheTimerLeftBehind(t *testing.T) {
	c, _ := flCache(t)
	wr(t, c, 3, 9)
	c.mu.Lock()
	err := c.flushLocked() // what the 100 ms timer does: the index, not the chunk
	c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	l := watchSyncs(t)
	if err := c.SyncChunks(3); err != nil {
		t.Fatal(err)
	}
	if got := l.String(); got != "data:3 dir:c" {
		t.Fatalf("syncs = %q, want the chunk's fdatasync then its directory", got)
	}
}

func TestUnchangedIndexStateDoesNotDirtyIt(t *testing.T) {
	c, _ := flCache(t)
	c.SetBase("m1", 4)
	if err := c.Grow(16 * testChunk * 2); err != nil {
		t.Fatal(err)
	}
	if err := c.Sync(); err != nil {
		t.Fatal(err)
	}
	writes := c.indexWrites
	c.SetBase("m1", 4)
	if err := c.Grow(16 * testChunk * 2); err != nil {
		t.Fatal(err)
	}
	if err := c.Sync(); err != nil {
		t.Fatal(err)
	}
	if c.indexWrites != writes {
		t.Fatal("re-recording the same base, epoch and size rewrote the index")
	}
	c.SetBase("m2", 5)
	if err := c.Sync(); err != nil || c.indexWrites != writes+1 {
		t.Fatalf("a changed base must reach the index (err %v, writes %d)", err, c.indexWrites-writes)
	}
}

// A rewrite of a chunk that was published (so clean in the index) is a new
// dirty chunk: it must reach the index before the FLUSH is acknowledged.
func TestRewriteOfPublishedChunkRecordsItDirty(t *testing.T) {
	c, dir := flCache(t)
	wr(t, c, 1, 1)
	if _, err := c.SetBoundary(); err != nil {
		t.Fatal(err)
	}
	c.ClearBoundary(true)
	if err := c.Sync(); err != nil {
		t.Fatal(err)
	}
	if len(c.Index().Dirty) != 0 {
		t.Fatal("precondition: published chunk should be clean")
	}
	wr(t, c, 1, 2)
	if err := c.SyncChunks(1); err != nil {
		t.Fatal(err)
	}
	e, err := Open(crashCopy(t, dir), 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()
	if !equalU32(e.Index().Dirty, []uint32{1}) {
		t.Fatalf("resumed dirty = %v, want [1]: a rewritten published chunk was resumed clean", e.Index().Dirty)
	}
	if !bytes.Equal(mustRead(t, e, 1)[:fl4k], filled(fl4k, 2)) {
		t.Fatal("acked FUA bytes lost")
	}
}

// A trimmed chunk rewritten then flushed must resume dirty and not Zero.
func TestRewriteOfTrimmedChunkResumesWithItsBytes(t *testing.T) {
	c, dir := flCache(t)
	wr(t, c, 1, 1)
	if err := c.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := c.MarkZero(1); err != nil {
		t.Fatal(err)
	}
	if err := c.Sync(); err != nil {
		t.Fatal(err)
	}
	wr(t, c, 1, 5)
	if err := c.SyncChunks(1); err != nil {
		t.Fatal(err)
	}
	e, err := Open(crashCopy(t, dir), 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()
	if len(e.Index().Zero) != 0 || !bytes.Equal(mustRead(t, e, 1)[:fl4k], filled(fl4k, 5)) {
		t.Fatalf("trimmed-then-rewritten chunk resumed as %+v", e.Index())
	}
}

// TestCrashAtEverySyncPoint runs a script of writes, FUAs and FLUSHes and
// takes a crash image before every durability syscall and after every ack.
// Each image must open, and every chunk whose bytes were acknowledged durable
// before the call in progress began must resume dirty with those bytes (or,
// if a later unacknowledged write to it was in flight, the later ones).
func TestCrashAtEverySyncPoint(t *testing.T) {
	c, dir := flCache(t)
	acked := map[uint32]byte{} // last value acknowledged durable
	type check struct {
		image string
		acked map[uint32]byte
	}
	var checks []check
	record := func() {
		cp := map[uint32]byte{}
		for k, v := range acked {
			cp[k] = v
		}
		checks = append(checks, check{crashCopy(t, dir), cp})
	}
	l := watchSyncs(t)
	l.hook = func(string) { record() }

	step := func(name string, fn func() error, onAck func()) {
		t.Helper()
		if err := fn(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		onAck()
		record()
	}
	noop := func() {}

	wr(t, c, 1, 1)
	wr(t, c, 2, 2)
	step("flush1", c.Sync, func() { acked[1], acked[2] = 1, 2 })
	wr(t, c, 1, 3) // overwrite of a chunk already in the index
	wr(t, c, 3, 4) // new chunk
	step("fua1", func() error { return c.SyncChunks(1) }, func() { acked[1] = 3 })
	step("fua3", func() error { return c.SyncChunks(3) }, func() { acked[3] = 4 })
	wr(t, c, 2, 5)
	step("flush2", c.Sync, func() { acked[2] = 5 })
	delete(acked, 2) // a trim in flight: a crash may show either the bytes or zeros
	if err := c.MarkZero(2); err != nil {
		t.Fatal(err)
	}
	step("flush3", c.Sync, noop)
	wr(t, c, 2, 6)
	step("fua2", func() error { return c.SyncChunks(2) }, func() { acked[2] = 6 })
	step("boundary", func() error { _, err := c.SetBoundary(); return err }, noop)
	wr(t, c, 3, 7) // copy-on-write of a boundary chunk
	step("fua3b", func() error { return c.SyncChunks(3) }, func() { acked[3] = 7 })
	delete(acked, 1) // published: the store holds these, so clean is right
	delete(acked, 2)
	c.ClearBoundary(true)
	step("flush4", c.Sync, noop)

	if len(checks) < 10 {
		t.Fatalf("only %d crash points exercised", len(checks))
	}
	for k, ck := range checks {
		e, err := Open(ck.image, 1<<30)
		if err != nil {
			t.Fatalf("crash point %d: Open: %v", k, err)
		}
		dirty := map[uint32]bool{}
		for _, v := range e.Index().Dirty {
			dirty[v] = true
		}
		for i, v := range ck.acked {
			if !dirty[i] {
				t.Errorf("crash point %d: acked chunk %d resumed clean or base-backed", k, i)
				continue
			}
			if got := mustRead(t, e, i)[0]; got != v {
				// A later, unacknowledged write may legitimately be visible.
				if got < v {
					t.Errorf("crash point %d: chunk %d has %d, acked %d", k, i, got, v)
				}
			}
		}
		_ = e.Close()
	}
}

// BenchmarkGuestFsync models what one guest fsync costs the cache: a data
// write and a metadata write to chunks already dirty, then the jbd2 commit's
// FLUSH and a FUA write of the commit block. The first pass over the chunks
// makes them dirty and is outside the timed loop, as in a steady-state fsync
// workload. Run with -bench GuestFsync -benchtime 1000x.
func BenchmarkGuestFsync(b *testing.B) {
	dir := filepath.Join(b.TempDir(), "cache")
	c, err := Create(dir, Index{Volume: "v", Size: 8 * srChunk, Chunk: srChunk}, 1<<30)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	p := filled(fl4k, 1)
	w := func(i uint32, off int64) {
		if err := c.WriteAt(i, off, p, nil, true); err != nil {
			b.Fatal(err)
		}
	}
	for i := range uint32(3) {
		w(i, 0)
	}
	if err := c.Sync(); err != nil {
		b.Fatal(err)
	}
	var data, dirs int
	oldData, oldDir := fdatasync, syncDir
	fdatasync = func(fd int) error { data++; return oldData(fd) }
	syncDir = func(d string) error { dirs++; return oldDir(d) }
	defer func() { fdatasync, syncDir = oldData, oldDir }()
	writes := c.indexWrites
	b.ResetTimer()
	for n := range b.N {
		off := int64(n%200) * fl4k
		w(0, off)                        // file data
		w(1, off)                        // inode / bitmap
		w(2, off)                        // journal blocks
		if err := c.Sync(); err != nil { // FLUSH
			b.Fatal(err)
		}
		w(2, off+fl4k)                          // commit block
		if err := c.SyncChunks(2); err != nil { // FUA
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(data)/float64(b.N), "fdatasync/op")
	b.ReportMetric(float64(dirs)/float64(b.N), "dirsync/op")
	b.ReportMetric(float64(c.indexWrites-writes)/float64(b.N), "index/op")
}
