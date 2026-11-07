package cache

// Review probes, kept as regression tests. plModel models POWER LOSS, not a
// process crash: a chunk file's bytes survive only as of its last fdatasync, a
// directory entry only as of its directory's fsync, index.json only as of the
// root fsync at the end of writeIndex.

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

type plModel struct {
	dir     string
	data    map[uint64][]byte            // inode -> bytes at last fdatasync
	entries map[string]map[string]uint64 // "c" -> name -> inode, at last fsync of c/
	snap    map[string]map[string][]byte // "b", "root" -> name -> content at last fsync
	events  []string
	hook    func(ev string) // runs after each durability syscall has taken effect
}

func ino(fi os.FileInfo) uint64 { return fi.Sys().(*syscall.Stat_t).Ino }

func (m *plModel) snapDir(which string) {
	d := m.dir
	if which != "root" {
		d = filepath.Join(m.dir, which)
	}
	es, _ := os.ReadDir(d)
	if which == "c" {
		m.entries["c"] = map[string]uint64{}
		for _, e := range es {
			fi, _ := e.Info()
			m.entries["c"][e.Name()] = ino(fi)
		}
		return
	}
	m.snap[which] = map[string][]byte{}
	for _, e := range es {
		if e.IsDir() {
			continue
		}
		b, _ := os.ReadFile(filepath.Join(d, e.Name())) //nolint:gosec // test path
		m.snap[which][e.Name()] = b
	}
}

func watchPower(t *testing.T, dir string) *plModel {
	t.Helper()
	m := &plModel{dir: dir, data: map[uint64][]byte{}, entries: map[string]map[string]uint64{}, snap: map[string]map[string][]byte{}}
	// Everything present now is taken as durable (Create fsynced the index).
	m.snapDir("root")
	m.snapDir("b")
	m.snapDir("c")
	for name, in := range m.entries["c"] {
		b, _ := os.ReadFile(filepath.Join(dir, "c", name)) //nolint:gosec // test path
		m.data[in] = b
	}
	oldData, oldDir := fdatasync, syncDir
	fdatasync = func(fd int) error {
		err := oldData(fd)
		var st syscall.Stat_t
		_ = syscall.Fstat(fd, &st)
		b := make([]byte, st.Size)
		_, _ = syscall.Pread(fd, b, 0) // chunk files may be O_DIRECT: fall back below
		if name, e := filepath.EvalSymlinks(fmt.Sprintf("/proc/self/fd/%d", fd)); e == nil {
			if rb, e := os.ReadFile(name); e == nil {
				b = rb
			}
		}
		m.data[st.Ino] = b
		name, _ := filepath.EvalSymlinks(fmt.Sprintf("/proc/self/fd/%d", fd))
		m.events = append(m.events, "data:"+filepath.Base(name))
		if m.hook != nil {
			m.hook("data:" + filepath.Base(name))
		}
		return err
	}
	syncDir = func(d string) error {
		err := oldDir(d)
		w := "root"
		if filepath.Base(d) == "c" || filepath.Base(d) == "b" {
			w = filepath.Base(d)
		}
		m.snapDir(w)
		m.events = append(m.events, "dir:"+w)
		if m.hook != nil {
			m.hook("dir:" + w)
		}
		return err
	}
	t.Cleanup(func() { fdatasync, syncDir = oldData, oldDir })
	return m
}

// image writes what a power cut now would leave on disk.
func (m *plModel) image(t *testing.T) string {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "power")
	for _, d := range []string{"c", "b"} {
		if err := os.MkdirAll(filepath.Join(dst, d), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	for n, b := range m.snap["root"] {
		_ = os.WriteFile(filepath.Join(dst, n), b, 0o600)
	}
	for n, b := range m.snap["b"] {
		_ = os.WriteFile(filepath.Join(dst, "b", n), b, 0o600)
	}
	for n, in := range m.entries["c"] {
		_ = os.WriteFile(filepath.Join(dst, "c", n), m.data[in], 0o600) // never fdatasynced: empty
	}
	return dst
}

// abandon is a process crash of c: it is never used again (its Close would
// otherwise sync on cleanup).
func abandon(c *Cache) {
	c.mu.Lock()
	c.closed = true
	if c.timer != nil {
		c.timer.Stop()
	}
	c.mu.Unlock()
}

// P1 (pre-existing): the timer writes an index naming a newly dirty chunk
// before that chunk's directory entry (or size) is durable. A power cut then
// makes Open refuse the WHOLE cache, stranding chunk 1, whose bytes a FLUSH
// acknowledged.
func TestProbePowerLossTimerIndexBeforeChunkEntry(t *testing.T) {
	c, dir := flCache(t)
	m := watchPower(t, dir)
	wr(t, c, 1, 1)
	if err := c.Sync(); err != nil { // FLUSH acked: chunk 1 durable
		t.Fatal(err)
	}
	wr(t, c, 5, 5) // a new chunk, not yet flushed
	c.mu.Lock()
	err := c.flushLocked() // the 100 ms timer
	c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	e, err := Open(m.image(t), 1<<30)
	if err != nil {
		t.Fatalf("power cut after a timer index write: Open: %v (acked chunk 1 stranded)", err)
	}
	defer func() { _ = e.Close() }()
	if mustRead(t, e, 1)[0] != 1 {
		t.Fatal("acked chunk 1 lost")
	}
}

// P2 (regression in this PR, data part pre-existing): decision 51 restarts the
// block server under a live device, so a FLUSH that reaches the new process
// owes the writes the old one acknowledged. Open seeds no obligations, and Sync
// now skips the c/ fsync too, so that FLUSH is a no-op.
func TestProbePowerLossAfterRestartFlushIsNoop(t *testing.T) {
	c, dir := flCache(t)
	m := watchPower(t, dir)
	wr(t, c, 1, 1)
	if err := c.Sync(); err != nil {
		t.Fatal(err)
	}
	wr(t, c, 5, 5) // acknowledged (no FUA); the guest has not flushed yet
	c.mu.Lock()
	err := c.flushLocked() // the timer records 5 dirty
	c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	abandon(c) // blockd killed; systemd restarts it on the same cache
	c2, err := Open(dir, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c2.Close() }()
	m.events = nil
	if err := c2.Sync(); err != nil { // the guest's FLUSH, acked by the new process
		t.Fatal(err)
	}
	t.Logf("syncs issued by the post-restart FLUSH: %q", m.events)
	e, err := Open(m.image(t), 1<<30)
	if err != nil {
		t.Fatalf("power cut after an acked FLUSH: Open: %v", err)
	}
	defer func() { _ = e.Close() }()
	if got := mustRead(t, e, 5); got[0] != 5 {
		t.Fatalf("acked+flushed chunk 5 reads %d after power cut", got[0])
	}
}

// P3 (pre-existing, process crash only): an acknowledged write to a new chunk
// whose index update the crash beat is deleted by Open, and the FLUSH the
// guest then sends to the restarted server succeeds.
func TestProbeProcessCrashLosesAckedWriteAcrossRestart(t *testing.T) {
	c, dir := flCache(t)
	wr(t, c, 5, 5) // acked, not flushed; crash inside the 100 ms window
	abandon(c)
	c2, err := Open(dir, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c2.Close() }()
	if err := c2.Sync(); err != nil { // the guest's FLUSH is acknowledged
		t.Fatal(err)
	}
	if !c2.Has(5) {
		t.Fatal("acked write to chunk 5 was discarded by Open, then a FLUSH covering it was acknowledged")
	}
}

// P4 (pre-existing): ClearBoundary(true) whose index write fails still moves
// the chunks into the clean LRU, where eviction unlinks them while the on-disk
// index still names them dirty. A crash then makes Open refuse the cache.
func TestProbeClearBoundaryFlushFailureThenEviction(t *testing.T) {
	holdFlush(t)
	c, dir := tmpCache(t, 2*testChunk)
	mustWrite(t, c, 1, 1)
	if err := c.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetBoundary(); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, indexName+".tmp"), 0o700); err != nil { // the next index write fails
		t.Fatal(err)
	}
	c.ClearBoundary(true)
	for i := uint32(2); i < 5; i++ { // evict chunk 1
		if err := c.Insert(i, fill(byte(i))); err != nil && !errors.Is(err, ErrFull) {
			t.Fatal(err)
		}
	}
	e, err := Open(crashCopy(t, dir), 1<<30)
	if err != nil {
		t.Fatalf("crash after a failed ClearBoundary flush + eviction: Open: %v", err)
	}
	_ = e.Close()
}

// P5: the PR's own crash script, re-run against a power-loss image.
func TestProbePowerLossAtEverySyncPoint(t *testing.T) {
	c, dir := flCache(t)
	m := watchPower(t, dir)
	acked := map[uint32]byte{}
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
		checks = append(checks, check{m.image(t), cp})
	}
	step := func(fn func() error, onAck func()) {
		if err := fn(); err != nil {
			t.Fatal(err)
		}
		onAck()
		record()
	}
	timer := func() error { c.mu.Lock(); defer c.mu.Unlock(); return c.flushLocked() }
	noop := func() {}
	wr(t, c, 1, 1)
	wr(t, c, 2, 2)
	step(c.Sync, func() { acked[1], acked[2] = 1, 2 })
	wr(t, c, 1, 3)
	wr(t, c, 3, 4)
	step(func() error { return c.SyncChunks(1) }, func() { acked[1] = 3 })
	step(func() error { return c.SyncChunks(3) }, func() { acked[3] = 4 })
	wr(t, c, 2, 5)
	step(c.Sync, func() { acked[2] = 5 })
	delete(acked, 2)
	if err := c.MarkZero(2); err != nil {
		t.Fatal(err)
	}
	step(timer, noop) // the trim's index + deferred unlink, by the timer
	wr(t, c, 2, 6)
	step(func() error { return c.SyncChunks(2) }, func() { acked[2] = 6 })
	wr(t, c, 4, 8)
	step(timer, noop)
	step(func() error { return c.SyncChunks(4) }, func() { acked[4] = 8 })
	step(func() error { _, err := c.SetBoundary(); return err }, noop)
	wr(t, c, 3, 7)
	step(func() error { return c.SyncChunks(3) }, func() { acked[3] = 7 })
	delete(acked, 1)
	delete(acked, 2)
	delete(acked, 4)
	c.ClearBoundary(true)
	step(c.Sync, noop)
	for k, ck := range checks {
		e, err := Open(ck.image, 1<<30)
		if err != nil {
			t.Errorf("point %d: Open: %v", k, err)
			continue
		}
		for i, v := range ck.acked {
			if !e.Has(i) {
				t.Errorf("point %d: acked chunk %d missing", k, i)
				continue
			}
			if got := mustRead(t, e, i); !bytes.Equal(got[:1], []byte{v}) && got[0] < v {
				t.Errorf("point %d: chunk %d = %d, acked %d", k, i, got[0], v)
			}
		}
		_ = e.Close()
	}
}

// After a restart under a live device the first FLUSH owes the overwrite of an
// already-indexed chunk that the old process acknowledged but never fdatasynced.
func TestPowerLossAfterRestartFlushSyncsOldProcessOverwrite(t *testing.T) {
	c, dir := flCache(t)
	m := watchPower(t, dir)
	wr(t, c, 1, 1)
	if err := c.Sync(); err != nil {
		t.Fatal(err)
	}
	wr(t, c, 1, 2) // acknowledged overwrite; chunk 1 is already dirty in the index
	abandon(c)
	c2, err := Open(dir, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c2.Close() }()
	if err := c2.Sync(); err != nil {
		t.Fatal(err)
	}
	e, err := Open(m.image(t), 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()
	if got := mustRead(t, e, 1)[0]; got != 2 {
		t.Fatalf("chunk 1 reads %d after power cut, want the acked overwrite 2", got)
	}
}

// Power cut at every point of a newly dirty chunk's first write, in particular
// between the index write and the acknowledgement: the image must open and, from
// the index write on, name the chunk with its bytes.
func TestPowerLossBetweenIndexWriteAndAck(t *testing.T) {
	c, dir := flCache(t)
	m := watchPower(t, dir)
	var images []string
	var after []bool // whether the index write had completed at that point
	done := false
	m.hook = func(ev string) {
		if ev == "dir:root" {
			done = true
		}
		images = append(images, m.image(t))
		after = append(after, done)
	}
	wr(t, c, 5, 5)
	if !done {
		t.Fatal("the write was acknowledged before an index was written")
	}
	for k, img := range images {
		e, err := Open(img, 1<<30)
		if err != nil {
			t.Fatalf("point %d: Open: %v", k, err)
		}
		if after[k] {
			if !e.Has(5) || mustRead(t, e, 5)[0] != 5 {
				t.Errorf("point %d: after the index write, chunk 5 or its bytes are missing", k)
			}
		}
		_ = e.Close()
	}
}
