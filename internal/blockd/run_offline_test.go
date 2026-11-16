package blockd

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/chunk"
	"github.com/greghaskins/csi-miami/internal/nbd"
)

// nbdIO sends one request (nbd transmission phase, simple replies) on the
// kernel's end of the socket and returns the reply's data. A non-zero error in
// the reply, which the kernel would pass up as EIO, is an error here.
func nbdIO(c net.Conn, typ uint16, handle uint64, off int64, data []byte, n int) ([]byte, error) {
	if err := c.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return nil, err
	}
	defer func() { _ = c.SetDeadline(time.Time{}) }()
	var h [28]byte
	binary.BigEndian.PutUint32(h[0:4], 0x25609513)
	binary.BigEndian.PutUint16(h[6:8], typ)
	binary.BigEndian.PutUint64(h[8:16], handle)
	binary.BigEndian.PutUint64(h[16:24], uint64(off)) //nolint:gosec // test offsets are small and positive
	binary.BigEndian.PutUint32(h[24:28], uint32(n))   //nolint:gosec // test lengths are small and positive
	if _, err := c.Write(append(h[:], data...)); err != nil {
		return nil, err
	}
	var r [16]byte
	if _, err := io.ReadFull(c, r[:]); err != nil {
		return nil, err
	}
	if e := binary.BigEndian.Uint32(r[4:8]); e != 0 {
		return nil, fmt.Errorf("nbd reply error %d", e)
	}
	if typ != 0 {
		return nil, nil
	}
	out := make([]byte, n)
	_, err := io.ReadFull(c, out)
	return out, err
}

const (
	nbdRead  = 0
	nbdWrite = 1
	nbdFlush = 3
)

// Issue csi-miami#102: the block server of a device that carries a mount
// restarts while the store is unreachable. It re-points the device at once
// from the local cache; reads of the chunks the resumed cache holds (its dirty
// set: a resumed cache starts cold for clean data) and writes within the cache
// bound succeed over the device, and so does a partial write into a chunk
// the base names as zero (the cache's copy of the base says so); a publish
// fails while the store is away, and the next one after it returns makes
// current hold every acknowledged write.
func TestRunRepointsALeftDeviceWhileTheStoreIsUnreachable(t *testing.T) {
	shortConfirm(t)
	shortBaseRetry(t)
	cfg, _ := testConfig(t)
	cfg.Size = 3 * testChunk
	store := openStore(t, cfg)
	a := pattern(1, testChunk)
	z := chunk.ZeroHash(testChunk)
	writeCurrent(t, store, 1, 1, []chunk.Hash{putChunk(t, store, a), z, z})

	// The server that crashed had written both chunks (dirty, never
	// published): the resumed cache holds them. Chunk 0 is also what the
	// re-point's confirming read touches.
	b0, b := pattern(4, testChunk), pattern(2, testChunk)
	s, err := newServer(context.Background(), cfg, testLog())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.back.WriteAt(b0, 0, true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.back.WriteAt(b, testChunk, true); err != nil {
		t.Fatal(err)
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}

	away := cfg.StoreDir + ".away"
	if err := os.Rename(cfg.StoreDir, away); err != nil {
		t.Fatal(err)
	}

	// The confirming read goes straight to the backend, so this test's
	// requests have the socket to themselves.
	k := &fakeKernel{devs: []nbd.Device{ourDevice(3, cfg.Size)}}
	k.cleanup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runAsync(ctx, cfg, k)
	waitIndex(t, cfg.ControlSocket, 3)
	k.mu.Lock()
	c := k.held[len(k.held)-1]
	k.mu.Unlock()

	got, err := nbdIO(c, nbdRead, 2, testChunk, nil, 4096)
	if err != nil || !bytes.Equal(got, b[:4096]) {
		t.Fatalf("cached dirty read with the store away: err %v, match %v", err, bytes.Equal(got, b[:4096]))
	}
	w := bytes.Repeat([]byte{0xab}, 4096)
	if _, err := nbdIO(c, nbdWrite, 3, testChunk+8192, w, len(w)); err != nil {
		t.Fatalf("sub-chunk write to a cached chunk with the store away: %v", err)
	}
	// Chunk 2 was never written: the base names it zero.
	if _, err := nbdIO(c, nbdWrite, 6, 2*testChunk+4096, w, len(w)); err != nil {
		t.Fatalf("partial write into a never-written chunk with the store away: %v", err)
	}
	got, err = nbdIO(c, nbdRead, 7, 2*testChunk, nil, 8192)
	if err != nil || !bytes.Equal(got[:4096], make([]byte, 4096)) || !bytes.Equal(got[4096:], w) {
		t.Fatalf("reading the never-written chunk back with the store away: err %v", err)
	}
	full := pattern(3, testChunk)
	if _, err := nbdIO(c, nbdWrite, 5, 0, full, len(full)); err != nil {
		t.Fatalf("full-chunk write with the store away: %v", err)
	}
	if _, err := nbdIO(c, nbdFlush, 4, 0, nil, 0); err != nil {
		t.Fatalf("flush with the store away: %v", err)
	}

	cli := dialT(t, cfg.ControlSocket)
	if err := cli.Boundary(ctx); err != nil {
		t.Fatalf("boundary with the store away: %v", err)
	}
	if _, err := cli.Publish(ctx, "test"); err == nil {
		t.Fatal("a publish with the store away succeeded")
	}

	if err := os.Rename(away, cfg.StoreDir); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		err := cli.Boundary(ctx)
		if err == nil {
			_, err = cli.Publish(ctx, "test")
		}
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no publish succeeded after the store came back: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	stopRun(t, cancel, done)

	name, ok, err := openStore(t, cfg).Current(context.Background())
	if err != nil || !ok {
		t.Fatalf("current: %v %v", ok, err)
	}
	m, err := openStore(t, cfg).ReadManifest(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	want1 := append([]byte(nil), b...)
	copy(want1[8192:], w)
	want2 := make([]byte, testChunk)
	copy(want2[4096:], w)
	for i, want := range [][]byte{full, want1, want2} {
		if m.Chunks[i] != chunk.Sum(want) {
			t.Errorf("chunk %d in %s is not the acknowledged bytes", i, name)
		}
	}
}

// dialT opens a control client for the test's lifetime.
func dialT(t *testing.T, socket string) *Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, err := Dial(ctx, socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

// shortBaseRetry shortens the wait between attempts to load the base.
func shortBaseRetry(t *testing.T) {
	t.Helper()
	lo, hi := baseRetryMin, baseRetryMax
	baseRetryMin, baseRetryMax = 5*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { baseRetryMin, baseRetryMax = lo, hi })
}

// The store's base differs from the cache's copy a resumed server answered
// hashes from: once the store is back, every publish fails with
// errBaseCopyMismatch (cache.ErrCorrupt) and Status says so, for the node to
// raise a Warning event.
func TestRunStopsPublishingWhenTheStoresBaseDiffersFromTheCopy(t *testing.T) {
	shortConfirm(t)
	shortBaseRetry(t)
	cfg, _ := testConfig(t)
	store := openStore(t, cfg)
	m := writeCurrent(t, store, 1, 1, []chunk.Hash{putChunk(t, store, pattern(1, testChunk)), chunk.ZeroHash(testChunk)})
	s, err := newServer(context.Background(), cfg, testLog())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.back.WriteAt(pattern(4, testChunk), 0, true); err != nil {
		t.Fatal(err)
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	// A copy under the same name with another hash for chunk 1.
	bad := m
	bad.Chunks = []chunk.Hash{m.Chunks[0], chunk.Sum(pattern(9, testChunk))}
	if err := writeBaseCopy(cfg.CacheDir, bad); err != nil {
		t.Fatal(err)
	}

	k := &fakeKernel{devs: []nbd.Device{ourDevice(3, testSize)}}
	k.cleanup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runAsync(ctx, cfg, k)
	waitIndex(t, cfg.ControlSocket, 3)
	cli := dialT(t, cfg.ControlSocket)
	deadline := time.Now().Add(10 * time.Second)
	for {
		st, err := cli.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if st.BaseMismatch != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Status never reported the mismatch")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := cli.Boundary(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Publish(ctx, "test"); err == nil || !strings.Contains(err.Error(), "differs from the cache's copy") {
		t.Fatalf("publish after the mismatch = %v, want it refused", err)
	}
	stopRun(t, cancel, done)
	if name, _, _ := openStore(t, cfg).Current(context.Background()); name != m.Name() {
		t.Errorf("current moved to %s after the mismatch", name)
	}
}

func TestReadBaseCopyRefusesACopyThatDoesNotFit(t *testing.T) {
	dir := t.TempDir()
	m := chunk.Manifest{Volume: testVol, Epoch: 1, Sequence: 2, Size: 2 * testChunk, Chunk: testChunk,
		Time: time.Now().UTC().Truncate(time.Second), Chunks: []chunk.Hash{chunk.ZeroHash(testChunk), chunk.ZeroHash(testChunk)}}
	if err := writeBaseCopy(dir, m); err != nil {
		t.Fatal(err)
	}
	if got, ok := readBaseCopy(dir, m.Name(), testVol, testChunk, 2*testChunk); !ok || got.Name() != m.Name() {
		t.Fatalf("a fitting copy was refused: %v", ok)
	}
	for _, c := range []struct {
		what, name, vol string
		chunk, size     int64
	}{
		{"another base", "1.3", testVol, testChunk, 2 * testChunk},
		{"no base", "", testVol, testChunk, 2 * testChunk},
		{"another volume", m.Name(), "pvc-other", testChunk, 2 * testChunk},
		{"another chunk size", m.Name(), testVol, 2 * testChunk, 2 * testChunk},
		{"a smaller cache", m.Name(), testVol, testChunk, testChunk},
	} {
		if _, ok := readBaseCopy(dir, c.name, c.vol, c.chunk, c.size); ok {
			t.Errorf("%s: the copy was used", c.what)
		}
	}
	// A copy that does not parse, and one whose content names another manifest.
	if err := os.WriteFile(baseCopyPath(dir, m.Name()), []byte("# format 1\ngarbage\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok := readBaseCopy(dir, m.Name(), testVol, testChunk, 2*testChunk); ok {
		t.Error("an unparsable copy was used")
	}
	other := m
	other.Sequence = 7
	if err := os.WriteFile(baseCopyPath(dir, m.Name()), other.Marshal(), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok := readBaseCopy(dir, m.Name(), testVol, testChunk, 2*testChunk); ok {
		t.Error("a copy of another manifest under this name was used")
	}
}

// The copy is written when a stage creates the cache from the store, when a
// resume with the store loads a base that has none, and after a publish,
// which keeps the previous one and drops older ones.
func TestBaseCopyFollowsTheBase(t *testing.T) {
	cfg, _ := testConfig(t)
	store := openStore(t, cfg)
	m1 := writeCurrent(t, store, 1, 1, []chunk.Hash{putChunk(t, store, pattern(1, testChunk)), chunk.ZeroHash(testChunk)})
	copies := func() []string {
		ents, _ := os.ReadDir(cfg.CacheDir)
		var out []string
		for _, e := range ents {
			if strings.HasPrefix(e.Name(), baseCopyPrefix) {
				out = append(out, strings.TrimPrefix(e.Name(), baseCopyPrefix))
			}
		}
		return out
	}

	s := newTestServer(t, cfg)
	if got := copies(); len(got) != 1 || got[0] != m1.Name() {
		t.Fatalf("after the stage: copies %v, want [%s]", got, m1.Name())
	}
	var names []string
	for i := range 3 {
		if _, err := s.back.WriteAt(pattern(byte(10+i), testChunk), 0, true); err != nil {
			t.Fatal(err)
		}
		if err := s.boundary(); err != nil {
			t.Fatal(err)
		}
		r, err := s.publish(context.Background(), "test")
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, r.Name)
	}
	got := copies()
	if len(got) != 2 || !slices.Contains(got, names[1]) || !slices.Contains(got, names[2]) {
		t.Fatalf("after three publishes: copies %v, want the last two of %v", got, names)
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}

	// A cache from before the copies: resuming with the store writes one.
	for _, n := range got {
		if err := os.Remove(baseCopyPath(cfg.CacheDir, n)); err != nil {
			t.Fatal(err)
		}
	}
	s2, err := newServer(context.Background(), cfg, testLog())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.close() }()
	if got := copies(); len(got) != 1 || got[0] != names[2] {
		t.Errorf("after a resume with the store: copies %v, want [%s]", got, names[2])
	}
}

// Decision 68, option 3: a resumed cache keeps its clean copies unverified and
// adopts one on first use when its SHA-256 is the base's. With the store away,
// a read of a clean chunk and a partial write into one (what ext4's journal
// does) succeed, and the next publish holds the acknowledged bytes.
func TestRunAdoptsCleanChunksWhileTheStoreIsUnreachable(t *testing.T) {
	shortConfirm(t)
	shortBaseRetry(t)
	cfg, _ := testConfig(t)
	store := openStore(t, cfg)
	a, b := pattern(1, testChunk), pattern(2, testChunk)
	writeCurrent(t, store, 1, 1, []chunk.Hash{putChunk(t, store, a), putChunk(t, store, b)})
	s, err := newServer(context.Background(), cfg, testLog())
	if err != nil {
		t.Fatal(err)
	}
	for _, off := range []int64{0, testChunk} { // fetched: clean copies, in no index
		if _, err := s.back.ReadAt(make([]byte, testChunk), off); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	away := cfg.StoreDir + ".away"
	if err := os.Rename(cfg.StoreDir, away); err != nil {
		t.Fatal(err)
	}

	k := &fakeKernel{devs: []nbd.Device{ourDevice(3, testSize)}}
	k.cleanup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runAsync(ctx, cfg, k)
	waitIndex(t, cfg.ControlSocket, 3)
	k.mu.Lock()
	c := k.held[len(k.held)-1]
	k.mu.Unlock()

	got, err := nbdIO(c, nbdRead, 1, testChunk+4096, nil, 4096)
	if err != nil || !bytes.Equal(got, b[4096:8192]) {
		t.Fatalf("read of a clean chunk with the store away: err %v, match %v", err, bytes.Equal(got, b[4096:8192]))
	}
	w := bytes.Repeat([]byte{0x5a}, 4096)
	if _, err := nbdIO(c, nbdWrite, 2, 8192, w, len(w)); err != nil {
		t.Fatalf("partial write into a clean chunk with the store away: %v", err)
	}
	if _, err := nbdIO(c, nbdFlush, 3, 0, nil, 0); err != nil {
		t.Fatal(err)
	}

	if err := os.Rename(away, cfg.StoreDir); err != nil {
		t.Fatal(err)
	}
	cli := dialT(t, cfg.ControlSocket)
	deadline := time.Now().Add(10 * time.Second)
	for {
		err := cli.Boundary(ctx)
		if err == nil {
			_, err = cli.Publish(ctx, "test")
		}
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no publish after the store came back: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	stopRun(t, cancel, done)
	name, _, _ := openStore(t, cfg).Current(context.Background())
	m, err := openStore(t, cfg).ReadManifest(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	want0 := append([]byte(nil), a...)
	copy(want0[8192:], w)
	if m.Chunks[0] != chunk.Sum(want0) || m.Chunks[1] != chunk.Sum(b) {
		t.Errorf("%s does not hold the acknowledged bytes", name)
	}
}

// The crash rule (decision 68): a chunk file the index does not name whose
// content is not the base's (a write that a crash beat to the index) is never
// served; the chunk reads as the base.
func TestRunNeverServesAnUnindexedFileThatIsNotTheBase(t *testing.T) {
	shortConfirm(t)
	shortBaseRetry(t)
	cfg, _ := testConfig(t)
	store := openStore(t, cfg)
	a := pattern(1, testChunk)
	writeCurrent(t, store, 1, 1, []chunk.Hash{putChunk(t, store, a), chunk.ZeroHash(testChunk)})
	s, err := newServer(context.Background(), cfg, testLog())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	stray := pattern(9, testChunk)
	if err := os.WriteFile(filepath.Join(cfg.CacheDir, "c", "0"), stray, 0o600); err != nil {
		t.Fatal(err)
	}
	k := &fakeKernel{devs: []nbd.Device{ourDevice(3, testSize)}}
	k.cleanup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runAsync(ctx, cfg, k)
	waitIndex(t, cfg.ControlSocket, 3)
	k.mu.Lock()
	c := k.held[len(k.held)-1]
	k.mu.Unlock()
	got, err := nbdIO(c, nbdRead, 1, 0, nil, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(got, stray[:4096]) || !bytes.Equal(got, a[:4096]) {
		t.Fatalf("chunk 0 read %x..., want the base's %x...", got[:4], a[:4])
	}
	stopRun(t, cancel, done)
}

// Readers and a writer racing on one unverified chunk with the store away:
// whoever adopts it, the others find it resident; none fails or waits for
// the store.
func TestConcurrentFirstUseOfAnUnverifiedChunk(t *testing.T) {
	cfg, _ := testConfig(t)
	store := openStore(t, cfg)
	a := pattern(1, testChunk)
	writeCurrent(t, store, 1, 1, []chunk.Hash{putChunk(t, store, a), chunk.ZeroHash(testChunk)})
	s, err := newServer(context.Background(), cfg, testLog())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.back.ReadAt(make([]byte, testChunk), 0); err != nil { // a clean copy, in no index
		t.Fatal(err)
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(cfg.StoreDir, cfg.StoreDir+".away"); err != nil {
		t.Fatal(err)
	}
	cfg.Timeout = 2 * time.Second // a fetch would fail fast, not hang the test
	for round := range 20 {
		r, err := resumeServer(context.Background(), cfg, testLog())
		if err != nil {
			t.Fatal(err)
		}
		if !r.back.cache.Unverified(0) {
			t.Fatalf("round %d: chunk 0 is not unverified after the resume", round)
		}
		var wg sync.WaitGroup
		errs := make(chan error, 9)
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				buf := make([]byte, 4096)
				if _, err := r.back.ReadAt(buf, 4096); err != nil {
					errs <- fmt.Errorf("read: %w", err)
				} else if !bytes.Equal(buf, a[4096:8192]) {
					errs <- errors.New("read the wrong bytes")
				}
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := r.back.WriteAt(bytes.Repeat([]byte{7}, 4096), 12288, false); err != nil {
				errs <- fmt.Errorf("write: %w", err)
			}
		}()
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Errorf("round %d: %v", round, err)
		}
		if err := r.close(); err != nil {
			t.Fatal(err)
		}
		if t.Failed() {
			return
		}
		// Back to one unverified clean copy for the next round: drop the
		// dirty write by rebuilding the cache from the original state.
		if err := os.Rename(cfg.StoreDir+".away", cfg.StoreDir); err != nil {
			t.Fatal(err)
		}
		if err := os.RemoveAll(cfg.CacheDir); err != nil {
			t.Fatal(err)
		}
		s, err := newServer(context.Background(), cfg, testLog())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.back.ReadAt(make([]byte, testChunk), 0); err != nil {
			t.Fatal(err)
		}
		if err := s.close(); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(cfg.StoreDir, cfg.StoreDir+".away"); err != nil {
			t.Fatal(err)
		}
	}
}
