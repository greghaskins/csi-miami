package main

import (
	"bytes"
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/chunk"
)

const testMiB = int64(1) << 20

// testStore is a volume directory built through the chunk package: the
// returned data holds the bytes the volume's chunks should assemble to.
type testStore struct {
	dir string
	s   *chunk.Store
}

func newTestStore(t *testing.T) testStore {
	t.Helper()
	dir := t.TempDir()
	s, err := chunk.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	return testStore{dir: dir, s: s}
}

// put stores one 1 MiB chunk filled with b and returns its hash.
func (ts testStore) put(t *testing.T, b byte) chunk.Hash {
	t.Helper()
	data := bytes.Repeat([]byte{b}, int(testMiB))
	h := chunk.Sum(data)
	if err := ts.s.Put(context.Background(), h, bytes.NewReader(data), testMiB); err != nil {
		t.Fatal(err)
	}
	return h
}

// manifest writes manifest epoch.seq of the chunks and, when publish is set,
// makes it current.
func (ts testStore) manifest(t *testing.T, epoch, seq uint64, publish bool, hs ...chunk.Hash) {
	t.Helper()
	m := chunk.Manifest{
		Volume: "pvc-test", Epoch: epoch, Sequence: seq, Size: int64(len(hs)) * testMiB, Chunk: testMiB,
		Time: time.Unix(1700000000, 0), Chunks: hs,
	}
	if err := ts.s.WriteManifest(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if publish {
		if err := ts.s.Publish(context.Background(), m.Name()); err != nil {
			t.Fatal(err)
		}
	}
}

func runMiami(args ...string) (code int, stdout, stderr string) {
	var o, e bytes.Buffer
	code = run(args, &o, &e)
	return code, o.String(), e.String()
}

func filled(b byte) []byte { return bytes.Repeat([]byte{b}, int(testMiB)) }

func TestRestoreAssemblesCurrent(t *testing.T) {
	t.Parallel()
	ts := newTestStore(t)
	a, b := ts.put(t, 'a'), ts.put(t, 'b')
	ts.manifest(t, 1, 1, true, a, chunk.ZeroHash(testMiB), b)
	out := filepath.Join(t.TempDir(), "v.img")

	code, _, stderr := runMiami("restore", "-store", ts.dir, "-out", out)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	got, err := os.ReadFile(out) //nolint:gosec // test fixture
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Join([][]byte{filled('a'), make([]byte, testMiB), filled('b')}, nil)
	if !bytes.Equal(got, want) {
		t.Fatal("restored bytes differ")
	}
}

func TestRestoreVerifiesBeforeWriting(t *testing.T) {
	t.Parallel()
	ts := newTestStore(t)
	a, b := ts.put(t, 'a'), ts.put(t, 'b')
	ts.manifest(t, 1, 1, true, a, b)
	// Flip a bit in the second chunk, the one assembled last.
	p := ts.s.ChunkPath(b)
	data, err := os.ReadFile(p) //nolint:gosec // test fixture
	if err != nil {
		t.Fatal(err)
	}
	data[100] ^= 1
	if err := os.WriteFile(p, data, 0o644); err != nil { //nolint:gosec // test fixture
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "v.img")

	code, _, stderr := runMiami("restore", "-store", ts.dir, "-out", out)
	if code != 1 {
		t.Fatalf("exit %d, want 1: %s", code, stderr)
	}
	if _, err := os.Lstat(out); !os.IsNotExist(err) {
		t.Fatalf("a bad chunk left an output file (stat err %v)", err)
	}
}

func TestRestoreMissingChunkLeavesNoOutput(t *testing.T) {
	t.Parallel()
	ts := newTestStore(t)
	a := ts.put(t, 'a')
	ts.manifest(t, 1, 1, true, a, chunk.Sum(filled('x')))
	out := filepath.Join(t.TempDir(), "v.img")
	if code, _, _ := runMiami("restore", "-store", ts.dir, "-out", out); code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	if _, err := os.Lstat(out); !os.IsNotExist(err) {
		t.Fatalf("a missing chunk left an output file (stat err %v)", err)
	}
}

func TestRestoreUsesNamedManifest(t *testing.T) {
	t.Parallel()
	ts := newTestStore(t)
	a, b := ts.put(t, 'a'), ts.put(t, 'b')
	ts.manifest(t, 1, 1, false, a)
	ts.manifest(t, 1, 2, true, b)
	out := filepath.Join(t.TempDir(), "v.img")

	if code, _, stderr := runMiami("restore", "-store", ts.dir, "-out", out, "-manifest", "1.1"); code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	got, _ := os.ReadFile(out) //nolint:gosec // test fixture
	if !bytes.Equal(got, filled('a')) {
		t.Fatal("restored the wrong manifest")
	}
}

func TestRestoreRefusesBadManifestNames(t *testing.T) {
	t.Parallel()
	ts := newTestStore(t)
	a := ts.put(t, 'a')
	ts.manifest(t, 1, 1, true, a)
	for _, name := range []string{"../current", "../../etc/passwd", "1.1/../1.1", "/etc/passwd", "..", "1.2", "01.1", "current", " "} {
		out := filepath.Join(t.TempDir(), "v.img")
		code, _, _ := runMiami("restore", "-store", ts.dir, "-out", out, "-manifest", name)
		if code == 0 {
			t.Errorf("manifest name %q was accepted", name)
		}
		if _, err := os.Lstat(out); !os.IsNotExist(err) {
			t.Errorf("manifest name %q left an output file", name)
		}
	}
}

func TestRestoreRefusesBadPaths(t *testing.T) {
	t.Parallel()
	ts := newTestStore(t)
	a := ts.put(t, 'a')
	ts.manifest(t, 1, 1, true, a)
	dir := t.TempDir()
	existing := filepath.Join(dir, "exists.img")
	if err := os.WriteFile(existing, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, args := range map[string][]string{
		"existing output":    {"-store", ts.dir, "-out", existing},
		"relative output":    {"-store", ts.dir, "-out", "v.img"},
		"traversal output":   {"-store", ts.dir, "-out", dir + "/x/../v.img"},
		"output in store":    {"-store", ts.dir, "-out", filepath.Join(ts.dir, "v.img")},
		"output in chunks":   {"-store", ts.dir, "-out", filepath.Join(ts.dir, "chunks", "v.img")},
		"relative store":     {"-store", "pvc", "-out", filepath.Join(dir, "v.img")},
		"missing store":      {"-store", filepath.Join(dir, "absent"), "-out", filepath.Join(dir, "v.img")},
		"no store":           {"-out", filepath.Join(dir, "v.img")},
		"no out":             {"-store", ts.dir},
		"directory as store": {"-store", dir, "-out", filepath.Join(dir, "v.img")},
	} {
		if code, _, _ := runMiami(append([]string{"restore"}, args...)...); code == 0 {
			t.Errorf("%s: accepted", name)
		}
	}
	if b, _ := os.ReadFile(existing); string(b) != "keep" { //nolint:gosec // test fixture
		t.Fatal("an existing output file was overwritten")
	}
	if _, err := os.Lstat(filepath.Join(ts.dir, "v.img")); !os.IsNotExist(err) {
		t.Fatal("restore wrote into the store")
	}
}

// storeSnapshot lists every entry under dir with its size and mtime.
func storeSnapshot(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		out = append(out, p+" "+fi.Mode().String()+" "+fi.ModTime().String()+" "+string(rune('0'+fi.Size()%10)))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestRestoreNeverWritesIntoTheStore(t *testing.T) {
	t.Parallel()
	ts := newTestStore(t)
	a := ts.put(t, 'a')
	ts.manifest(t, 1, 1, true, a)
	// Read-only directories: any write attempt fails (except as root, where
	// the snapshot comparison still catches it).
	var dirs []string
	_ = filepath.WalkDir(ts.dir, func(p string, d fs.DirEntry, _ error) error {
		if d != nil && d.IsDir() {
			dirs = append(dirs, p)
		}
		return nil
	})
	for _, d := range dirs {
		if err := os.Chmod(d, 0o555); err != nil { //nolint:gosec // test fixture
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		for _, d := range dirs {
			_ = os.Chmod(d, 0o755) //nolint:gosec // test fixture
		}
	})
	before := storeSnapshot(t, ts.dir)
	out := filepath.Join(t.TempDir(), "v.img")
	if code, _, stderr := runMiami("restore", "-store", ts.dir, "-out", out); code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if after := storeSnapshot(t, ts.dir); strings.Join(before, "\n") != strings.Join(after, "\n") {
		t.Fatalf("the store changed:\nbefore %v\nafter  %v", before, after)
	}
}

func TestRestoreDoesNotCreateStoreDirectories(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	out := filepath.Join(t.TempDir(), "v.img")
	if code, _, _ := runMiami("restore", "-store", dir, "-out", out); code == 0 {
		t.Fatal("restore accepted an empty directory")
	}
	if ents, _ := os.ReadDir(dir); len(ents) != 0 {
		t.Fatalf("restore created %d entries in the store", len(ents))
	}
}

func TestRestoreBoundedMemory(t *testing.T) {
	// Not parallel: it reads process-wide memory statistics.
	ts := newTestStore(t)
	var hs []chunk.Hash
	zero := chunk.ZeroHash(testMiB)
	for i := range 1024 { // 1 GiB, mostly holes
		if i%128 == 0 {
			hs = append(hs, ts.put(t, byte('a'+i/128)))
		} else {
			hs = append(hs, zero)
		}
	}
	ts.manifest(t, 1, 1, true, hs...)
	out := filepath.Join(t.TempDir(), "v.img")

	runtime.GC()
	var base, after runtime.MemStats
	runtime.ReadMemStats(&base)
	if code, _, stderr := runMiami("restore", "-store", ts.dir, "-out", out); code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	runtime.ReadMemStats(&after)
	// HeapSys only grows, so it bounds the peak heap the restore needed.
	if grew := int64(after.HeapSys) - int64(base.HeapSys); grew > 64<<20 { //nolint:gosec // test fixture
		t.Fatalf("restore grew the heap by %d MiB, want under 64", grew>>20)
	}
	fi, err := os.Stat(out)
	if err != nil || fi.Size() != 1<<30 {
		t.Fatalf("output: %v, %v", fi, err)
	}
}

// twoChunkStore is a store of two data chunks ('a', 'b') and the path of the
// second chunk's file.
func twoChunkStore(t *testing.T) (ts testStore, bPath string) {
	t.Helper()
	ts = newTestStore(t)
	a, b := ts.put(t, 'a'), ts.put(t, 'b')
	ts.manifest(t, 1, 1, true, a, b)
	return ts, ts.s.ChunkPath(b)
}

func corrupt(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path) //nolint:gosec // test fixture
	if err != nil {
		t.Fatal(err)
	}
	data[100] ^= 1
	if err := os.WriteFile(path, data, 0o644); err != nil { //nolint:gosec // test fixture
		t.Fatal(err)
	}
}

func dirEntries(t *testing.T, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range ents {
		out = append(out, e.Name())
	}
	return out
}

// Nothing, not even a temporary file, may exist in the output directory until
// every chunk has verified: the hook fails the test if the restore gets as far
// as creating anything over a corrupt store.
func TestRestoreCreatesNothingBeforeVerifyCompletes(t *testing.T) {
	t.Parallel()
	ts, bPath := twoChunkStore(t)
	corrupt(t, bPath)
	dir := t.TempDir()
	h := restoreHooks{beforeCreate: func() { t.Error("the restore reached file creation over a corrupt chunk") }}
	if _, err := restoreVolume(t.Context(), ts.dir, filepath.Join(dir, "v.img"), "", h); err == nil {
		t.Fatal("restore of a corrupt store succeeded")
	}
	if got := dirEntries(t, dir); len(got) != 0 {
		t.Fatalf("output directory holds %v", got)
	}
}

func TestRestoreImageAppearsOnlyWhenComplete(t *testing.T) {
	t.Parallel()
	ts, _ := twoChunkStore(t)
	dir := t.TempDir()
	dest := filepath.Join(dir, "v.img")
	var sawTemp bool
	h := restoreHooks{afterAssemble: func(tmp string) {
		// A kill here leaves a hidden temporary file and no image.
		if _, err := os.Lstat(dest); err == nil {
			t.Error("dest existed before the image was complete")
		}
		fi, err := os.Stat(tmp)
		if err != nil || fi.Size() != 2*testMiB || !strings.HasPrefix(filepath.Base(tmp), ".") {
			t.Errorf("temporary file: %v, %v", fi, err)
		}
		sawTemp = true
	}}
	if _, err := restoreVolume(t.Context(), ts.dir, dest, "", h); err != nil {
		t.Fatal(err)
	}
	if !sawTemp {
		t.Fatal("the hook did not run")
	}
	if got := dirEntries(t, dir); len(got) != 1 || got[0] != "v.img" {
		t.Fatalf("output directory holds %v, want only v.img", got)
	}
}

func TestRestoreFailureAfterVerifyLeavesNoImageAndNoTemp(t *testing.T) {
	t.Parallel()
	ts, bPath := twoChunkStore(t)
	dir := t.TempDir()
	// The store rots between verification and assembly.
	h := restoreHooks{beforeCreate: func() { corrupt(t, bPath) }}
	if _, err := restoreVolume(t.Context(), ts.dir, filepath.Join(dir, "v.img"), "", h); err == nil {
		t.Fatal("restore succeeded over a chunk that rotted mid-way")
	}
	if got := dirEntries(t, dir); len(got) != 0 {
		t.Fatalf("output directory holds %v", got)
	}
}

func TestRestorePanicLeavesNoTemp(t *testing.T) {
	t.Parallel()
	ts, _ := twoChunkStore(t)
	dir := t.TempDir()
	func() {
		defer func() { _ = recover() }()
		_, _ = restoreVolume(t.Context(), ts.dir, filepath.Join(dir, "v.img"), "", restoreHooks{afterAssemble: func(string) { panic("boom") }})
	}()
	if got := dirEntries(t, dir); len(got) != 0 {
		t.Fatalf("output directory holds %v", got)
	}
}

func TestRestoreNeverReplacesAFileThatAppearsMeanwhile(t *testing.T) {
	t.Parallel()
	ts, _ := twoChunkStore(t)
	dir := t.TempDir()
	dest := filepath.Join(dir, "v.img")
	h := restoreHooks{afterAssemble: func(string) {
		if err := os.WriteFile(dest, []byte("keep"), 0o600); err != nil {
			t.Error(err)
		}
	}}
	if _, err := restoreVolume(t.Context(), ts.dir, dest, "", h); err == nil {
		t.Fatal("restore replaced a file that appeared meanwhile")
	}
	if b, _ := os.ReadFile(dest); string(b) != "keep" { //nolint:gosec // test fixture
		t.Fatalf("dest = %q", b)
	}
	if got := dirEntries(t, dir); len(got) != 1 {
		t.Fatalf("output directory holds %v", got)
	}
}

// TestRestoreStreamsLargeChunks proves the restore reads a chunk through a
// small buffer: with 64 MiB chunks, a restore that held a chunk (or the
// image) in memory would allocate hundreds of MiB, and TotalAlloc counts
// every allocation whether or not the collector has run.
func TestRestoreStreamsLargeChunks(t *testing.T) {
	// Not parallel: it reads process-wide memory statistics.
	const big = 64 * testMiB
	dir := t.TempDir()
	s, err := chunk.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	zero := chunk.ZeroHash(big)
	hs := make([]chunk.Hash, 16) // 1 GiB
	for i := range hs {
		hs[i] = zero
	}
	for i, fill := range map[int]byte{3: 'x', 11: 'y'} {
		data := bytes.Repeat([]byte{fill}, int(big))
		hs[i] = chunk.Sum(data)
		if err := s.Put(t.Context(), hs[i], bytes.NewReader(data), big); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.EnsureZero(t.Context(), big); err != nil {
		t.Fatal(err)
	}
	m := chunk.Manifest{Volume: "pvc-test", Epoch: 1, Sequence: 1, Size: 16 * big, Chunk: big, Time: time.Unix(1700000000, 0), Chunks: hs}
	if err := s.WriteManifest(t.Context(), m); err != nil {
		t.Fatal(err)
	}
	if err := s.Publish(t.Context(), m.Name()); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "v.img")

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	if code, _, stderr := runMiami("restore", "-store", dir, "-out", out); code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	runtime.ReadMemStats(&after)
	if alloc := (after.TotalAlloc - before.TotalAlloc) >> 20; alloc > 32 {
		t.Fatalf("restore allocated %d MiB in total, want under 32", alloc)
	}
	fi, err := os.Stat(out)
	if err != nil || fi.Size() != 16*big {
		t.Fatalf("output: %v, %v", fi, err)
	}
}
