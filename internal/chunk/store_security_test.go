package chunk

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The store is on a shared export, and a writer there is not trusted for
// integrity: these tests plant links and oversized files where the store
// reads and removes, and require that nothing outside the store is read,
// written or removed, and that no file content reaches an error.

const secret = "TOPSECRET-token-abc"

// victim is a directory standing for a node directory outside the store, with
// a secret file and an old file Prune would remove if it followed a link.
func victim(t *testing.T) (dir, secretFile, oldFile string) {
	t.Helper()
	dir = t.TempDir()
	secretFile = filepath.Join(dir, "secret")
	if err := os.WriteFile(secretFile, []byte(secret+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	oldFile = filepath.Join(dir, "alice", ".bashrc")
	if err := os.Mkdir(filepath.Dir(oldFile), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(oldFile, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-48 * time.Hour)
	_ = os.Chtimes(oldFile, old, old)
	return dir, secretFile, oldFile
}

func mustExist(t *testing.T, p string) {
	t.Helper()
	if _, err := os.Lstat(p); err != nil {
		t.Fatalf("%s should be untouched: %v", p, err)
	}
}

func noSecret(t *testing.T, err error) {
	t.Helper()
	if err != nil && strings.Contains(err.Error(), secret) {
		t.Fatalf("error carries file content: %v", err)
	}
}

func TestCurrentSymlinkIsRefusedAndNotEchoed(t *testing.T) {
	t.Parallel()
	_, secretFile, _ := victim(t)
	s := openStore(t)
	if err := os.Symlink(secretFile, s.currentPath()); err != nil {
		t.Fatal(err)
	}
	_, ok, err := s.Current(context.Background())
	if err == nil || ok {
		t.Fatalf("Current followed a symlink: ok=%v err=%v", ok, err)
	}
	noSecret(t, err)
}

func TestCurrentWithContentIsNotEchoed(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	if err := os.WriteFile(s.currentPath(), []byte(secret+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, _, err := s.Current(context.Background())
	if err == nil {
		t.Fatal("a current that is not a manifest name was accepted")
	}
	noSecret(t, err)
}

func TestCurrentIsSizeCapped(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	if err := os.WriteFile(s.currentPath(), bytes.Repeat([]byte("1"), 1<<20), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Current(context.Background()); err == nil {
		t.Fatal("a 1 MiB current was read")
	}
}

func TestCurrentIsRegularOnly(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	if err := os.Mkdir(s.currentPath(), 0o750); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Current(context.Background()); err == nil {
		t.Fatal("a directory named current was accepted")
	}
}

func TestOpenRefusesSymlinkedRootAndSubdirectories(t *testing.T) {
	t.Parallel()
	v, _, old := victim(t)
	// A symlinked root.
	link := filepath.Join(t.TempDir(), "store")
	if err := os.Symlink(v, link); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(link); err == nil {
		t.Error("Open followed a symlinked root")
	}
	if _, err := OpenReadOnly(link); err == nil {
		t.Error("OpenReadOnly followed a symlinked root")
	}
	// A symlinked chunks/ or manifests/.
	for _, sub := range []string{"chunks", "manifests"} {
		root := t.TempDir()
		other := "manifests"
		if sub == "manifests" {
			other = "chunks"
		}
		if err := os.Mkdir(filepath.Join(root, other), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(v, filepath.Join(root, sub)); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(root); err == nil {
			t.Errorf("Open followed a symlinked %s", sub)
		}
		if _, err := OpenReadOnly(root); err == nil {
			t.Errorf("OpenReadOnly followed a symlinked %s", sub)
		}
	}
	mustExist(t, old)
}

// A link put in place after the store was opened is refused too: Prune
// resolves chunks/ afresh.
func TestPruneDoesNotFollowChunksSymlink(t *testing.T) {
	t.Parallel()
	v, _, old := victim(t)
	root := t.TempDir()
	s, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, "chunks")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(v, filepath.Join(root, "chunks")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Prune(context.Background(), 1); err == nil {
		t.Fatal("Prune walked a symlinked chunks/")
	}
	mustExist(t, old)
}

func TestPruneDoesNotFollowFanSymlink(t *testing.T) {
	t.Parallel()
	v, _, old := victim(t)
	s := openStore(t)
	if err := os.Symlink(v, filepath.Join(s.root, "chunks", "aa")); err != nil {
		t.Fatal(err)
	}
	if _, n, err := s.Prune(context.Background(), 1); err != nil || n != 0 {
		t.Fatalf("Prune = %d, %v", n, err)
	}
	mustExist(t, old)
	if _, err := os.Lstat(filepath.Join(s.root, "chunks", "aa")); err != nil {
		t.Fatalf("the link itself was removed: %v", err)
	}
}

func TestPruneDoesNotFollowManifestsSymlink(t *testing.T) {
	t.Parallel()
	v, _, old := victim(t)
	s := openStore(t)
	if err := os.Remove(filepath.Join(s.root, "manifests")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(v, filepath.Join(s.root, "manifests")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Prune(context.Background(), 1); err == nil {
		t.Fatal("Prune read through a symlinked manifests/")
	}
	mustExist(t, old)
}

func TestManifestSymlinkIsNotAManifest(t *testing.T) {
	t.Parallel()
	_, secretFile, _ := victim(t)
	s := openStore(t)
	if err := os.Symlink(secretFile, filepath.Join(s.root, "manifests", "1.1")); err != nil {
		t.Fatal(err)
	}
	_, err := s.ReadManifest(context.Background(), "1.1")
	if err == nil {
		t.Fatal("ReadManifest followed a symlink")
	}
	noSecret(t, err)
	names, err := s.Manifests(context.Background())
	if err != nil || len(names) != 0 {
		t.Fatalf("Manifests = %v, %v; a symlink is not a manifest", names, err)
	}
}

func TestManifestContentIsNotEchoed(t *testing.T) {
	t.Parallel()
	good := string(testManifest().Marshal())
	for name, body := range map[string]string{
		"format":      "# format " + secret + "\n",
		"first line":  secret + "\n",
		"hash line":   strings.Replace(good, "\n", "\n"+secret+"\n", 7),
		"epoch value": strings.Replace(good, "# epoch 7", "# epoch "+secret, 1),
		"time value":  strings.Replace(good, "# time ", "# time "+secret+" ", 1),
	} {
		t.Run(name, func(t *testing.T) {
			s := openStore(t)
			if err := os.WriteFile(filepath.Join(s.root, "manifests", "1.1"), []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := s.ReadManifest(context.Background(), "1.1")
			noSecret(t, err)
			_, err = ParseManifest(strings.NewReader(body))
			noSecret(t, err)
		})
	}
	if _, _, err := ParseManifestName(secret + ".1"); err == nil {
		t.Fatal("a bad name was accepted")
	} else {
		noSecret(t, err)
	}
	if _, err := ParseHash(secret); err == nil {
		t.Fatal("a bad hash was accepted")
	} else {
		noSecret(t, err)
	}
}

func TestOversizeManifestIsNotRead(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	if err := os.WriteFile(filepath.Join(s.root, "manifests", "1.1"), bytes.Repeat([]byte("#"), maxManifestBytes+1), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReadManifest(context.Background(), "1.1"); err == nil {
		t.Fatal("an oversize manifest was read")
	}
}

func TestChunkSymlinkIsNotAChunk(t *testing.T) {
	t.Parallel()
	_, secretFile, _ := victim(t)
	s := openStore(t)
	h := Sum([]byte(secret + "\n"))
	if err := os.MkdirAll(filepath.Dir(s.ChunkPath(h)), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(secretFile, s.ChunkPath(h)); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.Has(h); err != nil || ok {
		t.Fatalf("Has = %v, %v for a symlink", ok, err)
	}
	var out bytes.Buffer
	if err := s.Get(context.Background(), h, &out); err == nil || out.Len() != 0 {
		t.Fatalf("Get followed a symlink: %v, %d bytes", err, out.Len())
	}
	// Put replaces the link and leaves its target alone.
	data := []byte(secret + "\n")
	if err := s.Put(context.Background(), h, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(secretFile); !bytes.Equal(b, data) { //nolint:gosec // a test path
		t.Fatalf("the link target changed: %q", b)
	}
	fi, err := os.Lstat(s.ChunkPath(h))
	if err != nil || !fi.Mode().IsRegular() {
		t.Fatalf("the chunk is not a regular file after Put: %v %v", fi, err)
	}
}

func TestChunkSizeIsBounded(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	data := make([]byte, 2*mib)
	data[0] = 1
	h := Sum(data)
	if err := s.Put(context.Background(), h, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := s.GetExact(context.Background(), h, mib, &out); !errors.Is(err, ErrCorrupt) || out.Len() != 0 {
		t.Fatalf("GetExact(wrong size) = %v after %d bytes", err, out.Len())
	}
	if err := s.GetExact(context.Background(), h, 4*mib, &out); !errors.Is(err, ErrCorrupt) || out.Len() != 0 {
		t.Fatalf("GetExact(larger size) = %v after %d bytes", err, out.Len())
	}
	if err := s.GetExact(context.Background(), h, 2*mib, &out); err != nil {
		t.Fatalf("GetExact(right size) = %v", err)
	}
}

func TestGetRefusesBeyondMaxChunkSize(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	h := Sum([]byte("x"))
	p := s.ChunkPath(h)
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(p) //nolint:gosec // a test path
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(MaxChunkSize + 1); err != nil { // sparse
		t.Fatal(err)
	}
	_ = f.Close()
	var out bytes.Buffer
	if err := s.Get(context.Background(), h, &out); !errors.Is(err, ErrCorrupt) || out.Len() != 0 {
		t.Fatalf("Get = %v after %d bytes", err, out.Len())
	}
}

// Publish renames over current and PublishFirst links to it; neither writes
// through a link that is there.
func TestPublishReplacesCurrentSymlinkWithoutWritingThrough(t *testing.T) {
	t.Parallel()
	_, secretFile, _ := victim(t)
	s := openStore(t)
	m := writeM(t, s, 1, 1, Sum([]byte("a")))
	if err := os.Symlink(secretFile, s.currentPath()); err != nil {
		t.Fatal(err)
	}
	if err := s.PublishFirst(context.Background(), m.Name()); err == nil {
		t.Fatal("PublishFirst linked over an existing name")
	}
	if err := s.Publish(context.Background(), m.Name()); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(secretFile); string(b) != secret+"\n" { //nolint:gosec // a test path
		t.Fatalf("the link target was written: %q", b)
	}
	if name, ok, err := s.Current(context.Background()); err != nil || !ok || name != m.Name() {
		t.Fatalf("Current = %q %v %v", name, ok, err)
	}
}

func TestPublishRefusesAManifestThatIsASymlink(t *testing.T) {
	t.Parallel()
	_, secretFile, _ := victim(t)
	s := openStore(t)
	if err := os.Symlink(secretFile, filepath.Join(s.root, "manifests", "1.1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Publish(context.Background(), "1.1"); err == nil {
		t.Fatal("Publish named a symlink as the manifest")
	}
}

func TestStaleTempRemovalStaysInTheStore(t *testing.T) {
	t.Parallel()
	v, _, old := victim(t)
	s := openStore(t)
	fan := filepath.Join(s.root, "chunks", "ab")
	if err := os.Mkdir(fan, 0o750); err != nil {
		t.Fatal(err)
	}
	// A stale temp file that is a link to a file outside is removed as a link.
	tmp := filepath.Join(fan, ".tmp-x")
	if err := os.Symlink(filepath.Join(v, "alice", ".bashrc"), tmp); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Prune(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	mustExist(t, old)
}

func empty(t *testing.T, dir string) {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		t.Errorf("%s: %s was created outside the store", dir, e.Name())
	}
}

// Every write path refuses a symlinked chunks/, manifests/ or fan directory
// and creates nothing behind it.
func TestWritesRefuseSymlinkedDirectories(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	data := []byte("some chunk")
	h := Sum(data)
	for name, tc := range map[string]struct {
		replace string // the directory (relative to the store) to swap for a link
		op      func(s *Store) error
	}{
		"Put into chunks": {"chunks", func(s *Store) error { return s.Put(ctx, h, bytes.NewReader(data), int64(len(data))) }},
		"Put into a fan":  {"chunks/" + h.String()[:2], func(s *Store) error { return s.Put(ctx, h, bytes.NewReader(data), int64(len(data))) }},
		"EnsureZero":      {"chunks", func(s *Store) error { return s.EnsureZero(ctx, mib) }},
		"WriteManifest": {"manifests", func(s *Store) error {
			return s.WriteManifest(ctx, Manifest{Volume: "v", Epoch: 1, Sequence: 1, Size: mib, Chunk: mib, Time: time.Unix(1, 0), Chunks: []Hash{ZeroHash(mib)}})
		}},
		"Publish":      {"manifests", func(s *Store) error { return s.Publish(ctx, "1.1") }},
		"PublishFirst": {"manifests", func(s *Store) error { return s.PublishFirst(ctx, "1.1") }},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			out := t.TempDir()
			s := openStore(t)
			target := filepath.Join(s.root, tc.replace)
			if tc.replace != "chunks" && tc.replace != "manifests" {
				if err := os.MkdirAll(target, 0o750); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.RemoveAll(target); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(out, target); err != nil {
				t.Fatal(err)
			}
			if err := tc.op(s); err == nil {
				t.Fatal("the write went through a symlinked directory")
			}
			empty(t, out)
		})
	}
}

// A store that is already open keeps writing into the directory it opened,
// whatever the root's name becomes.
func TestWritesFollowTheOpenedRootNotItsName(t *testing.T) {
	t.Parallel()
	out := t.TempDir()
	root := t.TempDir()
	s, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	moved := root + ".moved"
	if err := os.Rename(root, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(out, root); err != nil {
		t.Fatal(err)
	}
	m := writeM(t, s, 1, 1, ZeroHash(mib))
	if err := s.PublishFirst(context.Background(), m.Name()); err != nil {
		t.Fatal(err)
	}
	empty(t, out)
	if _, err := os.Lstat(filepath.Join(moved, "current")); err != nil {
		t.Fatalf("current is not in the opened directory: %v", err)
	}
}

// The largest manifest the format allows is read and written whole, and one
// byte more than its bound is not read.
func TestManifestAtTheLineCapRoundTrips(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	chunks := make([]Hash, MaxManifestLines)
	for i := range chunks {
		chunks[i] = ZeroHash(MinChunkSize)
	}
	m := Manifest{
		Volume: strings.Repeat("v", 200), Epoch: 18446744073709551615, Sequence: 18446744073709551615,
		Size: int64(MaxManifestLines) * MinChunkSize, Chunk: MinChunkSize, Time: time.Unix(1700000000, 0), Chunks: chunks,
	}
	if err := s.WriteManifest(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(s.manifestPath(m.Name())); err != nil || fi.Size() < 4_000_000 {
		t.Fatalf("manifest file: %v, %v", fi, err)
	}
	got, err := s.ReadManifest(context.Background(), m.Name())
	if err != nil || len(got.Chunks) != MaxManifestLines {
		t.Fatalf("ReadManifest at the cap = %d chunks, %v", len(got.Chunks), err)
	}
}
