package chunk

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
)

const (
	mib = int64(1) << 20
	gib = int64(1) << 30
	tib = int64(1) << 40
)

func TestDefaultChunkSizeTable(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ size, want int64 }{
		{1 * gib, 4 * mib}, {256 * gib, 4 * mib}, {256*gib + 1, 8 * mib},
		{512 * gib, 8 * mib}, {4 * tib, 64 * mib},
	} {
		if got := DefaultChunkSize(tc.size); got != tc.want {
			t.Errorf("DefaultChunkSize(%d) = %d, want %d", tc.size, got, tc.want)
		}
	}
}

func TestCheckChunkSizeRejects(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		size, chunk int64
		ok          bool
	}{
		{"not a power of two", gib, 3 * mib, false},
		{"below minimum", gib, 512 << 10, false},
		{"too many lines", 1 * tib, 4 * mib, false},
		{"above maximum", 1 * tib, 512 * mib, false},
		{"zero size", 0, 4 * mib, false},
		{"fine", gib, 4 * mib, true},
		{"edge", 256 * gib, 4 * mib, true},
	} {
		err := CheckChunkSize(tc.size, tc.chunk)
		if (err == nil) != tc.ok {
			t.Errorf("%s: err = %v, want ok=%v", tc.name, err, tc.ok)
		}
	}
}

func TestRoundUp(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ size, chunk, want int64 }{
		{0, 4 * mib, 0}, {1, 4 * mib, 4 * mib}, {4 * mib, 4 * mib, 4 * mib}, {4*mib + 1, 4 * mib, 8 * mib},
	} {
		if got := RoundUp(tc.size, tc.chunk); got != tc.want {
			t.Errorf("RoundUp(%d,%d) = %d, want %d", tc.size, tc.chunk, got, tc.want)
		}
	}
}

func TestZeroHashIsStable(t *testing.T) {
	t.Parallel()
	want := Sum(make([]byte, mib))
	if got := ZeroHash(mib); got != want || ZeroHash(mib) != want {
		t.Fatalf("ZeroHash(1MiB) = %s, want %s", got, want)
	}
	if ZeroHash(mib) == ZeroHash(2*mib) {
		t.Fatal("different sizes share a zero hash")
	}
	// Well-known: sha256 of 1 MiB of zeros.
	if got := want.String(); got != "30e14955ebf1352266dc2ff8067e68104607e750abb9d3b36582b8af909fcb58" {
		t.Fatalf("zero chunk name = %s", got)
	}
}

func TestParseHash(t *testing.T) {
	t.Parallel()
	h := Sum([]byte("x"))
	if got, err := ParseHash(h.String()); err != nil || got != h {
		t.Fatalf("round trip: %v %v", got, err)
	}
	for _, s := range []string{"", "abc", strings.ToUpper(h.String()), strings.Repeat("g", 64), h.String() + "0"} {
		if _, err := ParseHash(s); err == nil {
			t.Errorf("ParseHash(%q) accepted", s)
		}
	}
}

func testManifest() Manifest {
	return Manifest{
		Volume: "pvc-0001", Epoch: 7, Sequence: 12, Size: 3 * mib, Chunk: mib,
		Time:   time.Date(2026, 11, 3, 10, 30, 0, 0, time.UTC),
		Chunks: []Hash{Sum([]byte("a")), ZeroHash(mib), Sum([]byte("b"))},
	}
}

func TestManifestRoundTrip(t *testing.T) {
	t.Parallel()
	m := testManifest()
	got := m.Marshal()
	golden, err := os.ReadFile("testdata/manifest.golden")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, golden) {
		t.Fatalf("golden mismatch:\n%s\nwant:\n%s", got, golden)
	}
	back, err := ParseManifest(bytes.NewReader(got))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(back.Marshal(), got) || back.Name() != "7.12" || !back.Time.Equal(m.Time) || !slices.Equal(back.Chunks, m.Chunks) {
		t.Fatalf("round trip differs: %+v", back)
	}
}

func TestParseManifestRejectsCountMismatch(t *testing.T) {
	t.Parallel()
	m := testManifest()
	for name, mutate := range map[string]func(*Manifest){
		"one too few":  func(m *Manifest) { m.Chunks = m.Chunks[:2] },
		"one too many": func(m *Manifest) { m.Chunks = append(m.Chunks, Sum([]byte("c"))) },
		"none":         func(m *Manifest) { m.Chunks = nil },
		"size not whole": func(m *Manifest) {
			m.Size++
		},
	} {
		mm := m
		mm.Chunks = slices.Clone(m.Chunks)
		mutate(&mm)
		if _, err := ParseManifest(bytes.NewReader(mm.Marshal())); !errors.Is(err, ErrManifestFormat) {
			t.Errorf("%s: err = %v, want ErrManifestFormat", name, err)
		}
	}
}

func TestParseManifestRejectsUnknownFormat(t *testing.T) {
	t.Parallel()
	good := string(testManifest().Marshal())
	for name, in := range map[string]string{
		"format 2":       strings.Replace(good, "# format 1", "# format 2", 1),
		"no format":      strings.Replace(good, "# format 1\n", "", 1),
		"format not 1st": strings.Replace(good, "# format 1\n", "", 1) + "# format 1\n",
		"missing key":    strings.Replace(good, "# epoch 7\n", "", 1),
		"duplicate key":  strings.Replace(good, "# epoch 7\n", "# epoch 7\n# epoch 7\n", 1),
		"bad hash":       strings.Replace(good, "\n"+Sum([]byte("a")).String(), "\nXYZ", 1),
		"upper hash":     strings.Replace(good, Sum([]byte("a")).String(), strings.ToUpper(Sum([]byte("a")).String()), 1),
		"empty":          "",
		"bad time":       strings.Replace(good, "2026-11-03T10:30:00Z", "yesterday", 1),
		"header late":    good + "# epoch 7\n",
		"plus size":      regexp.MustCompile(`# size (\d+)`).ReplaceAllString(good, "# size +$1"),
		"zero-led chunk": regexp.MustCompile(`# chunk (\d+)`).ReplaceAllString(good, "# chunk 0$1"),
	} {
		if _, err := ParseManifest(strings.NewReader(in)); !errors.Is(err, ErrManifestFormat) {
			t.Errorf("%s: err = %v, want ErrManifestFormat", name, err)
		}
	}
}

func FuzzParseManifest(f *testing.F) {
	f.Add(testManifest().Marshal())
	f.Add([]byte("# format 1\n"))
	f.Add([]byte("# format 2\n"))
	f.Add([]byte(""))
	f.Fuzz(func(t *testing.T, in []byte) {
		m, err := ParseManifest(bytes.NewReader(in))
		if err != nil {
			if !errors.Is(err, ErrManifestFormat) {
				t.Fatalf("error without ErrManifestFormat: %v", err)
			}
			return
		}
		if int64(len(m.Chunks))*m.Chunk != m.Size {
			t.Fatalf("accepted a manifest with %d lines for size %d chunk %d", len(m.Chunks), m.Size, m.Chunk)
		}
		again, err := ParseManifest(bytes.NewReader(m.Marshal()))
		if err != nil {
			t.Fatalf("a manifest that parsed does not parse again after Marshal: %v", err)
		}
		if !bytes.Equal(again.Marshal(), m.Marshal()) {
			t.Fatal("marshal is not stable")
		}
	})
}

func TestManifestNames(t *testing.T) {
	t.Parallel()
	for _, s := range []string{"", "1", "1.", ".1", "01.2", "1.02", "-1.2", "1.2.3", "a.b", "../x.1"} {
		if _, _, err := ParseManifestName(s); !errors.Is(err, ErrManifestFormat) {
			t.Errorf("ParseManifestName(%q) = %v", s, err)
		}
	}
	if e, q, err := ParseManifestName("10.2"); err != nil || e != 10 || q != 2 {
		t.Fatalf("got %d %d %v", e, q, err)
	}
}

func openStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestOpenRequiresRoot(t *testing.T) {
	t.Parallel()
	if _, err := Open(filepath.Join(t.TempDir(), "absent")); err == nil {
		t.Fatal("Open created a missing root")
	}
}

func TestPutVerifiesAndIsIdempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	data := []byte("hello chunk")
	h := Sum(data)
	if err := s.Put(ctx, h, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	p := s.ChunkPath(h)
	if !strings.HasSuffix(p, filepath.Join("chunks", h.String()[:2], h.String())) {
		t.Fatalf("path %s", p)
	}
	if ok, err := s.Has(h); !ok || err != nil {
		t.Fatalf("Has = %v %v", ok, err)
	}
	before, _ := os.Stat(p)
	// Present: a second Put, even with different bytes, rewrites nothing.
	if err := s.Put(ctx, h, strings.NewReader("other"), 5); err != nil {
		t.Fatal(err)
	}
	after, _ := os.Stat(p)
	if !os.SameFile(before, after) {
		t.Fatal("chunk was rewritten")
	}
	// Mismatch: ErrCorrupt, nothing stored, no temp left.
	bad := Sum([]byte("promised"))
	if err := s.Put(ctx, bad, strings.NewReader("delivered"), 9); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v", err)
	}
	if ok, _ := s.Has(bad); ok {
		t.Fatal("corrupt chunk stored")
	}
	// Short read.
	if err := s.Put(ctx, bad, strings.NewReader("abc"), 9); err == nil {
		t.Fatal("short read accepted")
	}
	ents, _ := os.ReadDir(filepath.Dir(s.ChunkPath(bad)))
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), ".tmp-") {
			t.Fatalf("leftover %s", e.Name())
		}
	}
	// Cancelled.
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := s.Put(cctx, Sum([]byte("z")), strings.NewReader("z"), 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
}

func TestGetDetectsCorruptChunk(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	data := []byte("payload bytes")
	h := Sum(data)
	if err := s.Put(ctx, h, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := s.Get(ctx, h, &out); err != nil || !bytes.Equal(out.Bytes(), data) {
		t.Fatalf("Get = %v %q", err, out.Bytes())
	}
	for name, corrupt := range map[string][]byte{"flipped": []byte("payload bytez"), "truncated": data[:5], "empty": nil} {
		if err := os.WriteFile(s.ChunkPath(h), corrupt, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := s.Get(ctx, h, &bytes.Buffer{}); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: err = %v, want ErrCorrupt", name, err)
		}
	}
	if err := s.Get(ctx, Sum([]byte("absent")), &bytes.Buffer{}); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("absent: %v", err)
	}
}

func writeM(t *testing.T, s *Store, epoch, seq uint64, hs ...Hash) Manifest {
	t.Helper()
	m := Manifest{
		Volume: "v", Epoch: epoch, Sequence: seq, Size: int64(len(hs)) * mib, Chunk: mib,
		Time: time.Unix(1700000000, 0), Chunks: hs,
	}
	if err := s.WriteManifest(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestPublishReplacesCurrent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	if _, ok, err := s.Current(ctx); ok || err != nil {
		t.Fatalf("fresh store: %v %v", ok, err)
	}
	if _, err := s.ReadManifest(ctx, ""); !errors.Is(err, ErrNoCurrent) {
		t.Fatalf("err = %v", err)
	}
	a := writeM(t, s, 1, 1, Sum([]byte("a")))
	b := writeM(t, s, 1, 2, Sum([]byte("b")))
	for _, m := range []Manifest{a, b} {
		if err := s.Publish(ctx, m.Name()); err != nil {
			t.Fatal(err)
		}
		if got, ok, err := s.Current(ctx); got != m.Name() || !ok || err != nil {
			t.Fatalf("Current = %q %v %v", got, ok, err)
		}
	}
	back, err := s.ReadManifest(ctx, "1.2")
	if err != nil || back.Chunks[0] != b.Chunks[0] {
		t.Fatalf("ReadManifest = %+v %v", back, err)
	}
	if err := s.Publish(ctx, "9.9"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("publishing a missing manifest: %v", err)
	}
	ents, _ := os.ReadDir(s.root)
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), ".current-") {
			t.Fatalf("leftover %s", e.Name())
		}
	}
}

func TestCurrentIsReadFresh(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	writeM(t, s, 1, 1, Sum([]byte("a")))
	writeM(t, s, 1, 2, Sum([]byte("b")))
	if err := s.Publish(ctx, "1.1"); err != nil {
		t.Fatal(err)
	}
	if got, _, _ := s.Current(ctx); got != "1.1" {
		t.Fatal(got)
	}
	// Another writer replaces current behind this Store's back.
	other, err := Open(s.root)
	if err != nil {
		t.Fatal(err)
	}
	if err := other.Publish(ctx, "1.2"); err != nil {
		t.Fatal(err)
	}
	if got, _, _ := s.Current(ctx); got != "1.2" {
		t.Fatalf("stale current %q", got)
	}
	if err := os.WriteFile(filepath.Join(s.root, "current"), []byte("../evil\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Current(ctx); !errors.Is(err, ErrManifestFormat) {
		t.Fatalf("err = %v", err)
	}
}

func TestWriteManifestRefusesReplace(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	m := writeM(t, s, 2, 1, Sum([]byte("a")))
	other := m
	other.Chunks = []Hash{Sum([]byte("different"))}
	if err := s.WriteManifest(ctx, other); !errors.Is(err, fs.ErrExist) {
		t.Fatalf("err = %v, want fs.ErrExist", err)
	}
	back, _ := s.ReadManifest(ctx, m.Name())
	if back.Chunks[0] != m.Chunks[0] {
		t.Fatal("manifest was replaced")
	}
	bad := m
	bad.Sequence = 2
	bad.Chunks = nil
	if err := s.WriteManifest(ctx, bad); !errors.Is(err, ErrManifestFormat) {
		t.Fatalf("invalid manifest written: %v", err)
	}
	names, _ := s.Manifests(ctx)
	if !slices.Equal(names, []string{"2.1"}) {
		t.Fatalf("names %v (temp files must not list)", names)
	}
}

func TestManifestsSortByEpochThenSequence(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	h := Sum([]byte("a"))
	for _, k := range [][2]uint64{{2, 1}, {1, 10}, {1, 9}, {10, 1}, {1, 2}} {
		writeM(t, s, k[0], k[1], h)
	}
	if err := os.WriteFile(filepath.Join(s.root, "manifests", "junk"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := s.Manifests(context.Background())
	want := []string{"1.2", "1.9", "1.10", "2.1", "10.1"}
	if err != nil || !slices.Equal(got, want) {
		t.Fatalf("got %v %v, want %v", got, err, want)
	}
}

func TestPruneKeepsNewestAndTheirChunks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	hs := map[string]Hash{}
	for _, n := range []string{"old", "shared", "mid", "new"} {
		hs[n] = Sum([]byte(n))
		if err := s.Put(ctx, hs[n], strings.NewReader(n), int64(len(n))); err != nil {
			t.Fatal(err)
		}
	}
	writeM(t, s, 1, 1, hs["old"], hs["shared"])
	writeM(t, s, 1, 2, hs["shared"], hs["mid"])
	writeM(t, s, 2, 1, hs["shared"], hs["new"])
	if err := s.Publish(ctx, "1.1"); err != nil { // current is the oldest
		t.Fatal(err)
	}
	tmp := filepath.Join(filepath.Dir(s.ChunkPath(hs["old"])), ".tmp-young")
	if err := os.WriteFile(tmp, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, _, err := s.Prune(ctx, 0); err == nil {
		t.Fatal("keep 0 accepted")
	}
	rm, rc, err := s.Prune(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	// Kept: 2.1 (newest) and 1.1 (current's target). Dropped: 1.2 and "mid".
	if rm != 1 || rc != 1 {
		t.Fatalf("removed %d manifests, %d chunks; want 1, 1", rm, rc)
	}
	names, _ := s.Manifests(ctx)
	if !slices.Equal(names, []string{"1.1", "2.1"}) {
		t.Fatalf("manifests %v", names)
	}
	for n, want := range map[string]bool{"old": true, "shared": true, "mid": false, "new": true} {
		if ok, _ := s.Has(hs[n]); ok != want {
			t.Errorf("Has(%s) = %v, want %v", n, ok, want)
		}
	}
	if _, err := os.Stat(tmp); err != nil {
		t.Fatal("young temp file removed")
	}
	// A second run is a no-op.
	if rm, rc, err := s.Prune(ctx, 1); err != nil || rm != 0 || rc != 0 {
		t.Fatalf("second prune %d %d %v", rm, rc, err)
	}
}

func TestPruneChangesNothingWhenAKeptManifestIsUnreadable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	h := Sum([]byte("a"))
	_ = s.Put(ctx, h, strings.NewReader("a"), 1)
	writeM(t, s, 1, 1, h)
	writeM(t, s, 1, 2, h)
	if err := os.WriteFile(filepath.Join(s.root, "manifests", "1.2"), []byte("garbage"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Prune(ctx, 1); !errors.Is(err, ErrManifestFormat) {
		t.Fatalf("err = %v", err)
	}
	names, _ := s.Manifests(ctx)
	if ok, _ := s.Has(h); !ok || len(names) != 2 {
		t.Fatal("prune deleted something before failing")
	}
}
