package intent

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
)

func openStore(t *testing.T) (*Store, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "intents")
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s, dir
}

func mustSave(t *testing.T, s *Store, in Intent) {
	t.Helper()
	if err := s.Save(in); err != nil {
		t.Fatalf("Save(%s): %v", in.VolumeID, err)
	}
}

func mustLoad(t *testing.T, s *Store) Loaded {
	t.Helper()
	l, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return l
}

// names lists the entries of dir, sorted.
func names(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		out = append(out, e.Name())
	}
	sort.Strings(out)
	return out
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestOpen(t *testing.T) {
	t.Parallel()

	t.Run("creates the directory with mode 0700", func(t *testing.T) {
		t.Parallel()
		dir := filepath.Join(t.TempDir(), "a", "b")
		s, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		if s.Dir() != dir {
			t.Errorf("Dir() = %q, want %q", s.Dir(), dir)
		}
		fi, err := os.Stat(dir)
		if err != nil {
			t.Fatal(err)
		}
		if !fi.IsDir() {
			t.Fatal("not a directory")
		}
		// The final directory is made by MkdirAll with 0700, subject to
		// umask, which can only remove bits.
		if perm := fi.Mode().Perm(); perm&^0o700 != 0 {
			t.Errorf("mode = %o, want no bits beyond 0700", perm)
		}
	})

	t.Run("accepts an existing directory", func(t *testing.T) {
		t.Parallel()
		if _, err := Open(t.TempDir()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("rejects an empty path", func(t *testing.T) {
		t.Parallel()
		if _, err := Open(""); err == nil {
			t.Fatal("want an error")
		}
	})

	t.Run("rejects a path that is a file", func(t *testing.T) {
		t.Parallel()
		f := filepath.Join(t.TempDir(), "file")
		writeFile(t, f, "x")
		_, err := Open(f)
		if err == nil || !strings.Contains(err.Error(), f) {
			t.Fatalf("Open(file) = %v, want an error naming %s", err, f)
		}
	})
}

func TestSaveLoadRoundTrip(t *testing.T) {
	t.Parallel()
	s, dir := openStore(t)

	empty := sample("pvc-empty")

	full := sample("pvc-full")
	full.Publishes = []Publish{
		{TargetPath: "/pods/a/volumes/mount", ReadOnly: false},
		{TargetPath: "/pods/b/volumes/mount", ReadOnly: true},
	}
	full.MountFlags = []string{"ro", "noatime"}

	local := sample("pvc-local")
	local.Transport.Kind = "local"
	local.Transport.Server = ""
	local.Transport.Export = ""
	local.Transport.Path = "/srv/data"

	for _, in := range []Intent{empty, full, local} {
		mustSave(t, s, in)
	}

	got := mustLoad(t, s)
	if len(got.Failures) != 0 {
		t.Fatalf("Failures = %v, want none", got.Failures)
	}
	want := map[string]Intent{
		empty.VolumeID: empty,
		full.VolumeID:  full,
		local.VolumeID: local,
	}
	if !reflect.DeepEqual(got.Intents, want) {
		t.Fatalf("Intents =\n%+v\nwant\n%+v", got.Intents, want)
	}
	if got.Intents[empty.VolumeID].Publishes != nil {
		t.Errorf("empty Publishes loaded as %v, want nil", got.Intents[empty.VolumeID].Publishes)
	}

	wantFiles := []string{"directory%3Apvc-empty.json", "directory%3Apvc-full.json", "directory%3Apvc-local.json"}
	if gotFiles := names(t, dir); !reflect.DeepEqual(gotFiles, wantFiles) {
		t.Fatalf("files = %v, want %v", gotFiles, wantFiles)
	}
	fi, err := os.Stat(filepath.Join(dir, wantFiles[0]))
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Errorf("record mode = %o, want 600", perm)
	}
}

func TestRecordFormat(t *testing.T) {
	t.Parallel()
	s, dir := openStore(t)
	in := sample("pvc-1")
	in.Publishes = []Publish{{TargetPath: "/pods/a/mount", ReadOnly: true}}
	mustSave(t, s, in)

	data, err := os.ReadFile(filepath.Join(dir, "directory%3Apvc-1.json")) // #nosec G304 -- test file
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"version", "volumeID", "kind", "transport", "name", "stagePath", "publishes", "accessMode"} {
		if _, ok := raw[k]; !ok {
			t.Errorf("record has no %q key", k)
		}
	}
	if _, ok := raw["mountFlags"]; ok {
		t.Error("empty mountFlags should be omitted")
	}
	if string(raw["version"]) != "1" {
		t.Errorf("version = %s, want 1", raw["version"])
	}
}

func TestSaveReplacesAtomically(t *testing.T) {
	t.Parallel()
	s, dir := openStore(t)

	old := sample("pvc-1")
	mustSave(t, s, old)

	next := old
	next.Publishes = []Publish{{TargetPath: "/pods/a/mount"}}
	next.AccessMode = "SINGLE_NODE_WRITER"
	mustSave(t, s, next)

	got := mustLoad(t, s)
	if !reflect.DeepEqual(got.Intents[old.VolumeID], next) {
		t.Fatalf("loaded %+v, want the new record %+v", got.Intents[old.VolumeID], next)
	}
	if files := names(t, dir); !reflect.DeepEqual(files, []string{"directory%3Apvc-1.json"}) {
		t.Fatalf("files = %v, want only the record (no temp file)", files)
	}
}

func TestSaveRejectsInvalid(t *testing.T) {
	t.Parallel()
	s, dir := openStore(t)
	in := sample("pvc-1")
	in.Version = 7
	err := s.Save(in)
	if err == nil || !strings.Contains(err.Error(), "version 7") {
		t.Fatalf("Save = %v, want a version error", err)
	}
	if files := names(t, dir); len(files) != 0 {
		t.Fatalf("files = %v, want none after a rejected Save", files)
	}
}

func TestSaveMissingDirectory(t *testing.T) {
	t.Parallel()
	s, dir := openStore(t)
	if err := os.Remove(dir); err != nil {
		t.Fatal(err)
	}
	err := s.Save(sample("pvc-1"))
	if err == nil {
		t.Fatal("Save into a missing directory succeeded")
	}
	if !strings.Contains(err.Error(), dir) {
		t.Fatalf("error %q does not name the directory %s", err, dir)
	}
}

func TestSaveRenameFailureLeavesNoTemp(t *testing.T) {
	t.Parallel()
	s, dir := openStore(t)
	// A non-empty directory where the record belongs makes rename fail.
	blocker := filepath.Join(dir, "directory%3Apvc-1.json")
	if err := os.MkdirAll(filepath.Join(blocker, "x"), 0o700); err != nil {
		t.Fatal(err)
	}
	err := s.Save(sample("pvc-1"))
	if err == nil || !strings.Contains(err.Error(), blocker) {
		t.Fatalf("Save = %v, want an error naming %s", err, blocker)
	}
	if files := names(t, dir); !reflect.DeepEqual(files, []string{"directory%3Apvc-1.json"}) {
		t.Fatalf("files = %v, want only the blocking directory", files)
	}
}

func TestDelete(t *testing.T) {
	t.Parallel()
	s, dir := openStore(t)
	a, b := sample("pvc-a"), sample("pvc-b")
	mustSave(t, s, a)
	mustSave(t, s, b)

	for i := range 3 {
		if err := s.Delete(a.VolumeID); err != nil {
			t.Fatalf("Delete #%d: %v", i, err)
		}
	}
	if err := s.Delete("directory:never-existed"); err != nil {
		t.Fatalf("Delete of a missing record: %v", err)
	}
	got := mustLoad(t, s)
	if _, ok := got.Intents[a.VolumeID]; ok {
		t.Error("deleted record still loads")
	}
	if _, ok := got.Intents[b.VolumeID]; !ok {
		t.Error("Delete removed another volume's record")
	}
	if files := names(t, dir); !reflect.DeepEqual(files, []string{"directory%3Apvc-b.json"}) {
		t.Fatalf("files = %v", files)
	}
}

func TestDeleteError(t *testing.T) {
	t.Parallel()
	s, dir := openStore(t)
	blocker := filepath.Join(dir, "directory%3Apvc-1.json")
	if err := os.MkdirAll(filepath.Join(blocker, "x"), 0o700); err != nil {
		t.Fatal(err)
	}
	err := s.Delete("directory:pvc-1")
	if err == nil || !strings.Contains(err.Error(), blocker) {
		t.Fatalf("Delete = %v, want an error naming %s", err, blocker)
	}
}

func TestDeleteDoesNotEscapeDirectory(t *testing.T) {
	t.Parallel()
	s, dir := openStore(t)
	outside := filepath.Join(filepath.Dir(dir), "victim.json")
	writeFile(t, outside, "{}")
	// The escaped form of this ID contains no separator, so it can only name
	// a file inside the store.
	if err := s.Delete("../victim"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(outside); err != nil {
		t.Fatalf("file outside the store was touched: %v", err)
	}
}

func TestLoadMissingDirectory(t *testing.T) {
	t.Parallel()
	s, dir := openStore(t)
	if err := os.Remove(dir); err != nil {
		t.Fatal(err)
	}
	_, err := s.Load()
	if err == nil || !strings.Contains(err.Error(), dir) {
		t.Fatalf("Load = %v, want an error naming %s", err, dir)
	}
}

func TestLoadEmpty(t *testing.T) {
	t.Parallel()
	s, _ := openStore(t)
	got := mustLoad(t, s)
	if got.Intents == nil || len(got.Intents) != 0 || len(got.Failures) != 0 {
		t.Fatalf("Load of an empty store = %+v, want empty non-nil map and no failures", got)
	}
}

func TestLoadCorruptRecords(t *testing.T) {
	t.Parallel()

	marshal := func(t *testing.T, in Intent) string {
		t.Helper()
		b, err := json.Marshal(in)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	good := sample("pvc-good")
	good2 := sample("pvc-good2")
	good2.Publishes = []Publish{{TargetPath: "/pods/a/mount"}}

	type corrupt struct {
		file    string // file name in the store
		content func(t *testing.T) string
		want    string // substring of the failure
	}
	cases := []corrupt{
		{
			file: "directory%3Atruncated.json",
			content: func(t *testing.T) string {
				full := marshal(t, sample("truncated"))
				return full[:len(full)/2]
			},
			want: "decode",
		},
		{
			file:    "directory%3Aempty.json",
			content: func(*testing.T) string { return "" },
			want:    "decode",
		},
		{
			file:    "directory%3Aarray.json",
			content: func(*testing.T) string { return "[]" },
			want:    "decode",
		},
		{
			file: "directory%3Atrailing.json",
			content: func(t *testing.T) string {
				return marshal(t, sample("trailing")) + `{"extra":1}`
			},
			want: "decode",
		},
		{
			file: "directory%3Afuture.json",
			content: func(t *testing.T) string {
				in := sample("future")
				in.Version = 2
				return marshal(t, in)
			},
			want: "version 2",
		},
		{
			file: "directory%3Afile-name.json",
			content: func(t *testing.T) string {
				return marshal(t, sample("some-other-volume"))
			},
			want: "belongs in file",
		},
		{
			file: "unescaped:name.json",
			content: func(t *testing.T) string {
				return marshal(t, sample("unescaped-name"))
			},
			want: "belongs in file",
		},
		{
			file: "directory%3Ainvalid.json",
			content: func(t *testing.T) string {
				in := sample("invalid")
				in.StagePath = "relative/path"
				return marshal(t, in)
			},
			want: "stagePath",
		},
	}

	s, dir := openStore(t)
	mustSave(t, s, good)
	mustSave(t, s, good2)
	for _, c := range cases {
		writeFile(t, filepath.Join(dir, c.file), c.content(t))
	}
	// A directory that looks like a record.
	if err := os.Mkdir(filepath.Join(dir, "directory%3Aadir.json"), 0o700); err != nil {
		t.Fatal(err)
	}
	cases = append(cases, corrupt{file: "directory%3Aadir.json", want: ""})

	// Stray temp files from a crash, and a file that is not ours.
	strayTemp := filepath.Join(dir, ".directory%3Apvc-good.json.tmp-0123456789abcdef")
	writeFile(t, strayTemp, `{"partial`)
	other := filepath.Join(dir, "README")
	writeFile(t, other, "not a record")

	before := names(t, dir)
	got := mustLoad(t, s)

	if len(got.Intents) != 2 || !reflect.DeepEqual(got.Intents[good.VolumeID], good) ||
		!reflect.DeepEqual(got.Intents[good2.VolumeID], good2) {
		t.Fatalf("good records did not load: %+v", got.Intents)
	}
	if len(got.Failures) != len(cases) {
		t.Fatalf("got %d failures, want %d: %v", len(got.Failures), len(cases), got.Failures)
	}
	byPath := make(map[string]error)
	for _, f := range got.Failures {
		if f.Err == nil {
			t.Errorf("failure for %s has no error", f.Path)
		}
		byPath[f.Path] = f.Err
	}
	for _, c := range cases {
		p := filepath.Join(dir, c.file)
		err, ok := byPath[p]
		if !ok {
			t.Errorf("no failure reported for %s", p)
			continue
		}
		if c.want != "" && !strings.Contains(err.Error(), c.want) {
			t.Errorf("failure for %s = %q, want it to contain %q", c.file, err, c.want)
		}
		if _, err := os.Lstat(p); err != nil {
			t.Errorf("corrupt file %s was not kept: %v", c.file, err)
		}
	}
	if _, err := os.Lstat(strayTemp); !os.IsNotExist(err) {
		t.Errorf("stray temp file still present (Lstat err = %v)", err)
	}
	if _, err := os.Lstat(other); err != nil {
		t.Errorf("file that is not a record was removed: %v", err)
	}
	// Nothing but the stray temp file disappeared.
	after := names(t, dir)
	var removed []string
	for _, n := range before {
		if !contains(after, n) {
			removed = append(removed, n)
		}
	}
	if !reflect.DeepEqual(removed, []string{filepath.Base(strayTemp)}) {
		t.Errorf("Load removed %v, want only the stray temp file", removed)
	}

	// A second Load reports the same failures: nothing was repaired or moved.
	again := mustLoad(t, s)
	if len(again.Failures) != len(cases) || len(again.Intents) != 2 {
		t.Errorf("second Load: %d intents, %d failures", len(again.Intents), len(again.Failures))
	}
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func TestLoadUnreadableRecord(t *testing.T) {
	t.Parallel()
	if os.Geteuid() == 0 {
		t.Skip("running as root: chmod 000 does not make a file unreadable to root")
	}
	s, dir := openStore(t)
	good := sample("pvc-good")
	mustSave(t, s, good)
	bad := sample("pvc-bad")
	mustSave(t, s, bad)
	badPath := filepath.Join(dir, "directory%3Apvc-bad.json")
	if err := os.Chmod(badPath, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(badPath, 0o600) })

	got := mustLoad(t, s)
	if _, ok := got.Intents[good.VolumeID]; !ok || len(got.Intents) != 1 {
		t.Fatalf("Intents = %v, want only the good record", got.Intents)
	}
	if len(got.Failures) != 1 || got.Failures[0].Path != badPath {
		t.Fatalf("Failures = %v, want one for %s", got.Failures, badPath)
	}
	if _, err := os.Lstat(badPath); err != nil {
		t.Fatalf("unreadable record was not kept: %v", err)
	}
}

func TestSaveRepairsALoadFailure(t *testing.T) {
	t.Parallel()
	s, dir := openStore(t)
	p := filepath.Join(dir, "directory%3Apvc-1.json")
	writeFile(t, p, "{")
	if got := mustLoad(t, s); len(got.Failures) != 1 {
		t.Fatalf("Failures = %v, want 1", got.Failures)
	}
	in := sample("pvc-1")
	mustSave(t, s, in)
	got := mustLoad(t, s)
	if len(got.Failures) != 0 || !reflect.DeepEqual(got.Intents[in.VolumeID], in) {
		t.Fatalf("after Save: %+v", got)
	}
}

func TestConcurrentUse(t *testing.T) {
	t.Parallel()
	s, dir := openStore(t)

	const writers, rounds = 8, 20
	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			in := sample(fmt.Sprintf("pvc-%d", w))
			for r := range rounds {
				in.Publishes = []Publish{{TargetPath: fmt.Sprintf("/pods/%d/mount", r)}}
				if err := s.Save(in); err != nil {
					t.Errorf("Save: %v", err)
					return
				}
				if r%5 == 4 {
					if err := s.Delete(in.VolumeID); err != nil {
						t.Errorf("Delete: %v", err)
						return
					}
				}
			}
		}()
	}
	// Loads run alongside the writers and must never see a failure: a
	// record is either absent or complete, and a live temp file is never
	// removed from under a Save.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 50 {
			l, err := s.Load()
			if err != nil {
				t.Errorf("Load: %v", err)
				return
			}
			if len(l.Failures) != 0 {
				t.Errorf("Load saw failures: %v", l.Failures)
				return
			}
		}
	}()
	wg.Wait()

	for _, n := range names(t, dir) {
		if strings.HasPrefix(n, ".") {
			t.Errorf("temp file left behind: %s", n)
		}
	}
}
