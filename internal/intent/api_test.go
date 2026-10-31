package intent

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestOpenTightensAnExistingDirectory(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "intents")
	if err := os.Mkdir(dir, 0o755); err != nil { // #nosec G301 -- the test needs a loose directory
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o755); err != nil { // #nosec G302 -- same, immune to umask
		t.Fatal(err)
	}
	if _, err := Open(dir); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0o700 {
		t.Fatalf("mode = %o, want 700", perm)
	}
}

func TestOpenCreatesMissingParents(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "a", "b", "c")
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	mustSave(t, s, sample("pvc-1"))
	if got := mustLoad(t, s); len(got.Intents) != 1 {
		t.Fatalf("Intents = %v", got.Intents)
	}
}

func TestGet(t *testing.T) {
	t.Parallel()
	s, dir := openStore(t)
	in := sample("pvc-1")
	mustSave(t, s, in)

	got, ok, err := s.Get(in.VolumeID)
	if err != nil || !ok || !reflect.DeepEqual(got, in) {
		t.Fatalf("Get = %+v, %v, %v; want the saved record", got, ok, err)
	}

	got, ok, err = s.Get("directory:absent")
	if err != nil || ok || !reflect.DeepEqual(got, Intent{}) {
		t.Fatalf("Get of a missing record = %+v, %v, %v; want zero, false, nil", got, ok, err)
	}

	// A record that is there but invalid is an error, not "no intent".
	writeFile(t, filepath.Join(dir, "directory%3Abroken.json"), "{")
	got, ok, err = s.Get("directory:broken")
	if err == nil || ok || !reflect.DeepEqual(got, Intent{}) {
		t.Fatalf("Get of a corrupt record = %+v, %v, %v; want an error", got, ok, err)
	}
}

func TestGetWithMissingDirectoryIsAnError(t *testing.T) {
	t.Parallel()
	s, dir := openStore(t)
	if err := os.Remove(dir); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.Get("directory:x"); err == nil || ok {
		t.Fatalf("Get = _, %v, %v; want an error, not 'no intent'", ok, err)
	}
}

func TestUpdate(t *testing.T) {
	t.Parallel()

	t.Run("creates a record from a zero Intent with Version set", func(t *testing.T) {
		t.Parallel()
		s, _ := openStore(t)
		want := sample("pvc-1")
		err := s.Update(want.VolumeID, func(in *Intent) error {
			if !reflect.DeepEqual(*in, Intent{Version: Version}) {
				t.Errorf("fn got %+v, want only Version set", *in)
			}
			*in = want
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		got, ok, err := s.Get(want.VolumeID)
		if err != nil || !ok || !reflect.DeepEqual(got, want) {
			t.Fatalf("Get = %+v, %v, %v", got, ok, err)
		}
	})

	t.Run("changes an existing record", func(t *testing.T) {
		t.Parallel()
		s, dir := openStore(t)
		old := sample("pvc-1")
		mustSave(t, s, old)
		err := s.Update(old.VolumeID, func(in *Intent) error {
			if !reflect.DeepEqual(*in, old) {
				t.Errorf("fn got %+v, want the stored record", *in)
			}
			in.Publishes = append(in.Publishes, Publish{TargetPath: "/pods/a/mount"})
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		got, _, _ := s.Get(old.VolumeID)
		if len(got.Publishes) != 1 || got.Publishes[0].TargetPath != "/pods/a/mount" {
			t.Fatalf("Publishes = %v", got.Publishes)
		}
		if files := names(t, dir); len(files) != 1 {
			t.Fatalf("files = %v, want one", files)
		}
	})

	t.Run("a sentinel error aborts without writing", func(t *testing.T) {
		t.Parallel()
		s, dir := openStore(t)
		old := sample("pvc-1")
		mustSave(t, s, old)
		errAbort := errors.New("abort")
		err := s.Update(old.VolumeID, func(in *Intent) error {
			in.AccessMode = "SINGLE_NODE_WRITER"
			return errAbort
		})
		if err != errAbort {
			t.Fatalf("Update = %v, want the sentinel unchanged", err)
		}
		got, _, _ := s.Get(old.VolumeID)
		if !reflect.DeepEqual(got, old) {
			t.Fatalf("record changed to %+v", got)
		}
		// Aborting a create writes nothing either.
		err = s.Update("directory:new", func(*Intent) error { return errAbort })
		if err != errAbort {
			t.Fatalf("Update = %v", err)
		}
		if files := names(t, dir); len(files) != 1 {
			t.Fatalf("files = %v, want only the first record", files)
		}
	})

	t.Run("an invalid result is not written", func(t *testing.T) {
		t.Parallel()
		s, _ := openStore(t)
		old := sample("pvc-1")
		mustSave(t, s, old)
		err := s.Update(old.VolumeID, func(in *Intent) error {
			in.StagePath = "relative"
			return nil
		})
		if err == nil || !strings.Contains(err.Error(), "stagePath") {
			t.Fatalf("Update = %v, want a stagePath error", err)
		}
		if got, _, _ := s.Get(old.VolumeID); !reflect.DeepEqual(got, old) {
			t.Fatalf("record changed to %+v", got)
		}
	})

	t.Run("a new record must set its volume ID", func(t *testing.T) {
		t.Parallel()
		s, dir := openStore(t)
		err := s.Update("directory:pvc-1", func(*Intent) error { return nil })
		if err == nil {
			t.Fatal("Update of nothing succeeded")
		}
		if files := names(t, dir); len(files) != 0 {
			t.Fatalf("files = %v", files)
		}
	})

	t.Run("fn cannot move the record to another volume", func(t *testing.T) {
		t.Parallel()
		s, dir := openStore(t)
		old := sample("pvc-1")
		mustSave(t, s, old)
		err := s.Update(old.VolumeID, func(in *Intent) error {
			in.VolumeID = "directory:other"
			return nil
		})
		if err == nil {
			t.Fatal("Update changed the volume ID")
		}
		if files := names(t, dir); !reflect.DeepEqual(files, []string{"directory%3Apvc-1.json"}) {
			t.Fatalf("files = %v", files)
		}
	})

	t.Run("a corrupt record is an error and is kept; Save repairs it", func(t *testing.T) {
		t.Parallel()
		s, dir := openStore(t)
		p := filepath.Join(dir, "directory%3Apvc-1.json")
		writeFile(t, p, "{")
		called := false
		err := s.Update("directory:pvc-1", func(*Intent) error { called = true; return nil })
		if err == nil || called {
			t.Fatalf("Update = %v (fn called: %v), want an error before fn", err, called)
		}
		if b, _ := os.ReadFile(p); string(b) != "{" { // #nosec G304 -- test file
			t.Fatalf("corrupt record was touched: %q", b)
		}
		mustSave(t, s, sample("pvc-1"))
		if got := mustLoad(t, s); len(got.Failures) != 0 || len(got.Intents) != 1 {
			t.Fatalf("after Save: %+v", got)
		}
	})
}

func TestUpdateIsAtomicUnderConcurrency(t *testing.T) {
	t.Parallel()
	s, _ := openStore(t)
	in := sample("pvc-1")
	mustSave(t, s, in)

	const workers, per = 8, 10
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := range per {
				target := fmt.Sprintf("/pods/%d-%d/mount", w, r)
				err := s.Update(in.VolumeID, func(cur *Intent) error {
					cur.Publishes = append(cur.Publishes, Publish{TargetPath: target})
					return nil
				})
				if err != nil {
					t.Errorf("Update: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	got, _, err := s.Get(in.VolumeID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Publishes) != workers*per {
		t.Fatalf("%d publishes survived, want %d: concurrent Updates lost changes", len(got.Publishes), workers*per)
	}
}

// Error text names fields and rules, never values: a field that later holds a
// secret must not leak through an error. The subdir stands in for such a
// field.
func TestErrorsNeverContainFieldValues(t *testing.T) {
	t.Parallel()
	const sentinel = "SENTINEL-VALUE-7f3a"

	s, dir := openStore(t)
	check := func(what string, err error) {
		t.Helper()
		if err == nil {
			t.Errorf("%s: no error", what)
			return
		}
		if strings.Contains(err.Error(), sentinel) {
			t.Errorf("%s: error text contains the field value: %v", what, err)
		}
	}

	// Save: the sentinel is in a field that fails validation.
	bad := sample("pvc-1")
	bad.Subdir = sentinel + "/../x"
	check("Save with a bad subdir", s.Save(bad))
	bad = sample("pvc-1")
	bad.Subdir = sentinel
	bad.StagePath = "relative/" + sentinel
	check("Save with a bad stage path", s.Save(bad))
	bad = sample("pvc-1")
	bad.Subdir = sentinel
	bad.Publishes = []Publish{{TargetPath: "/p/" + sentinel}, {TargetPath: "/p/" + sentinel}}
	check("Save with duplicate targets", s.Save(bad))
	bad = sample("pvc-1")
	bad.Subdir = sentinel
	bad.MountFlags = []string{sentinel + ",x"}
	check("Save with a bad mount flag", s.Save(bad))
	bad = sample("pvc-1")
	bad.Subdir = sentinel
	bad.AccessMode = sentinel
	check("Save with a bad access mode", s.Save(bad))
	bad = sample("pvc-1")
	bad.Subdir = sentinel
	bad.Transport.Server = sentinel + "\n"
	check("Save with a bad server", s.Save(bad))

	// Update with a failing result.
	mustSave(t, s, sample("pvc-2"))
	check("Update to an invalid record", s.Update("directory:pvc-2", func(in *Intent) error {
		in.Subdir = sentinel + "/.."
		return nil
	}))

	// Load: records that fail in different ways, each carrying the sentinel.
	valid := sample("pvc-3")
	valid.Subdir = sentinel
	mustSave(t, s, valid)
	good, err := os.ReadFile(filepath.Join(dir, "directory%3Apvc-3.json")) // #nosec G304 -- test file
	if err != nil {
		t.Fatal(err)
	}
	text := string(good)
	variants := map[string]string{
		"directory%3Atruncated.json":  text[:len(text)/2],
		"directory%3Aversion.json":    strings.Replace(text, `"version": 1`, `"version": 2`, 1),
		"directory%3Awrong-type.json": strings.Replace(text, `"version": 1`, `"version": "`+sentinel+`"`, 1),
		"directory%3Amismatch.json":   text,
		"directory%3Astage.json":      strings.Replace(strings.Replace(text, "pvc-3", "stage", 1), `"stagePath": "/`, `"stagePath": "`+sentinel+`/`, 1),
		"directory%3Atrailing.json":   text + sentinel,
	}
	for name, content := range variants {
		writeFile(t, filepath.Join(dir, name), content)
	}
	got := mustLoad(t, s)
	if len(got.Failures) != len(variants) {
		t.Fatalf("got %d failures, want %d: %v", len(got.Failures), len(variants), got.Failures)
	}
	for _, f := range got.Failures {
		check("Load failure for "+filepath.Base(f.Path), f.Err)
	}
	for _, name := range []string{"directory:truncated", "directory:wrong-type"} {
		_, _, err := s.Get(name)
		check("Get of "+name, err)
	}
}
