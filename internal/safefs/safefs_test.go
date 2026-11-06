package safefs

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

func link(t *testing.T, target, name string) {
	t.Helper()
	if err := os.Symlink(target, name); err != nil {
		t.Fatal(err)
	}
}

func TestOpenRootRefusesASymlink(t *testing.T) {
	t.Parallel()
	target := t.TempDir()
	l := filepath.Join(t.TempDir(), "l")
	link(t, target, l)
	if _, err := OpenRoot(l); err == nil {
		t.Fatal("OpenRoot followed a symlink")
	}
	if _, err := OpenRoot(filepath.Join(target, "missing")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("OpenRoot(missing) = %v", err)
	}
}

func TestOpenDoesNotFollowSymlinksAnywhere(t *testing.T) {
	t.Parallel()
	root, outside := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "f"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	link(t, outside, filepath.Join(root, "dirlink"))
	link(t, filepath.Join(outside, "f"), filepath.Join(root, "filelink"))
	d, err := OpenRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	for _, rel := range []string{"dirlink/f", "filelink", "../" + filepath.Base(outside) + "/f"} {
		if _, _, err := d.OpenRegular(rel, 10); err == nil {
			t.Errorf("OpenRegular(%q) succeeded", rel)
		}
	}
	if _, err := d.Stat("filelink"); err == nil {
		t.Error("Stat followed a symlink")
	}
	if _, err := d.Sub("dirlink"); !IsSymlinkError(err) {
		t.Errorf("Sub(dirlink) = %v, want a symlink refusal", err)
	}
}

func TestReadFileCapsAndRequiresRegular(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "big"), make([]byte, 100), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "ok"), []byte("hi"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "dir"), 0o750); err != nil {
		t.Fatal(err)
	}
	d, _ := OpenRoot(root)
	defer func() { _ = d.Close() }()
	if _, err := d.ReadFile("big", 99); !errors.Is(err, ErrTooLarge) {
		t.Errorf("big = %v", err)
	}
	if _, err := d.ReadFile("dir", 99); !errors.Is(err, ErrNotRegular) {
		t.Errorf("dir = %v", err)
	}
	if b, err := d.ReadFile("ok", 2); err != nil || string(b) != "hi" {
		t.Errorf("ok = %q, %v", b, err)
	}
}

func TestNameOperationsStayInTheDirectory(t *testing.T) {
	t.Parallel()
	root, outside := t.TempDir(), t.TempDir()
	victim := filepath.Join(outside, "v")
	if err := os.WriteFile(victim, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	link(t, victim, filepath.Join(root, "l"))
	d, _ := OpenRoot(root)
	defer func() { _ = d.Close() }()
	for _, bad := range []string{"", ".", "..", "a/b", "../x"} {
		if err := d.Unlink(bad); err == nil {
			t.Errorf("Unlink(%q) accepted", bad)
		}
	}
	// A link is removed as a link.
	if err := d.Unlink("l"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(victim); err != nil {
		t.Fatalf("target removed: %v", err)
	}
	// EnsureSub refuses a link and a file, and creates a missing directory.
	link(t, outside, filepath.Join(root, "sub"))
	if _, err := d.EnsureSub("sub", 0o755); err == nil {
		t.Error("EnsureSub followed a symlink")
	}
	s, err := d.EnsureSub("new", 0o755)
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	// Link refuses to replace; Rename replaces.
	f, err := d.Create("a", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if err := d.Link("a", d, "b"); err != nil {
		t.Fatal(err)
	}
	if err := d.Link("a", d, "b"); !errors.Is(err, fs.ErrExist) {
		t.Errorf("Link over an existing name = %v", err)
	}
	if err := d.Rename("a", d, "b"); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Create("b", 0o644); !errors.Is(err, fs.ErrExist) {
		t.Errorf("Create over an existing name = %v", err)
	}
}

func TestLstatAndReadDirReportLinksAsLinks(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	link(t, "/nonexistent", filepath.Join(root, "l"))
	d, _ := OpenRoot(root)
	defer func() { _ = d.Close() }()
	fi, err := d.Lstat("l")
	if err != nil || fi.Mode()&fs.ModeSymlink == 0 {
		t.Fatalf("Lstat = %v, %v", fi, err)
	}
	ents, err := d.ReadDir()
	if err != nil || len(ents) != 1 || ents[0].Type()&fs.ModeSymlink == 0 {
		t.Fatalf("ReadDir = %v, %v", ents, err)
	}
}

func TestCheckResolves(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "d"), 0o750); err != nil {
		t.Fatal(err)
	}
	link(t, filepath.Join(root, "d"), filepath.Join(root, "l"))
	if err := CheckResolves(filepath.Join(root, "d")); err != nil {
		t.Errorf("a plain directory: %v", err)
	}
	for _, p := range []string{filepath.Join(root, "l"), filepath.Join(root, "l", "."), filepath.Join(root, "missing")} {
		if err := CheckResolves(p); err == nil {
			t.Errorf("CheckResolves(%s) accepted", p)
		}
	}
	// An unclean spelling of a plain path is not "itself".
	if err := CheckResolves(filepath.Join(root, "d") + "/../d"); err == nil {
		t.Error("an unclean path was accepted")
	}
}

func TestOpenRootRefusesASymlinkInThePrefixButOperatorDoesNot(t *testing.T) {
	t.Parallel()
	target := t.TempDir()
	if err := os.Mkdir(filepath.Join(target, "d"), 0o750); err != nil {
		t.Fatal(err)
	}
	l := filepath.Join(t.TempDir(), "l")
	link(t, target, l)
	if _, err := OpenRoot(filepath.Join(l, "d")); err == nil {
		t.Error("OpenRoot followed a symlink in the prefix")
	}
	d, err := OpenRootOperator(filepath.Join(l, "d"))
	if err != nil {
		t.Fatalf("OpenRootOperator: %v", err)
	}
	_ = d.Close()
	if _, err := OpenRootOperator(l); err == nil {
		t.Error("OpenRootOperator followed a symlink in the last element")
	}
}
