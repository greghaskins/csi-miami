package lock

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestOSDirWritesAtomicallyAndReadsRelativeToItself(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	var o osFS
	h, err := o.OpenDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = h.Close() }()

	if err := h.WriteAtomic("holder", []byte("h")); err != nil {
		t.Fatal(err)
	}
	if err := h.WriteAtomic("holder", []byte("h2")); err != nil {
		t.Fatal(err)
	}
	if got := names(t, dir); !slices.Equal(got, []string{"holder"}) || read(t, filepath.Join(dir, "holder")) != "h2" {
		t.Errorf("directory holds %v, holder %q", got, read(t, filepath.Join(dir, "holder")))
	}
	if b, err := h.ReadFile("holder"); err != nil || string(b) != "h2" {
		t.Errorf("ReadFile = %q, %v", b, err)
	}
	if _, err := h.ReadFile("missing"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("ReadFile of a missing file = %v, want ErrNotExist", err)
	}

	// The rename fails (the target is a directory with something in it): the
	// temporary file does not stay.
	if err := os.MkdirAll(filepath.Join(dir, "since", "x"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := h.WriteAtomic("since", nil); err == nil {
		t.Error("WriteAtomic over a non-empty directory succeeded")
	}
	if got := names(t, dir); !slices.Equal(got, []string{"holder", "since"}) {
		t.Errorf("directory holds %v after a failed write", got)
	}
}

func TestOSDirFollowsTheDirectoryNotThePath(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	path, side := filepath.Join(base, "lock"), filepath.Join(base, "side")
	if err := os.Mkdir(path, 0o750); err != nil {
		t.Fatal(err)
	}
	var o osFS
	h, err := o.OpenDir(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = h.Close() }()
	if same, err := h.Same(path); err != nil || !same {
		t.Errorf("Same(path) = %v, %v; want true", same, err)
	}

	// Moved away and replaced by another directory under the same path.
	if err := os.Rename(path, side); err != nil {
		t.Fatal(err)
	}
	if same, err := h.Same(path); err != nil || same {
		t.Errorf("Same(path) of a missing path = %v, %v; want false", same, err)
	}
	if err := os.Mkdir(path, 0o750); err != nil {
		t.Fatal(err)
	}
	if same, err := h.Same(path); err != nil || same {
		t.Errorf("Same(path) of another directory = %v, %v; want false", same, err)
	}
	if same, err := h.Same(side); err != nil || !same {
		t.Errorf("Same(moved) = %v, %v; want true", same, err)
	}
	// A write through the handle lands in the moved directory, not the new one.
	if err := h.WriteAtomic("since", nil); err != nil {
		t.Fatal(err)
	}
	if got := names(t, path); len(got) != 0 {
		t.Errorf("the new directory holds %v, want nothing", got)
	}
	if got := names(t, side); !slices.Equal(got, []string{"since"}) {
		t.Errorf("the moved directory holds %v, want since", got)
	}

	// Removed: the handle answers "does not exist".
	if err := os.RemoveAll(side); err != nil {
		t.Fatal(err)
	}
	if err := h.WriteAtomic("since", nil); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("WriteAtomic in a removed directory = %v, want ErrNotExist", err)
	}
	if _, err := h.ReadFile("holder"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("ReadFile in a removed directory = %v, want ErrNotExist", err)
	}
	// A symbolic link is not opened.
	if err := os.Symlink(path, filepath.Join(base, "link")); err != nil {
		t.Fatal(err)
	}
	if same, err := h.Same(filepath.Join(base, "link")); err != nil || same {
		t.Errorf("Same(symlink) = %v, %v; want false", same, err)
	}
	if _, err := o.OpenDir(filepath.Join(base, "link")); err == nil {
		t.Error("OpenDir followed a symbolic link")
	}
	if _, err := o.OpenDir(filepath.Join(base, "missing")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("OpenDir of a missing directory = %v, want ErrNotExist", err)
	}
}

func TestOSFSProbeReadsAndRemoves(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	var o osFS
	before := time.Now().Add(-time.Minute)
	got, err := o.Probe(dir, "web")
	if err != nil {
		t.Fatal(err)
	}
	// On a local file system the "server" is this machine; the comparison is
	// only that a time was read.
	if got.Before(before) || got.After(time.Now().Add(time.Minute)) {
		t.Errorf("probe time %v is not near now", got)
	}
	if left := names(t, dir); len(left) != 0 {
		t.Errorf("probe left %v behind", left)
	}
	if _, err := o.Probe(filepath.Join(dir, "missing"), "web"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Probe in a missing directory = %v, want ErrNotExist", err)
	}
}

func TestOSFSModTimeReadsFilesAndDirectories(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	var o osFS
	for _, p := range []string{dir, filepath.Join(dir, "f")} {
		if p != dir {
			if err := os.WriteFile(p, nil, 0o600); err != nil {
				t.Fatal(err)
			}
		}
		st, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		if got, err := o.ModTime(p); err != nil || !got.Equal(st.ModTime()) {
			t.Errorf("ModTime(%s) = %v, %v; want %v", filepath.Base(p), got, err, st.ModTime())
		}
	}
	if _, err := o.ModTime(filepath.Join(dir, "missing")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("ModTime of a missing file = %v", err)
	}
}

func TestCleanDropsThePathButKeepsTheCause(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	tests := []struct {
		name string
		err  error
		want error
	}{
		{"path error", os.Mkdir(filepath.Join(dir, "a", "b"), 0o750), fs.ErrNotExist},
		{"link error", os.Rename(filepath.Join(dir, "a"), filepath.Join(dir, "b")), fs.ErrNotExist},
		{"other", fs.ErrPermission, fs.ErrPermission},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := clean(tt.err)
			if !errors.Is(got, tt.want) || strings.Contains(got.Error(), dir) {
				t.Errorf("clean(%v) = %v", tt.err, got)
			}
		})
	}
}
