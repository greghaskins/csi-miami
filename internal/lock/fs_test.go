package lock

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOSFSWriteAtomicLeavesNoTemporaryFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	var o osFS

	if err := o.WriteAtomic(dir, "holder", []byte("h")); err != nil {
		t.Fatal(err)
	}
	if err := o.WriteAtomic(dir, "holder", []byte("h2")); err != nil {
		t.Fatal(err)
	}
	if got := names(t, dir); !equalStrings(got, []string{"holder"}) || read(t, filepath.Join(dir, "holder")) != "h2" {
		t.Errorf("directory holds %v, holder %q", got, read(t, filepath.Join(dir, "holder")))
	}

	// The rename fails (the target is a directory with something in it): the
	// temporary file does not stay.
	if err := os.MkdirAll(filepath.Join(dir, "since", "x"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := o.WriteAtomic(dir, "since", nil); err == nil {
		t.Error("WriteAtomic over a non-empty directory succeeded")
	}
	if got := names(t, dir); !equalStrings(got, []string{"holder", "since"}) {
		t.Errorf("directory holds %v after a failed write", got)
	}

	if err := o.WriteAtomic(filepath.Join(dir, "missing"), "holder", nil); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("WriteAtomic in a missing directory = %v, want ErrNotExist", err)
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
		{"path error", os.Mkdir(filepath.Join(dir, "a", "b"), 0o755), fs.ErrNotExist},
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
