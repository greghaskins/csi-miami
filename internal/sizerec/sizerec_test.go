package sizerec

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const name = "pvc-0b9e1a3c-5d7f-4e21-8a6b-1c2d3e4f5a6b"

func TestReadOfNoRecordIsNone(t *testing.T) {
	t.Parallel()
	size, ok, err := Read(t.TempDir(), name)
	if err != nil || ok || size != 0 {
		t.Fatalf("Read = %d, %v, %v; want 0, false, nil", size, ok, err)
	}
}

func TestGrowWritesAndRaises(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	for _, step := range []struct{ ask, want int64 }{{8 << 20, 8 << 20}, {16 << 20, 16 << 20}} {
		got, err := Grow(dir, name, step.ask)
		if err != nil || got != step.want {
			t.Fatalf("Grow(%d) = %d, %v; want %d", step.ask, got, err, step.want)
		}
		if size, ok, err := Read(dir, name); err != nil || !ok || size != step.want {
			t.Fatalf("Read = %d, %v, %v; want %d", size, ok, err, step.want)
		}
	}
}

func TestGrowNeverLowersTheRecord(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if _, err := Grow(dir, name, 16<<20); err != nil {
		t.Fatal(err)
	}
	got, err := Grow(dir, name, 8<<20)
	if err != nil || got != 16<<20 {
		t.Fatalf("Grow(smaller) = %d, %v; want the record's 16 MiB", got, err)
	}
	if size, _, _ := Read(dir, name); size != 16<<20 {
		t.Fatalf("record = %d after a smaller Grow, want 16 MiB", size)
	}
}

func TestGrowLeavesNoTempFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if _, err := Grow(dir, name, 4<<20); err != nil {
		t.Fatal(err)
	}
	ents, _ := os.ReadDir(dir)
	if len(ents) != 1 || ents[0].Name() != filepath.Base(Path(dir, name)) {
		t.Fatalf("directory holds %v, want only the record", ents)
	}
}

func TestReadRefusesAMalformedRecord(t *testing.T) {
	t.Parallel()
	for _, body := range []string{"", "abc\n", "-4\n", "0\n", "08\n", "4 \n"} {
		dir := t.TempDir()
		if err := os.WriteFile(Path(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := Read(dir, name); !errors.Is(err, ErrMalformed) {
			t.Errorf("Read(%q) error = %v, want ErrMalformed", body, err)
		}
		// A record that cannot be read is never replaced by a smaller one.
		if _, err := Grow(dir, name, 4<<20); !errors.Is(err, ErrMalformed) {
			t.Errorf("Grow over %q error = %v, want ErrMalformed", body, err)
		}
	}
}

func TestGrowRefusesANonPositiveSize(t *testing.T) {
	t.Parallel()
	if _, err := Grow(t.TempDir(), name, 0); err == nil {
		t.Fatal("Grow(0) succeeded")
	}
}

// The record is on a shared export: a link or an oversized file there must not
// be followed, and its content must not reach an error.
func TestReadDoesNotFollowASymlinkOrEchoContent(t *testing.T) {
	t.Parallel()
	const secret = "TOPSECRET-token-abc"
	dir := t.TempDir()
	target := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(target, []byte(secret+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, Path(dir, name)); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := Read(dir, name); err == nil || ok {
		t.Fatalf("Read followed a symlink: %v, %v", ok, err)
	} else if strings.Contains(err.Error(), secret) {
		t.Fatalf("error carries content: %v", err)
	}
	// Grow replaces the link, leaving the target alone... after refusing it:
	// a record that cannot be read is never overwritten.
	if _, err := Grow(dir, name, 4<<20); err == nil {
		t.Fatal("Grow wrote over an unreadable record")
	}
	if b, _ := os.ReadFile(target); string(b) != secret+"\n" { //nolint:gosec // a test path
		t.Fatalf("target changed: %q", b)
	}
}

func TestReadIsSizeCappedAndRegularOnly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := os.WriteFile(Path(dir, name), []byte(strings.Repeat("1", 1<<20)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Read(dir, name); !errors.Is(err, ErrMalformed) {
		t.Fatalf("oversize Read = %v, want ErrMalformed", err)
	}
	dir = t.TempDir()
	if err := os.Mkdir(Path(dir, name), 0o750); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Read(dir, name); !errors.Is(err, ErrMalformed) {
		t.Fatalf("directory Read = %v, want ErrMalformed", err)
	}
}

func TestGrowRefusesASymlinkedDirectory(t *testing.T) {
	t.Parallel()
	target := t.TempDir()
	l := filepath.Join(t.TempDir(), "export")
	if err := os.Symlink(target, l); err != nil {
		t.Fatal(err)
	}
	if _, err := Grow(l, name, 4<<20); err == nil {
		t.Fatal("Grow wrote through a symlinked directory")
	}
	if ents, _ := os.ReadDir(target); len(ents) != 0 {
		t.Fatalf("files created behind the link: %v", ents)
	}
	if _, _, err := Read(l, name); err == nil {
		t.Fatal("Read worked through a symlinked directory")
	}
}
