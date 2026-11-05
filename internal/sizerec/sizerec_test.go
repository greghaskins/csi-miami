package sizerec

import (
	"errors"
	"os"
	"path/filepath"
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
