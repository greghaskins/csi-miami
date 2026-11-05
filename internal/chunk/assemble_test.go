package chunk

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// putData stores a mib-sized chunk filled with b and returns its hash.
func putData(t *testing.T, s *Store, b byte) Hash {
	t.Helper()
	data := bytes.Repeat([]byte{b}, int(mib))
	h := Sum(data)
	if err := s.Put(context.Background(), h, bytes.NewReader(data), mib); err != nil {
		t.Fatal(err)
	}
	return h
}

func TestAssembleWritesChunksAndSynthesisesZeros(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	a, b := putData(t, s, 'a'), putData(t, s, 'b')
	// The zero chunk is deliberately not stored: a manifest may name it
	// without any file behind it.
	m := writeM(t, s, 0, 1, a, ZeroHash(mib), b, ZeroHash(mib))

	if err := s.Verify(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "v.img")
	f, err := os.OpenFile(out, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600) //nolint:gosec // test fixture
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if err := s.Assemble(context.Background(), m, f); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(out) //nolint:gosec // test fixture
	if err != nil {
		t.Fatal(err)
	}
	var want []byte
	want = append(want, bytes.Repeat([]byte{'a'}, int(mib))...)
	want = append(want, make([]byte, mib)...)
	want = append(want, bytes.Repeat([]byte{'b'}, int(mib))...)
	want = append(want, make([]byte, mib)...)
	if !bytes.Equal(got, want) {
		t.Fatalf("assembled bytes differ (len %d, want %d)", len(got), len(want))
	}
}

func TestVerifyFlagsMissingAndCorruptChunks(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	a, b := putData(t, s, 'a'), putData(t, s, 'b')
	m := writeM(t, s, 0, 1, a, b)

	if err := os.WriteFile(s.ChunkPath(b), []byte("rot"), 0o644); err != nil { //nolint:gosec // test fixture
		t.Fatal(err)
	}
	if err := s.Verify(context.Background(), m); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt chunk: err = %v, want ErrCorrupt", err)
	}
	if err := os.Remove(s.ChunkPath(b)); err != nil {
		t.Fatal(err)
	}
	if err := s.Verify(context.Background(), m); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing chunk: err = %v, want ErrNotExist", err)
	}
}

func TestOpenReadOnlyNeverCreates(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	if _, err := OpenReadOnly(root); err == nil {
		t.Fatal("OpenReadOnly accepted a directory with no chunks/ or manifests/")
	}
	ents, _ := os.ReadDir(root)
	if len(ents) != 0 {
		t.Fatalf("OpenReadOnly created %d entries", len(ents))
	}
	if _, err := OpenReadOnly(filepath.Join(root, "absent")); err == nil {
		t.Fatal("OpenReadOnly accepted a missing root")
	}
	if _, err := Open(root); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenReadOnly(root); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyRefusesAChunkOfTheWrongLength(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	short := []byte("short")
	h := Sum(short)
	if err := s.Put(context.Background(), h, bytes.NewReader(short), int64(len(short))); err != nil {
		t.Fatal(err)
	}
	m := writeM(t, s, 0, 1, h)
	if err := s.Verify(context.Background(), m); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
}
