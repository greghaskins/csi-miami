package cache

import (
	"bytes"
	"errors"
	"os"
	"testing"
)

// resumedWithClean leaves a cache whose chunk 3 is a clean copy (inserted, so
// in no index) holding fill(7), and reopens it: chunk 3 is then unverified.
func resumedWithClean(t *testing.T, bound int64) (*Cache, string) {
	t.Helper()
	c, dir := tmpCache(t, bound)
	if err := c.Insert(3, fill(7)); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c2, err := Open(dir, bound)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c2.Close() })
	return c2, dir
}

// matches says whether the file at path holds want, as a hash check would.
func matches(want []byte) func(string) (bool, error) {
	return func(path string) (bool, error) {
		got, err := os.ReadFile(path) //nolint:gosec // a test's own temp file
		return bytes.Equal(got, want), err
	}
}

func TestResumeKeepsAnUnindexedFileButNeverServesIt(t *testing.T) {
	c, _ := resumedWithClean(t, 16*testChunk)
	if !c.Unverified(3) || c.Has(3) {
		t.Fatalf("unverified %v, has %v; want kept and not served", c.Unverified(3), c.Has(3))
	}
	if err := c.Read(3, AlignedBuffer(testChunk)); !errors.Is(err, ErrMiss) {
		t.Fatalf("Read of an unverified chunk = %v, want ErrMiss", err)
	}
	if got := c.ReadAt(3, 0, make([]byte, 4096)); !errors.Is(got, ErrMiss) {
		t.Fatalf("ReadAt of an unverified chunk = %v, want ErrMiss", got)
	}
	if err := c.WriteAt(3, 0, make([]byte, 4096), nil, false); !errors.Is(err, ErrNotResident) {
		t.Fatalf("a partial write with no base = %v, want ErrNotResident", err)
	}
	if c.Used() != testChunk {
		t.Errorf("Used = %d, want the unverified file counted (%d)", c.Used(), testChunk)
	}
}

func TestAdoptServesAMatchingFile(t *testing.T) {
	c, _ := resumedWithClean(t, 16*testChunk)
	ok, err := c.Adopt(3, matches(fill(7)))
	if err != nil || !ok {
		t.Fatalf("Adopt = %v, %v", ok, err)
	}
	if !bytes.Equal(mustRead(t, c, 3), fill(7)) || c.Unverified(3) {
		t.Fatal("an adopted chunk does not read back as itself")
	}
}

// A write that a crash beat to the index left bytes that are not the base's:
// they must never be served, and the file goes.
func TestAdoptDeletesAMismatchingFile(t *testing.T) {
	c, dir := resumedWithClean(t, 16*testChunk)
	ok, err := c.Adopt(3, matches(fill(9)))
	if err != nil || ok {
		t.Fatalf("Adopt of a mismatch = %v, %v", ok, err)
	}
	if c.Has(3) || c.Unverified(3) {
		t.Fatal("a mismatching file is still in the cache")
	}
	if _, err := os.Stat(c.cpath(3)); !os.IsNotExist(err) {
		t.Fatalf("its file in %s is still there: %v", dir, err)
	}
}

func TestWritesInsertsAndTrimsClearUnverified(t *testing.T) {
	for _, tc := range []struct {
		name string
		op   func(c *Cache) error
	}{
		{"full write", func(c *Cache) error { return c.Write(3, fill(1)) }},
		{"insert", func(c *Cache) error { return c.Insert(3, fill(1)) }},
		{"trim", func(c *Cache) error { return c.MarkZero(3) }},
		{"partial write with a base", func(c *Cache) error { return c.WriteAt(3, 0, make([]byte, 4096), fill(1), false) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := resumedWithClean(t, 16*testChunk)
			if err := tc.op(c); err != nil {
				t.Fatal(err)
			}
			if c.Unverified(3) {
				t.Fatal("still unverified")
			}
			// A later Adopt finds nothing to adopt and changes nothing.
			if ok, _ := c.Adopt(3, matches(fill(7))); ok && tc.name == "trim" {
				t.Fatal("Adopt resurrected a trimmed chunk")
			}
		})
	}
}

func TestEvictionTakesUnverifiedFilesFirst(t *testing.T) {
	c, _ := resumedWithClean(t, 2*testChunk)
	if err := c.Insert(4, fill(4)); err != nil {
		t.Fatal(err)
	}
	if err := c.Insert(5, fill(5)); err != nil { // the bound is two chunks: one must go
		t.Fatal(err)
	}
	if c.Unverified(3) || !c.Has(4) || !c.Has(5) {
		t.Fatalf("unverified %v, has 4 %v, has 5 %v; want the unverified file evicted first", c.Unverified(3), c.Has(4), c.Has(5))
	}
}

func TestDropUnverified(t *testing.T) {
	c, _ := resumedWithClean(t, 16*testChunk)
	c.DropUnverified()
	if c.Unverified(3) || c.Used() != 0 {
		t.Fatal("DropUnverified left something")
	}
	if ok, _ := c.Adopt(3, matches(fill(7))); ok {
		t.Fatal("Adopt after DropUnverified adopted")
	}
}
