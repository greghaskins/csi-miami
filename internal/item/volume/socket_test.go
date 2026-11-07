package volume

import (
	"fmt"
	"path/filepath"
	"testing"
)

// Two names collide only if their first 64 bits of SHA-256 do. Over the few
// hundred volumes one node ever holds the chance is about n^2 / 2^65, below
// 1e-14, so the table checks the realistic shape (pvc-<uuid>) and not a bound.
func TestSocketPathIsShortAndDistinct(t *testing.T) {
	t.Parallel()
	names := []string{
		"pvc-00000000-0000-0000-0000-000000000000",
		"pvc-00000000-0000-0000-0000-000000000001",
		"pvc-10000000-0000-0000-0000-000000000000",
		"pvc-6f1c2d3e-4a5b-4c6d-8e7f-0a1b2c3d4e5f",
		"pvc-6f1c2d3e-4a5b-4c6d-8e7f-0a1b2c3d4e50",
		"pvc-a1b2c3d4-e5f6-4789-9abc-def012345678",
	}
	for i := range 500 {
		names = append(names, fmt.Sprintf("pvc-%08x-1111-4222-8333-%012x", i*7919, i))
	}
	seen := map[string]string{}
	for _, n := range names {
		p := SocketPath("/c", n)
		if other, dup := seen[p]; dup {
			t.Fatalf("%q and %q share the socket %s", n, other, p)
		}
		seen[p] = n
		if filepath.Dir(p) != "/c" || len(filepath.Base(p)) != 18 || filepath.Ext(p) != ".s" {
			t.Errorf("SocketPath(%q) = %q", n, p)
		}
		if p != SocketPath("/c", n) {
			t.Errorf("SocketPath(%q) is not stable", n)
		}
	}
}

func TestSocketPathLen(t *testing.T) {
	t.Parallel()
	if got, want := SocketPathLen("/cache"), len(SocketPath("/cache", "pvc-00000000-0000-0000-0000-000000000000")); got != want {
		t.Errorf("SocketPathLen = %d, want %d", got, want)
	}
}
