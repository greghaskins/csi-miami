package blockd

import (
	"os"
	"testing"

	"github.com/greghaskins/csi-miami/internal/chunk"
)

// A fetch from the store is counted with its duration in the status, and a
// fetch that fails is counted as an error; a read the cache serves counts as
// neither.
func TestStatusCountsChunkFetches(t *testing.T) {
	cfg, _ := testConfig(t)
	store := openStore(t, cfg)
	good := putChunk(t, store, pattern(1, testChunk))
	bad := putChunk(t, store, pattern(2, testChunk))
	if err := os.WriteFile(store.ChunkPath(bad), pattern(99, testChunk), 0o600); err != nil {
		t.Fatal(err)
	}
	writeCurrent(t, store, 1, 1, []chunk.Hash{good, bad})
	s := newTestServer(t, cfg)

	if f := s.status().Fetch; f.Count != 0 || f.Errors != 0 {
		t.Fatalf("fetch stats before any read = %+v", f)
	}
	readDevice(t, s, 0, testChunk) // a miss: one fetch
	readDevice(t, s, 0, testChunk) // a hit: none
	if _, err := s.back.ReadAt(make([]byte, testChunk), testChunk); err == nil {
		t.Fatal("the corrupt chunk read succeeded")
	}
	f := s.status().Fetch
	if f.Count != 2 || f.Errors != 1 {
		t.Fatalf("fetch stats = %+v, want 2 fetches and 1 error", f)
	}
	if len(f.Buckets) != len(FetchBuckets) {
		t.Fatalf("%d buckets, want %d", len(f.Buckets), len(FetchBuckets))
	}
	// Buckets are cumulative: the last bound has seen every fetch that was quick.
	for i := 1; i < len(f.Buckets); i++ {
		if f.Buckets[i] < f.Buckets[i-1] {
			t.Fatalf("buckets are not cumulative: %v", f.Buckets)
		}
	}
	if f.Sum <= 0 {
		t.Errorf("sum = %v, want positive", f.Sum)
	}
}
