package blockd

import (
	"bytes"
	"sync"
	"testing"

	"github.com/greghaskins/csi-miami/internal/chunk"
)

// A read whose fetch of a chunk outlives a write, a publish and an eviction of
// that chunk must not install what it fetched: the base has moved on, and the
// cache would serve the old bytes as clean.
func TestAReadFetchThatOutlivesAPublishDoesNotInstallTheOldChunk(t *testing.T) {
	cfg, _ := testConfig(t)
	cfg.CacheBound = testChunk // one chunk: reading chunk 1 evicts clean chunk 0
	store := openStore(t, cfg)
	h0 := putChunk(t, store, pattern(1, testChunk))
	h1 := putChunk(t, store, pattern(2, testChunk))
	writeCurrent(t, store, 1, 1, []chunk.Hash{h0, h1})
	s := newTestServer(t, cfg)

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	fetchHook = func(i uint32) {
		if i != 0 {
			return
		}
		first := false
		once.Do(func() { first = true })
		if first {
			close(entered)
			<-release
		}
	}
	t.Cleanup(func() { fetchHook = nil })

	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		_, _ = s.back.ReadAt(make([]byte, 4096), 0)
	}()
	waitEntered(t, entered) // the read holds the hash of the old chunk 0

	fresh := pattern(9, testChunk)
	prepareBoundaryWith(t, s, fresh)
	if _, err := s.publish(t.Context(), "request"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	readDevice(t, s, testChunk, 4096) // chunk 1 takes the cache's one slot: chunk 0 is evicted
	if hasChunk(s, 0) {
		t.Fatal("chunk 0 was not evicted")
	}
	close(release)
	<-readDone

	if got := readDevice(t, s, 0, testChunk); !bytes.Equal(got, fresh) {
		t.Fatal("chunk 0 reads the old content after the stalled read finished")
	}
}

// prepareBoundaryWith writes data as chunk 0 and sets the boundary.
func prepareBoundaryWith(t *testing.T, s *server, data []byte) {
	t.Helper()
	writeDevice(t, s, 0, data, false)
	if err := s.boundary(); err != nil {
		t.Fatalf("boundary: %v", err)
	}
}
