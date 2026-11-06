package blockd

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/chunk"
)

// The store is on a shared export, so what is in it is not trusted.

// A valid manifest of another volume, current in this volume's directory, must
// not be served as this volume's device.
func TestStartRefusesAManifestOfAnotherVolume(t *testing.T) {
	cfg, _ := testConfig(t)
	store := openStore(t, cfg)
	h := putChunk(t, store, pattern(3, testChunk))
	m := chunk.Manifest{
		Volume: "someone-elses-volume", Epoch: 1, Sequence: 1, Size: testSize, Chunk: testChunk,
		Time: time.Now().UTC(), Chunks: []chunk.Hash{h, chunk.ZeroHash(testChunk)},
	}
	ctx := context.Background()
	if err := store.WriteManifest(ctx, m); err != nil {
		t.Fatal(err)
	}
	if err := store.Publish(ctx, m.Name()); err != nil {
		t.Fatal(err)
	}
	if _, err := newServer(ctx, cfg, testLog()); err == nil || !strings.Contains(err.Error(), "another volume") {
		t.Fatalf("newServer over another volume's manifest = %v, want a refusal", err)
	}
}

// The base a cache descends from is checked too, not only current.
func TestLoadBaseRefusesAManifestOfAnotherVolume(t *testing.T) {
	cfg, _ := testConfig(t)
	store := openStore(t, cfg)
	m := chunk.Manifest{
		Volume: "someone-elses-volume", Epoch: 1, Sequence: 1, Size: testSize, Chunk: testChunk,
		Time: time.Now().UTC(), Chunks: []chunk.Hash{chunk.ZeroHash(testChunk), chunk.ZeroHash(testChunk)},
	}
	if err := store.WriteManifest(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadBase(context.Background(), store, testVol, m.Name()); err == nil {
		t.Fatal("loadBase accepted another volume's manifest")
	}
	m.Volume = testVol
	m.Sequence = 2
	if err := store.WriteManifest(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadBase(context.Background(), store, testVol, m.Name()); err != nil {
		t.Fatalf("loadBase of our own manifest: %v", err)
	}
}

// A manifest planted at the largest sequence must not wrap the next one.
func TestNextSequenceRefusesAtTheMaximum(t *testing.T) {
	cfg, _ := testConfig(t)
	s := newTestServer(t, cfg)
	name := "1.18446744073709551615"
	if err := os.WriteFile(filepath.Join(cfg.StoreDir, "manifests", name), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if seq, err := s.nextSequence(context.Background()); err == nil {
		t.Fatalf("nextSequence = %d, want a refusal at the maximum", seq)
	}
}

// blockd resolves the store path itself: a symlink in the middle of it (the
// class's subdirectory replaced by a link to a node directory) is refused.
func TestNewServerRefusesAStorePathThroughASymlink(t *testing.T) {
	cfg, _ := testConfig(t)
	link := filepath.Join(t.TempDir(), "ns")
	if err := os.Symlink(filepath.Dir(cfg.StoreDir), link); err != nil {
		t.Fatal(err)
	}
	cfg.StoreDir = filepath.Join(link, filepath.Base(cfg.StoreDir))
	if _, err := newServer(context.Background(), cfg, testLog()); err == nil {
		t.Fatal("newServer opened a store through a symlink in its path")
	}
}
