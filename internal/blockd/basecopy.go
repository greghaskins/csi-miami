package blockd

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/greghaskins/csi-miami/internal/cache"
	"github.com/greghaskins/csi-miami/internal/chunk"
)

// The cache directory keeps a copy of the base manifest its index names, so a
// server resumed while the store is unreachable still knows the hash of every
// chunk (csi-miami#102): a chunk the base names as zero, or does not name,
// reads as zeros and takes a partial write with no fetch. The copy answers
// only that; it never stands in for the store's current. It is named after the
// manifest, so a copy is used only for the base the index names, and it is
// made durable before the index names that base: a stale copy taken as the
// base would zero-fill real data on a partial write. It lives in the cache
// directory and goes with it.
const baseCopyPrefix = "base-"

func baseCopyPath(dir, name string) string { return filepath.Join(dir, baseCopyPrefix+name) }

// writeBaseCopy writes m's copy into dir durably: a temp file, fsynced,
// renamed over the name, and the directory fsynced.
func writeBaseCopy(dir string, m chunk.Manifest) error {
	path := baseCopyPath(dir, m.Name())
	f, err := os.CreateTemp(dir, baseCopyPrefix+"tmp-")
	if err != nil {
		return fmt.Errorf("blockd: base copy: %w", err)
	}
	tmp := f.Name()
	ok := false
	defer func() {
		if !ok {
			_ = f.Close()
			_ = os.Remove(tmp)
		}
	}()
	if _, err := f.Write(m.Marshal()); err != nil {
		return fmt.Errorf("blockd: base copy: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("blockd: base copy: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("blockd: base copy: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("blockd: base copy: %w", err)
	}
	ok = true
	d, err := os.Open(dir) //nolint:gosec // the server's own cache directory
	if err != nil {
		return fmt.Errorf("blockd: base copy: %w", err)
	}
	defer func() { _ = d.Close() }()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("blockd: base copy: %w", err)
	}
	return nil
}

// readBaseCopy returns the copy of the manifest called name in dir, if there is
// one that can be used: it parses, its name is name, and its volume, chunk size
// and size fit the cache (a size never shrinks, so the base is at most the
// cache's). Anything else is no copy, and the server waits for the store.
func readBaseCopy(dir, name, volume string, chunkSize, size int64) (chunk.Manifest, bool) {
	if name == "" {
		return chunk.Manifest{}, false
	}
	raw, err := os.ReadFile(baseCopyPath(dir, name)) //nolint:gosec // a file in the server's own cache directory
	if err != nil {
		return chunk.Manifest{}, false
	}
	m, err := chunk.ParseManifest(bytes.NewReader(raw))
	if err != nil || m.Name() != name || m.Volume != volume || m.Chunk != chunkSize || m.Size > size {
		return chunk.Manifest{}, false
	}
	return m, true
}

// pruneBaseCopies removes every base copy (and leftover temp file) in dir but
// the ones named keep. Errors are ignored: a stray copy is never used for a
// base the index does not name.
func pruneBaseCopies(dir string, keep ...string) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range ents {
		n := e.Name()
		if !strings.HasPrefix(n, baseCopyPrefix) {
			continue
		}
		kept := false
		for _, k := range keep {
			if k != "" && n == baseCopyPrefix+k {
				kept = true
			}
		}
		if !kept {
			_ = os.Remove(filepath.Join(dir, n))
		}
	}
}

// errBaseCopyMismatch is the store's copy of the base disagreeing with the
// local one a resumed server served from: the hashes it answered may have been
// wrong, so it publishes nothing and waits for a person, as for an unreadable
// cache index.
var errBaseCopyMismatch = fmt.Errorf("%w: the store's base manifest differs from the cache's copy of it; publishing is stopped until a person looks", cache.ErrCorrupt)
