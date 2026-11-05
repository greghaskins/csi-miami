package chunk

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

var (
	// ErrCorrupt means a chunk's bytes do not hash to its name.
	ErrCorrupt = errors.New("chunk: content does not match its hash")
	// ErrNoCurrent means there is no manifest to read: current is absent, or
	// the caller passed the empty name Current returns when it is.
	ErrNoCurrent = errors.New("chunk: no current manifest")
)

// Store is the directory of one volume on its transport.
type Store struct {
	root string
}

// Open opens the store at root, which must exist, creating chunks/ and
// manifests/ when absent.
func Open(root string) (*Store, error) {
	fi, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("chunk: open store: %w", err)
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("chunk: open store: %s is not a directory", root)
	}
	for _, d := range []string{"chunks", "manifests"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil { //nolint:gosec // shared store directory, read by restore tools
			return nil, fmt.Errorf("chunk: open store: %w", err)
		}
	}
	return &Store{root: root}, nil
}

// ChunkPath is chunks/<aa>/<hash> under the root.
func (s *Store) ChunkPath(h Hash) string {
	hs := h.String()
	return filepath.Join(s.root, "chunks", hs[:2], hs)
}

func (s *Store) manifestPath(name string) string { return filepath.Join(s.root, "manifests", name) }

func (s *Store) currentPath() string { return filepath.Join(s.root, "current") }

func randSuffix() string {
	var b [6]byte
	_, _ = rand.Read(b[:]) // crypto/rand does not fail on supported platforms
	return hex.EncodeToString(b[:])
}

// Has reports whether the chunk is stored.
func (s *Store) Has(h Hash) (bool, error) {
	fi, err := os.Stat(s.ChunkPath(h))
	switch {
	case err == nil:
		return fi.Mode().IsRegular(), nil
	case errors.Is(err, fs.ErrNotExist):
		return false, nil
	default:
		return false, err
	}
}

type ctxReader struct {
	ctx context.Context //nolint:containedctx // a reader wrapper lives for one call
	r   io.Reader
}

func (c ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

// writeTemp creates dir/<prefix><rand> exclusively, lets fill write it,
// fsyncs and closes it, and returns its path; on any failure the file is
// removed. The caller renames or links it into place.
func writeTemp(dir, prefix string, fill func(f *os.File) error) (string, error) {
	tmp := filepath.Join(dir, prefix+randSuffix())
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644) //nolint:gosec // shared store file, read by restore tools
	if err != nil {
		return "", err
	}
	err = fill(f)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return "", err
	}
	return tmp, nil
}

// syncDir fsyncs a directory so a rename or link into it is durable.
func syncDir(dir string) error {
	d, err := os.Open(dir) //nolint:gosec // dir is built from the store root
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}

// Put stores the n bytes of r under h, verifying the hash while copying. It
// is a no-op when the chunk is present (a chunk is never rewritten) and
// returns ErrCorrupt, storing nothing, when the bytes do not hash to h.
func (s *Store) Put(ctx context.Context, h Hash, r io.Reader, n int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if ok, err := s.Has(h); err != nil || ok {
		return err
	}
	final := s.ChunkPath(h)
	dir := filepath.Dir(final)
	if err := os.MkdirAll(dir, 0o755); err != nil { //nolint:gosec // shared store directory, read by restore tools
		return err
	}
	d := sha256.New()
	tmp, err := writeTemp(dir, ".tmp-"+h.String()+"-", func(f *os.File) error {
		got, err := io.Copy(io.MultiWriter(f, d), io.LimitReader(ctxReader{ctx, r}, n))
		if err != nil {
			return err
		}
		if got != n {
			return fmt.Errorf("chunk: put %s: %w (%d of %d bytes)", h, io.ErrUnexpectedEOF, got, n)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !bytes.Equal(d.Sum(nil), h[:]) {
		_ = os.Remove(tmp)
		return fmt.Errorf("%w: put %s", ErrCorrupt, h)
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return syncDir(dir)
}

// Get writes the chunk to w and verifies it against its name. The bytes reach
// w before the verdict, so a caller must discard them on an error.
func (s *Store) Get(ctx context.Context, h Hash, w io.Writer) error {
	f, err := os.Open(s.ChunkPath(h))
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	d := sha256.New()
	if _, err := io.Copy(io.MultiWriter(w, d), ctxReader{ctx, f}); err != nil {
		return err
	}
	if !bytes.Equal(d.Sum(nil), h[:]) {
		return fmt.Errorf("%w: get %s", ErrCorrupt, h)
	}
	return nil
}

// Current is the name in current. It opens the file fresh on every call so
// that the client's attribute cache cannot hide a change; ok is false when
// there is no current.
func (s *Store) Current(ctx context.Context) (name string, ok bool, err error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	b, err := os.ReadFile(s.currentPath())
	if errors.Is(err, fs.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	name = strings.TrimSpace(string(b))
	if _, _, err := ParseManifestName(name); err != nil {
		return "", false, err
	}
	return name, true, nil
}

// ReadManifest reads and validates the named manifest; the empty name (what
// Current returns without one) is ErrNoCurrent.
func (s *Store) ReadManifest(ctx context.Context, name string) (Manifest, error) {
	if err := ctx.Err(); err != nil {
		return Manifest{}, err
	}
	if name == "" {
		return Manifest{}, ErrNoCurrent
	}
	epoch, seq, err := ParseManifestName(name)
	if err != nil {
		return Manifest{}, err
	}
	f, err := os.Open(s.manifestPath(name))
	if err != nil {
		return Manifest{}, err
	}
	defer func() { _ = f.Close() }()
	m, err := ParseManifest(f)
	if err != nil {
		return Manifest{}, fmt.Errorf("manifest %s: %w", name, err)
	}
	if m.Epoch != epoch || m.Sequence != seq {
		return Manifest{}, bad("manifest %s declares %s", name, m.Name())
	}
	return m, nil
}

// WriteManifest stores m under its name, refusing to replace an existing
// manifest (fs.ErrExist). The content goes to a temporary file that is
// hard-linked to the final name, since link fails on an existing name where a
// rename would silently replace it, and link is atomic on NFSv3.
func (s *Store) WriteManifest(ctx context.Context, m Manifest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data := m.Marshal()
	if _, err := ParseManifest(bytes.NewReader(data)); err != nil {
		return fmt.Errorf("chunk: refusing to write: %w", err)
	}
	dir := filepath.Join(s.root, "manifests")
	tmp, err := writeTemp(dir, ".tmp-"+m.Name()+"-", func(f *os.File) error {
		_, err := f.Write(data)
		return err
	})
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp) }()
	if err := os.Link(tmp, s.manifestPath(m.Name())); err != nil {
		return err
	}
	return syncDir(dir)
}

// Publish makes the named manifest current: a temporary file renamed over
// current. The manifest must exist.
func (s *Store) Publish(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, _, err := ParseManifestName(name); err != nil {
		return err
	}
	if _, err := os.Stat(s.manifestPath(name)); err != nil {
		return err
	}
	tmp, err := writeTemp(s.root, ".current-", func(f *os.File) error {
		_, err := f.WriteString(name + "\n")
		return err
	})
	if err != nil {
		return err
	}
	if err := os.Rename(tmp, s.currentPath()); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return syncDir(s.root)
}

type manifestKey struct {
	name       string
	epoch, seq uint64
}

func (s *Store) sortedManifests() ([]manifestKey, error) {
	ents, err := os.ReadDir(filepath.Join(s.root, "manifests"))
	if err != nil {
		return nil, err
	}
	var ks []manifestKey
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		ep, sq, err := ParseManifestName(e.Name())
		if err != nil {
			continue // temporary files and strays
		}
		ks = append(ks, manifestKey{e.Name(), ep, sq})
	}
	slices.SortFunc(ks, func(a, b manifestKey) int {
		switch {
		case a.epoch != b.epoch:
			return cmpU(a.epoch, b.epoch)
		default:
			return cmpU(a.seq, b.seq)
		}
	})
	return ks, nil
}

func cmpU(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// Manifests lists the stored manifest names, oldest first: by epoch, then
// sequence, numerically.
func (s *Store) Manifests(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ks, err := s.sortedManifests()
	if err != nil {
		return nil, err
	}
	names := make([]string, len(ks))
	for i, k := range ks {
		names[i] = k.name
	}
	return names, nil
}

// EnsureZero makes sure the all-zero chunk of chunkSize is stored, so that a
// manifest naming it (as every unwritten or discarded region does) can be
// verified and assembled from files alone. Absent, it is created as a sparse
// file by truncation, since its content is zeros and so its name verifies;
// present, nothing is written (one stat). A writer calls it before each
// manifest that names the zero chunk, because Prune can drop it between
// generations.
func (s *Store) EnsureZero(ctx context.Context, chunkSize int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	h := ZeroHash(chunkSize)
	if ok, err := s.Has(h); err != nil || ok {
		return err
	}
	final := s.ChunkPath(h)
	dir := filepath.Dir(final)
	if err := os.MkdirAll(dir, 0o755); err != nil { //nolint:gosec // shared store directory, read by restore tools
		return err
	}
	tmp, err := writeTemp(dir, ".tmp-"+h.String()+"-", func(f *os.File) error { return f.Truncate(chunkSize) })
	if err != nil {
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return syncDir(dir)
}

// PublishFirst makes the named manifest current only if there is no current:
// the temporary file is hard-linked to current, which fails (fs.ErrExist) on an
// existing name where Publish's rename would replace it. It is how a volume's
// first generation is created, so that it can never roll back a volume that
// has since gone live.
func (s *Store) PublishFirst(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, _, err := ParseManifestName(name); err != nil {
		return err
	}
	if _, err := os.Stat(s.manifestPath(name)); err != nil {
		return err
	}
	tmp, err := writeTemp(s.root, ".current-", func(f *os.File) error {
		_, err := f.WriteString(name + "\n")
		return err
	})
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp) }()
	if err := os.Link(tmp, s.currentPath()); err != nil {
		return err
	}
	return syncDir(s.root)
}
