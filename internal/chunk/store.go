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

	"github.com/greghaskins/csi-miami/internal/safefs"
)

var (
	// ErrCorrupt means a chunk's bytes do not hash to its name.
	ErrCorrupt = errors.New("chunk: content does not match its hash")
	// ErrNoCurrent means there is no manifest to read: current is absent, or
	// the caller passed the empty name Current returns when it is.
	ErrNoCurrent = errors.New("chunk: no current manifest")
)

// maxCurrentBytes bounds what Store reads of `current`: a manifest name is two
// 20-digit numbers, a dot and a newline.
const maxCurrentBytes = 128

// Store is the directory of one volume on its transport. The shared export is
// not trusted for integrity: the store holds one descriptor on its root, opened
// without following a symlink, and every access is relative to it and follows
// no symlink (package safefs), so nothing on the export can point an access at
// a file outside the store.
type Store struct {
	root string
	dir  *safefs.Dir
}

// Open opens the store at root, which must exist, creating chunks/ and
// manifests/ when absent. A symlink at root, chunks or manifests is refused.
func Open(root string) (*Store, error) {
	dir, err := safefs.OpenRoot(root)
	if err != nil {
		return nil, fmt.Errorf("chunk: open store: %w", err)
	}
	for _, d := range []string{"chunks", "manifests"} {
		sub, err := dir.EnsureSub(d, 0o755)
		if err != nil {
			_ = dir.Close()
			return nil, fmt.Errorf("chunk: open store: %w", err)
		}
		_ = sub.Close()
	}
	return &Store{root: root, dir: dir}, nil
}

// Close releases the store's descriptor.
func (s *Store) Close() error { return s.dir.Close() }

// CurrentAt is Store.Current for the store directory root, opened for just
// that read: it creates nothing, and a root that does not exist is an error.
func CurrentAt(ctx context.Context, root string) (name string, ok bool, err error) {
	dir, err := safefs.OpenRoot(root)
	if err != nil {
		return "", false, err
	}
	defer func() { _ = dir.Close() }()
	return (&Store{root: root, dir: dir}).Current(ctx)
}

// ReadManifestAt is Store.ReadManifest for the store directory root, opened for
// just that read: it creates nothing and does not require chunks/.
func ReadManifestAt(ctx context.Context, root, name string) (Manifest, error) {
	dir, err := safefs.OpenRoot(root)
	if err != nil {
		return Manifest{}, err
	}
	defer func() { _ = dir.Close() }()
	return (&Store{root: root, dir: dir}).ReadManifest(ctx, name)
}

// ChunkPath is chunks/<aa>/<hash> under the root.
func (s *Store) ChunkPath(h Hash) string {
	return filepath.Join(s.root, chunkRel(h))
}

func chunkRel(h Hash) string {
	hs := h.String()
	return "chunks/" + hs[:2] + "/" + hs
}

func (s *Store) manifestPath(name string) string { return filepath.Join(s.root, "manifests", name) }

func (s *Store) currentPath() string { return filepath.Join(s.root, "current") }

func randSuffix() string {
	var b [6]byte
	_, _ = rand.Read(b[:]) // crypto/rand does not fail on supported platforms
	return hex.EncodeToString(b[:])
}

// Has reports whether the chunk is stored. A name that is not a regular file
// (a symlink, say) is not a chunk.
func (s *Store) Has(h Hash) (bool, error) {
	fi, err := s.dir.Stat(chunkRel(h))
	switch {
	case err == nil:
		return fi.Mode().IsRegular(), nil
	case errors.Is(err, fs.ErrNotExist), safefs.IsSymlinkError(err):
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
// fsyncs and closes it, and returns its name; on any failure the file is
// removed. The caller renames or links it into place.
func writeTemp(dir *safefs.Dir, prefix string, fill func(f *os.File) error) (string, error) {
	tmp := prefix + randSuffix()
	f, err := dir.Create(tmp, 0o644)
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
		_ = dir.Unlink(tmp)
		return "", err
	}
	return tmp, nil
}

// fanDir opens chunks/<aa> for the chunk, creating it when create is set.
func (s *Store) fanDir(h Hash, create bool) (*safefs.Dir, error) {
	chunks, err := s.dir.Sub("chunks")
	if err != nil {
		return nil, err
	}
	defer func() { _ = chunks.Close() }()
	fan := h.String()[:2]
	if create {
		return chunks.EnsureSub(fan, 0o755)
	}
	return chunks.Sub(fan)
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
	dir, err := s.fanDir(h, true)
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
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
		_ = dir.Unlink(tmp)
		return fmt.Errorf("%w: put %s", ErrCorrupt, h)
	}
	if err := dir.Rename(tmp, dir, h.String()); err != nil {
		_ = dir.Unlink(tmp)
		return err
	}
	return dir.Sync()
}

// Get writes the chunk to w and verifies it against its name. The bytes reach
// w before the verdict, so a caller must discard them on an error. The stored
// file must be a regular file of at most MaxChunkSize bytes; GetExact is the
// form that knows the size.
func (s *Store) Get(ctx context.Context, h Hash, w io.Writer) error {
	return s.get(ctx, h, -1, w)
}

// GetExact is Get for a chunk that must be exactly size bytes: a stored file of
// any other length is ErrCorrupt, before a byte of it is read.
func (s *Store) GetExact(ctx context.Context, h Hash, size int64, w io.Writer) error {
	return s.get(ctx, h, size, w)
}

func (s *Store) get(ctx context.Context, h Hash, size int64, w io.Writer) error {
	limit := int64(MaxChunkSize)
	if size >= 0 {
		limit = size
	}
	f, n, err := s.dir.OpenRegular(chunkRel(h), limit)
	if err != nil {
		if errors.Is(err, safefs.ErrTooLarge) || errors.Is(err, safefs.ErrNotRegular) {
			return fmt.Errorf("%w: get %s: %w", ErrCorrupt, h, err)
		}
		return err
	}
	defer func() { _ = f.Close() }()
	if size >= 0 && n != size {
		return fmt.Errorf("%w: %s is %d bytes, want %d", ErrCorrupt, h, n, size)
	}
	d := sha256.New()
	// Read no more than the size that was checked: the file can grow while it
	// is read, and a longer read would not hash to the name anyway.
	if _, err := io.Copy(io.MultiWriter(w, d), ctxReader{ctx, io.LimitReader(f, n)}); err != nil {
		return err
	}
	if !bytes.Equal(d.Sum(nil), h[:]) {
		return fmt.Errorf("%w: get %s", ErrCorrupt, h)
	}
	return nil
}

// Current is the name in current. It opens the file fresh on every call so
// that the client's attribute cache cannot hide a change; ok is false when
// there is no current. The file must be a small regular file; what it holds is
// never put in an error.
func (s *Store) Current(ctx context.Context) (name string, ok bool, err error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	b, err := s.dir.ReadFile("current", maxCurrentBytes)
	if errors.Is(err, fs.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	name = strings.TrimSpace(string(b))
	if _, _, err := ParseManifestName(name); err != nil {
		return "", false, bad("current holds %d bytes that are not a manifest name", len(b))
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
	f, _, err := s.dir.OpenRegular("manifests/"+name, maxManifestBytes)
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
	dir, err := s.dir.Sub("manifests")
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	tmp, err := writeTemp(dir, ".tmp-"+m.Name()+"-", func(f *os.File) error {
		_, err := f.Write(data)
		return err
	})
	if err != nil {
		return err
	}
	defer func() { _ = dir.Unlink(tmp) }()
	if err := dir.Link(tmp, dir, m.Name()); err != nil {
		return err
	}
	return dir.Sync()
}

// manifestExists requires the named manifest to be a regular file in the store.
func (s *Store) manifestExists(name string) error {
	fi, err := s.dir.Stat("manifests/" + name)
	if err != nil {
		return err
	}
	if !fi.Mode().IsRegular() {
		return fmt.Errorf("manifest %s: %w", name, safefs.ErrNotRegular)
	}
	return nil
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
	if err := s.manifestExists(name); err != nil {
		return err
	}
	tmp, err := writeTemp(s.dir, ".current-", func(f *os.File) error {
		_, err := f.WriteString(name + "\n")
		return err
	})
	if err != nil {
		return err
	}
	if err := s.dir.Rename(tmp, s.dir, "current"); err != nil {
		_ = s.dir.Unlink(tmp)
		return err
	}
	return s.dir.Sync()
}

type manifestKey struct {
	name       string
	epoch, seq uint64
}

func (s *Store) sortedManifests() ([]manifestKey, error) {
	dir, err := s.dir.Sub("manifests")
	if err != nil {
		return nil, err
	}
	defer func() { _ = dir.Close() }()
	ents, err := dir.ReadDir()
	if err != nil {
		return nil, err
	}
	var ks []manifestKey
	for _, e := range ents {
		if !e.Type().IsRegular() {
			continue // directories, symlinks and strays are not manifests
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
	dir, err := s.fanDir(h, true)
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	tmp, err := writeTemp(dir, ".tmp-"+h.String()+"-", func(f *os.File) error { return f.Truncate(chunkSize) })
	if err != nil {
		return err
	}
	if err := dir.Rename(tmp, dir, h.String()); err != nil {
		_ = dir.Unlink(tmp)
		return err
	}
	return dir.Sync()
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
	if err := s.manifestExists(name); err != nil {
		return err
	}
	tmp, err := writeTemp(s.dir, ".current-", func(f *os.File) error {
		_, err := f.WriteString(name + "\n")
		return err
	})
	if err != nil {
		return err
	}
	defer func() { _ = s.dir.Unlink(tmp) }()
	if err := s.dir.Link(tmp, s.dir, "current"); err != nil {
		return err
	}
	return s.dir.Sync()
}
