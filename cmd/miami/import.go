package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/greghaskins/csi-miami/internal/chunk"
	"github.com/greghaskins/csi-miami/internal/holder"
)

// imageClass is what an image's own format says about the class that can hold
// it.
type imageClass int

const (
	classUnknown   imageClass = iota
	classEncrypted            // a LUKS2 header: an encrypted class, header kept
	classPlain                // an ext4 superblock: a -plain class
)

func (c imageClass) String() string {
	switch c {
	case classEncrypted:
		return "encrypted (LUKS2)"
	case classPlain:
		return "plain (ext4)"
	}
	return "unknown"
}

const (
	// ext4 superblock fields, at byte 1024 of the device.
	ext4SuperOffset   = 1024
	ext4MagicOffset   = ext4SuperOffset + 56
	ext4IncompatOff   = ext4SuperOffset + 0x60
	ext4Magic         = 0xEF53
	ext4IncompatFlags = 0x40 | 0x80 | 0x200 // extents, 64bit, flex_bg: what makes ext2/3 into ext4

	// headerLen is how much of the image detectFormat needs.
	headerLen = ext4SuperOffset + 1024
)

// detectFormat classifies an image from its first headerLen bytes: a LUKS2
// header, or an ext4 superblock. LUKS1 and ext2/ext3 (no ext4 feature bit) are
// not ext4 for this purpose. It never opens the image, so it needs no key.
func detectFormat(hdr []byte) imageClass {
	if len(hdr) >= 8 && bytes.Equal(hdr[:6], []byte("LUKS\xba\xbe")) {
		if binary.BigEndian.Uint16(hdr[6:8]) == 2 {
			return classEncrypted
		}
		return classUnknown
	}
	if len(hdr) >= headerLen &&
		binary.LittleEndian.Uint16(hdr[ext4MagicOffset:]) == ext4Magic &&
		binary.LittleEndian.Uint32(hdr[ext4IncompatOff:])&ext4IncompatFlags != 0 {
		return classPlain
	}
	return classUnknown
}

var volumeNameShape = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// importFlags are the flags of `miami import`.
type importFlags struct {
	image     string
	store     string
	volume    string
	chunkSize int64
}

// runImport parses the flags of `miami import` and writes a raw image file as
// generation 0.1 of a new volume. Help goes to stdout and exits 0; a flag error
// goes to stderr and exits 2; a failure to import exits 1.
func runImport(args []string, stdout, stderr io.Writer) int {
	var out bytes.Buffer
	var f importFlags
	fl := flag.NewFlagSet("miami import", flag.ContinueOnError)
	fl.SetOutput(&out)
	fl.StringVar(&f.image, "image", "", "the raw block image file to import, an absolute path (required)")
	fl.StringVar(&f.store, "store", "", "the new volume's directory on the transport, an absolute path; its parent must exist (required)")
	fl.StringVar(&f.volume, "volume", "", "the volume name recorded in the manifest (default: the store directory's name)")
	fl.Int64Var(&f.chunkSize, "chunk-size", 0, "chunk size in bytes, a power of two (default: derived from the image size)")

	if err := fl.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			_, _ = stdout.Write(out.Bytes())
			return 0
		}
		_, _ = stderr.Write(out.Bytes())
		return 2
	}
	if fl.NArg() > 0 {
		_, _ = fmt.Fprintf(stderr, "miami import: unexpected argument %q\n", fl.Arg(0))
		return 2
	}
	if f.image == "" || f.store == "" {
		_, _ = fmt.Fprintln(stderr, "miami import: -image and -store are required")
		return 2
	}
	for _, p := range []struct{ flag, path string }{{"image", f.image}, {"store", f.store}} {
		if err := checkFlagPath(p.path); err != nil {
			_, _ = fmt.Fprintf(stderr, "miami import: -%s: %v\n", p.flag, err)
			return 2
		}
	}
	if f.volume == "" {
		f.volume = filepath.Base(f.store)
	}
	if !volumeNameShape.MatchString(f.volume) {
		_, _ = fmt.Fprintf(stderr, "miami import: -volume %q is not a valid volume name\n", f.volume)
		return 2
	}
	if f.chunkSize != 0 && !chunkSizeOK(f.chunkSize) {
		_, _ = fmt.Fprintf(stderr, "miami import: -chunk-size %d is not a power of two from %d to %d\n", f.chunkSize, chunk.MinChunkSize, chunk.MaxChunkSize)
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	r, err := importImage(ctx, f)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "miami import: %v\n", err)
		return 1
	}
	_, _ = fmt.Fprintf(stdout, "imported %s as manifest %s of %s\n", f.image, r.manifest, f.volume)
	_, _ = fmt.Fprintf(stdout, "class: %s; the image's own format decided it, no key was needed or read\n", r.class)
	_, _ = fmt.Fprintf(stdout, "image %d bytes, device %d bytes (%d zero bytes of padding to a whole number of %d byte chunks)\n", r.imageSize, r.deviceSize, r.deviceSize-r.imageSize, r.chunk)
	_, _ = fmt.Fprintf(stdout, "%d chunks: %d stored, %d all-zero and not stored; the PV capacity must be at least %d bytes and the class chunkSize %d\n", r.chunks, r.stored, r.zero, r.deviceSize, r.chunk)
	return 0
}

func chunkSizeOK(c int64) bool {
	return c >= chunk.MinChunkSize && c <= chunk.MaxChunkSize && c&(c-1) == 0
}

type importResult struct {
	manifest              string
	class                 imageClass
	imageSize, deviceSize int64
	chunk                 int64
	chunks, stored, zero  int
}

// importEpoch and importSequence name the imported generation. The first
// holder of a volume takes epoch 1 (an epoch is one above any claimed), so
// epoch 0 sorts below every generation a holder will publish and the import is
// the base the first holder resumes from; the sequence is 1, the first
// generation.
const (
	importEpoch    = 0
	importSequence = 1
)

// importImage chunks the image file into the store and publishes it. Nothing
// is created until the image's format is known and the volume is seen to be
// new; the manifest is written after every chunk and current after the
// manifest, as a publisher would.
func importImage(ctx context.Context, f importFlags) (importResult, error) {
	var r importResult
	img, err := os.Open(f.image) //nolint:gosec // f.image is a validated absolute path
	if err != nil {
		return r, err
	}
	defer func() { _ = img.Close() }()
	fi, err := img.Stat()
	if err != nil {
		return r, err
	}
	if !fi.Mode().IsRegular() {
		return r, fmt.Errorf("%s is not a regular file", f.image)
	}
	r.imageSize = fi.Size()
	hdr := make([]byte, headerLen)
	n, err := io.ReadFull(img, hdr)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return r, err
	}
	if r.class = detectFormat(hdr[:n]); r.class == classUnknown {
		return r, errors.New("the image is neither a LUKS2 image nor an ext4 filesystem; refusing to import it")
	}

	r.chunk = f.chunkSize
	if r.chunk == 0 {
		r.chunk = chunk.DefaultChunkSize(r.imageSize)
	}
	r.deviceSize = chunk.RoundUp(r.imageSize, r.chunk)
	if err := chunk.CheckChunkSize(r.deviceSize, r.chunk); err != nil {
		return r, err
	}

	if err := requireNewVolume(f.store); err != nil {
		return r, err
	}
	if err := os.Mkdir(f.store, 0o755); err != nil && !errors.Is(err, fs.ErrExist) { //nolint:gosec // shared store directory, read by restore tools
		return r, err
	}
	s, err := chunk.Open(f.store)
	if err != nil {
		return r, err
	}
	hashes, err := putImage(ctx, s, img, r.imageSize, r.chunk, int(r.deviceSize/r.chunk))
	if err != nil {
		return r, err
	}
	zero := chunk.ZeroHash(r.chunk)
	for _, h := range hashes {
		if h == zero {
			r.zero++
		} else {
			r.stored++
		}
	}
	r.chunks = len(hashes)

	m := chunk.Manifest{
		Volume: f.volume, Epoch: importEpoch, Sequence: importSequence,
		Size: r.deviceSize, Chunk: r.chunk, Time: time.Now(), Chunks: hashes,
	}
	// The volume was new when we looked; look again before the two writes
	// that make it live, so a volume that appeared meanwhile is not replaced.
	if err := requireNewVolume(f.store); err != nil {
		return r, err
	}
	if err := s.WriteManifest(ctx, m); err != nil {
		return r, err
	}
	if err := s.Publish(ctx, m.Name()); err != nil {
		return r, err
	}
	r.manifest = m.Name()
	return r, nil
}

// putImage stores the image's chunks, one chunk of memory at a time, and
// returns the hash of each of the n chunks of the device. The image is padded
// with zeros to n whole chunks; an all-zero chunk is named, never stored.
func putImage(ctx context.Context, s *chunk.Store, img *os.File, imageSize, size int64, n int) ([]chunk.Hash, error) {
	zero := chunk.ZeroHash(size)
	buf := make([]byte, size)
	hashes := make([]chunk.Hash, 0, n)
	var off int64
	for range n {
		got, err := io.ReadFull(io.NewSectionReader(img, off, min(size, max(imageSize-off, 0))), buf)
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
			return nil, err
		}
		clear(buf[got:])
		if want := min(size, max(imageSize-off, 0)); int64(got) != want {
			return nil, fmt.Errorf("image changed while it was read (%d of %d bytes at offset %d)", got, want, off)
		}
		off += size
		h := chunk.Sum(buf)
		hashes = append(hashes, h)
		if h == zero {
			continue
		}
		if err := s.Put(ctx, h, bytes.NewReader(buf), size); err != nil {
			return nil, err
		}
	}
	return hashes, nil
}

// requireNewVolume refuses a store directory that already has a current, any
// manifest, or a holder file (or an epoch claim) beside it: a live volume is
// never imported over.
func requireNewVolume(store string) error {
	if _, err := os.Lstat(filepath.Join(store, "current")); err == nil {
		return fmt.Errorf("%s already has a current manifest; refusing to import over a volume", store)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	ents, err := os.ReadDir(filepath.Join(store, "manifests"))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if len(ents) > 0 {
		return fmt.Errorf("%s already has manifests; refusing to import over a volume", store)
	}
	dir, name := filepath.Dir(store), filepath.Base(store)
	if _, err := os.Lstat(holder.Path(dir, name)); err == nil {
		return fmt.Errorf("%s already has a holder file; refusing to import over a volume", store)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	sibs, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range sibs {
		if strings.HasPrefix(e.Name(), name+".holder.epoch-") {
			return fmt.Errorf("%s already has a holder epoch claim; refusing to import over a volume", store)
		}
	}
	return nil
}
