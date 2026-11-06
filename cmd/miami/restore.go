package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/greghaskins/csi-miami/internal/chunk"
)

// runRestore parses the flags of `miami restore` and assembles one manifest of
// a volume's store into a new image file, after every chunk has been verified.
// Help goes to stdout and exits 0; a flag error goes to stderr and exits 2; a
// failure to restore exits 1. The store is only ever read.
func runRestore(args []string, stdout, stderr io.Writer) int {
	var out bytes.Buffer
	var store, dest, manifest string
	fs := flag.NewFlagSet("miami restore", flag.ContinueOnError)
	fs.SetOutput(&out)
	fs.StringVar(&store, "store", "", "the volume's directory on the transport, an absolute path (required)")
	fs.StringVar(&dest, "out", "", "the image file to create, an absolute path that must not exist (required)")
	fs.StringVar(&manifest, "manifest", "", "the manifest to restore, <epoch>.<sequence> under manifests/ (default: the one current names)")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			_, _ = stdout.Write(out.Bytes())
			return 0
		}
		_, _ = stderr.Write(out.Bytes())
		return 2
	}
	if fs.NArg() > 0 {
		_, _ = fmt.Fprintf(stderr, "miami restore: unexpected argument %q\n", fs.Arg(0))
		return 2
	}
	for _, p := range []struct {
		msg string
		bad bool
	}{
		{"-store is required", store == ""},
		{"-out is required", dest == ""},
	} {
		if p.bad {
			_, _ = fmt.Fprintf(stderr, "miami restore: %s\n", p.msg)
			return 2
		}
	}
	for _, p := range []struct{ flag, path string }{{"store", store}, {"out", dest}} {
		if err := checkFlagPath(p.path); err != nil {
			_, _ = fmt.Fprintf(stderr, "miami restore: -%s: %v\n", p.flag, err)
			return 2
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	m, err := restoreVolume(ctx, store, dest, manifest, restoreHooks{})
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "miami restore: %v\n", err)
		return 1
	}
	if m.Volume != filepath.Base(store) {
		// Not an error: a copy of a store (an offsite backup, a snapshot) is
		// often a renamed directory, and restoring it is the point. But a
		// manifest that names another volume is worth a look.
		_, _ = fmt.Fprintf(stderr, "miami restore: warning: the manifest is of volume %s, but the store directory is named %s\n", m.Volume, filepath.Base(store))
	}
	_, _ = fmt.Fprintf(stdout, "restored manifest %s of %s: %d bytes in %d chunks to %s\n", m.Name(), m.Volume, m.Size, len(m.Chunks), dest)
	return 0
}

// checkFlagPath requires an absolute path with no ".." element, so that a flag
// names exactly one place however the working directory is set.
func checkFlagPath(p string) error {
	if !filepath.IsAbs(p) {
		return errors.New("must be an absolute path")
	}
	for _, el := range strings.Split(p, string(filepath.Separator)) {
		if el == ".." {
			return errors.New("must not contain a .. element")
		}
	}
	return nil
}

// within reports whether the real location of dir is root or below it.
func within(dir, root string) (bool, error) {
	d, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return false, err
	}
	r, err := filepath.EvalSymlinks(root)
	if err != nil {
		return false, err
	}
	rel, err := filepath.Rel(r, d)
	if err != nil {
		return false, err
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))), nil
}

// restoreHooks let a test observe the steps of restoreVolume; every field may
// be nil.
type restoreHooks struct {
	beforeCreate  func()                 // called just before the temporary file is created
	afterAssemble func(tmp string)       // called once the temporary file is complete and synced
	syncDir       func(dir string) error // replaces the output directory's fsync
}

// restoreVolume writes the named manifest of the store (the one current names
// when name is empty) to a new file at dest. Every chunk is verified before
// anything is created. The image is assembled into a hidden temporary file
// beside dest and linked to dest only when complete, so dest either does not
// exist or is the whole image, whatever kills the process; the link fails if
// dest exists, so an existing file is never replaced. It never writes to the
// store.
func restoreVolume(ctx context.Context, store, dest, name string, h restoreHooks) (chunk.Manifest, error) {
	s, err := chunk.OpenReadOnlyOperator(store)
	if err != nil {
		return chunk.Manifest{}, err
	}
	defer func() { _ = s.Close() }()
	if inside, err := within(filepath.Dir(dest), store); err != nil {
		return chunk.Manifest{}, fmt.Errorf("output directory: %w", err)
	} else if inside {
		return chunk.Manifest{}, errors.New("the output file must not be inside the store")
	}
	if _, err := os.Lstat(dest); err == nil {
		return chunk.Manifest{}, fmt.Errorf("%s already exists", dest)
	}
	if name == "" {
		cur, ok, err := s.Current(ctx)
		if err != nil {
			return chunk.Manifest{}, err
		}
		if !ok {
			return chunk.Manifest{}, chunk.ErrNoCurrent
		}
		name = cur
	}
	m, err := s.ReadManifest(ctx, name)
	if err != nil {
		return chunk.Manifest{}, err
	}
	if err := s.Verify(ctx, m); err != nil {
		return chunk.Manifest{}, fmt.Errorf("nothing written: %w", err)
	}

	if h.beforeCreate != nil {
		h.beforeCreate()
	}
	tmp := filepath.Join(filepath.Dir(dest), "."+filepath.Base(dest)+".tmp-"+randHex())
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600) //nolint:gosec // dest is a validated absolute path
	if err != nil {
		return chunk.Manifest{}, err
	}
	defer func() { _ = os.Remove(tmp) }() // also on panic; after a successful link it removes the second name
	err = s.Assemble(ctx, m, f)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return chunk.Manifest{}, err
	}
	if h.afterAssemble != nil {
		h.afterAssemble(tmp)
	}
	if err := os.Link(tmp, dest); err != nil {
		return chunk.Manifest{}, err
	}
	// Make the name durable, so a host crash cannot lose a completed image.
	sync := h.syncDir
	if sync == nil {
		sync = fsyncDir
	}
	if err := sync(filepath.Dir(dest)); err != nil {
		return chunk.Manifest{}, fmt.Errorf("sync %s: %w", filepath.Dir(dest), err)
	}
	return m, nil
}

// fsyncDir fsyncs a directory so a link into it survives a crash.
func fsyncDir(dir string) error {
	d, err := os.Open(dir) //nolint:gosec // dir is the parent of a validated absolute path
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}

func randHex() string {
	var b [6]byte
	_, _ = rand.Read(b[:]) // crypto/rand does not fail on supported platforms
	return hex.EncodeToString(b[:])
}
