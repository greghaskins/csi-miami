package main

import (
	"bytes"
	"context"
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

	m, err := restoreVolume(ctx, store, dest, manifest)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "miami restore: %v\n", err)
		return 1
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

// restoreVolume writes the named manifest of the store (the one current names
// when name is empty) to a new file at dest. Every chunk is verified before
// dest is created, and dest is removed again if assembling fails after that,
// so a damaged store leaves no image behind. It never writes to the store.
func restoreVolume(ctx context.Context, store, dest, name string) (chunk.Manifest, error) {
	s, err := chunk.OpenReadOnly(store)
	if err != nil {
		return chunk.Manifest{}, err
	}
	if inside, err := within(filepath.Dir(dest), store); err != nil {
		return chunk.Manifest{}, fmt.Errorf("output directory: %w", err)
	} else if inside {
		return chunk.Manifest{}, errors.New("the output file must not be inside the store")
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

	f, err := os.OpenFile(dest, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600) //nolint:gosec // dest is a validated absolute path
	if err != nil {
		return chunk.Manifest{}, err
	}
	err = s.Assemble(ctx, m, f)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(dest)
		return chunk.Manifest{}, err
	}
	return m, nil
}
