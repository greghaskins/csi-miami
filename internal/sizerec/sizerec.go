// Package sizerec is the durable size of a volume of kind volume that has
// been grown: one small file beside the volume's directory in its transport,
// <dir>/<name>.size, holding the size in bytes in decimal.
//
// A PersistentVolume's attributes are immutable, so the size the volume was
// created with is all a later stage on any node finds there. ControllerExpand
// raises this record before it answers, and a stage takes the largest of the
// attributes' size, this record and the size of the current manifest, so a
// volume that was grown, unstaged and staged again (on any node) is never
// brought up smaller than it was.
//
// The record only ever grows. Grow is the one writer; it writes a temporary
// file, syncs it, renames it over the record and syncs the directory, so a
// reader sees the old size or the new one and a crash leaves one of them.
package sizerec

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/greghaskins/csi-miami/internal/safefs"
)

// maxBytes bounds the record: a positive int64 in decimal and a newline.
const maxBytes = 32

// ErrMalformed is a record that is not a positive decimal number and a
// newline. It is never repaired or overwritten: a smaller size must not come
// from a record that could not be read.
var ErrMalformed = errors.New("the size record is malformed")

// Path is the record of the volume called name, whose directory is below dir.
func Path(dir, name string) string { return filepath.Join(dir, name+recordSuffix) }

const recordSuffix = ".size"

// Read returns the recorded size, and false when there is no record. The
// record is on a shared export: it is read without following a symlink, must be
// a small regular file, and what it holds is never put in an error.
func Read(dir, name string) (int64, bool, error) {
	d, err := safefs.OpenRoot(dir)
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = d.Close() }()
	return read(d, dir, name)
}

func read(d *safefs.Dir, dir, name string) (int64, bool, error) {
	b, err := d.ReadFile(name+recordSuffix, maxBytes)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, false, nil
	}
	if errors.Is(err, safefs.ErrTooLarge) || errors.Is(err, safefs.ErrNotRegular) {
		return 0, false, fmt.Errorf("%w: %s", ErrMalformed, Path(dir, name))
	}
	if err != nil {
		return 0, false, err
	}
	s, ok := strings.CutSuffix(string(b), "\n")
	n, perr := strconv.ParseInt(s, 10, 64)
	if !ok || perr != nil || n <= 0 || strconv.FormatInt(n, 10) != s {
		return 0, false, fmt.Errorf("%w: %s", ErrMalformed, Path(dir, name))
	}
	return n, true, nil
}

// Grow raises the record to size if it is larger than the record, and returns
// the size the record has afterwards (the larger of the two). A size that is
// not larger changes nothing. The caller serialises Grow per volume.
func Grow(dir, name string, size int64) (int64, error) {
	if size <= 0 {
		return 0, fmt.Errorf("sizerec: size %d is not positive", size)
	}
	d, err := safefs.OpenRoot(dir)
	if err != nil {
		return 0, err
	}
	defer func() { _ = d.Close() }()
	cur, ok, err := read(d, dir, name)
	if err != nil {
		return 0, err
	}
	if ok && cur >= size {
		return cur, nil
	}
	var rnd [6]byte
	_, _ = rand.Read(rnd[:]) // crypto/rand does not fail on supported platforms
	tmp := name + ".size.tmp-" + hex.EncodeToString(rnd[:])
	f, err := d.Create(tmp, 0o644) // other nodes' users must read the record
	if err != nil {
		return 0, err
	}
	cherr := f.Chmod(0o644) //nolint:gosec // other nodes' users must read the record
	_, werr := f.WriteString(strconv.FormatInt(size, 10) + "\n")
	serr := f.Sync()
	// Close is where an NFS client reports a failed write.
	cerr := f.Close()
	if err := errors.Join(cherr, werr, serr, cerr); err != nil {
		_ = d.Unlink(tmp)
		return 0, err
	}
	if err := d.Rename(tmp, d, name+recordSuffix); err != nil {
		_ = d.Unlink(tmp)
		return 0, err
	}
	if err := d.Sync(); err != nil {
		return 0, err
	}
	return size, nil
}
