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
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// ErrMalformed is a record that is not a positive decimal number and a
// newline. It is never repaired or overwritten: a smaller size must not come
// from a record that could not be read.
var ErrMalformed = errors.New("the size record is malformed")

// Path is the record of the volume called name, whose directory is below dir.
func Path(dir, name string) string { return filepath.Join(dir, name+".size") }

// Read returns the recorded size, and false when there is no record.
func Read(dir, name string) (int64, bool, error) {
	b, err := os.ReadFile(Path(dir, name))
	if errors.Is(err, fs.ErrNotExist) {
		return 0, false, nil
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
	cur, ok, err := Read(dir, name)
	if err != nil {
		return 0, err
	}
	if ok && cur >= size {
		return cur, nil
	}
	path := Path(dir, name)
	f, err := os.CreateTemp(dir, name+".size.tmp-*")
	if err != nil {
		return 0, err
	}
	tmp := f.Name()
	_, werr := f.WriteString(strconv.FormatInt(size, 10) + "\n")
	serr := f.Sync()
	// Close is where an NFS client reports a failed write.
	cerr := f.Close()
	if err := errors.Join(werr, serr, cerr); err != nil {
		_ = os.Remove(tmp)
		return 0, err
	}
	if err := os.Chmod(tmp, 0o644); err != nil { //nolint:gosec // other nodes' users must read the record
		_ = os.Remove(tmp)
		return 0, err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return 0, err
	}
	d, err := os.Open(dir) // #nosec G304 -- the volume's own parent directory
	if err != nil {
		return 0, err
	}
	serr = d.Sync()
	cerr = d.Close()
	if err := errors.Join(serr, cerr); err != nil {
		return 0, err
	}
	return size, nil
}
