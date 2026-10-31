package observe

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// MountinfoPath is the kernel's per-process mount table.
const MountinfoPath = "/proc/self/mountinfo"

// maxLine bounds one mountinfo line. Super options of a busy filesystem can
// run to many kilobytes; a megabyte is far past anything real.
const maxLine = 1 << 20

// Mount is one line of /proc/self/mountinfo, decoded.
type Mount struct {
	ID, ParentID int
	Major, Minor int
	// Root is the path inside the source filesystem that is mounted. A bind
	// mount of a subdirectory shows that subdirectory here. The kernel's
	// "//deleted" suffix, which marks a root whose directory has been
	// removed, is stripped into Deleted.
	Root       string
	Deleted    bool
	MountPoint string
	// Options are the per-mount options, e.g. "rw", "ro", "noatime".
	Options []string
	// Optional are the optional fields, e.g. "shared:12". There may be none.
	Optional     []string
	FSType       string
	Source       string
	SuperOptions []string
}

// ReadOnly reports whether this mount is read-only: the per-mount "ro"
// option, which is what a read-only bind remount sets. The superblock's own
// options are not consulted.
func (m Mount) ReadOnly() bool {
	for _, o := range m.Options {
		if o == "ro" {
			return true
		}
	}
	return false
}

// ReadMountinfo reads and parses /proc/self/mountinfo.
func ReadMountinfo() ([]Mount, error) {
	f, err := os.Open(MountinfoPath)
	if err != nil {
		return nil, fmt.Errorf("observe: %w", err)
	}
	defer func() { _ = f.Close() }()
	return Parse(f)
}

// Parse decodes mountinfo text, returning the mounts in file order (the
// kernel's creation order). Empty lines are skipped; any other malformed line
// fails the whole parse with an error that names its line number, because a
// table with a missing entry would be a wrong answer about the node.
//
// The format is exact: the kernel separates fields with a single space and
// escapes only space, tab, newline and backslash in paths and sources. Any
// other byte, including a vertical tab or a no-break space, is data, so lines
// are split on single spaces only, and a trailing space is rejected.
func Parse(r io.Reader) ([]Mount, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), maxLine)
	sc.Split(splitLines)
	var out []Mount
	n := 0
	for sc.Scan() {
		n++
		line := sc.Text()
		if line == "" {
			continue
		}
		m, err := parseLine(line)
		if err != nil {
			return nil, fmt.Errorf("observe: mountinfo line %d: %w", n, err)
		}
		out = append(out, m)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("observe: reading mountinfo after line %d: %w", n, err)
	}
	return out, nil
}

// parseLine decodes one line:
//
//	36 35 98:0 /mnt1 /mnt2 rw,noatime master:1 - ext3 /dev/root rw,errors=continue
//	(1)(2)(3)   (4)   (5)      (6)      (7)   (8) (9)   (10)         (11)
func parseLine(line string) (Mount, error) {
	if strings.HasSuffix(line, " ") {
		return Mount{}, errors.New("trailing space")
	}
	f := strings.Split(line, " ")
	if len(f) < 10 {
		return Mount{}, fmt.Errorf("want at least 10 fields, got %d", len(f))
	}
	sep := -1
	for i := 6; i < len(f); i++ {
		if f[i] == "-" {
			sep = i
			break
		}
	}
	if sep < 0 {
		return Mount{}, errors.New(`no "-" separator after the optional fields`)
	}
	for i := 0; i < sep; i++ {
		if f[i] == "" {
			return Mount{}, fmt.Errorf("field %d is empty", i+1)
		}
	}
	// Exactly three fields follow the separator. The source alone may be
	// empty: a mount made with no source prints two spaces there.
	if len(f)-sep-1 != 3 {
		return Mount{}, fmt.Errorf("want exactly 3 fields (filesystem type, source, super options) after the separator, got %d", len(f)-sep-1)
	}
	if f[sep+1] == "" || f[sep+3] == "" {
		return Mount{}, errors.New("filesystem type and super options must not be empty")
	}

	var m Mount
	var err error
	if m.ID, err = atoi("mount ID", f[0]); err != nil {
		return Mount{}, err
	}
	if m.ParentID, err = atoi("parent ID", f[1]); err != nil {
		return Mount{}, err
	}
	maj, minor, ok := strings.Cut(f[2], ":")
	if !ok {
		return Mount{}, fmt.Errorf("device %q is not major:minor", f[2])
	}
	if m.Major, err = atoi("device major", maj); err != nil {
		return Mount{}, err
	}
	if m.Minor, err = atoi("device minor", minor); err != nil {
		return Mount{}, err
	}
	if m.Root, err = unescape(f[3]); err != nil {
		return Mount{}, fmt.Errorf("root: %w", err)
	}
	if root, ok := strings.CutSuffix(m.Root, "//deleted"); ok {
		m.Root, m.Deleted = root, true
	}
	if m.MountPoint, err = unescape(f[4]); err != nil {
		return Mount{}, fmt.Errorf("mount point: %w", err)
	}
	m.Options = strings.Split(f[5], ",")
	if sep > 6 {
		m.Optional = append([]string(nil), f[6:sep]...)
	}
	if m.FSType, err = unescape(f[sep+1]); err != nil {
		return Mount{}, fmt.Errorf("filesystem type: %w", err)
	}
	if m.Source, err = unescape(f[sep+2]); err != nil {
		return Mount{}, fmt.Errorf("source: %w", err)
	}
	m.SuperOptions = strings.Split(f[sep+3], ",")
	return m, nil
}

// splitLines splits on "\n" only. bufio.ScanLines would also strip a trailing
// "\r", which the kernel does not escape and which is therefore data.
func splitLines(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.IndexByte(data, '\n'); i >= 0 {
		return i + 1, data[:i], nil
	}
	if atEOF {
		return len(data), data, nil
	}
	return 0, nil, nil
}

func atoi(what, s string) (int, error) {
	v, err := strconv.ParseUint(s, 10, 31)
	if err != nil {
		return 0, fmt.Errorf("%s %q is not a non-negative integer", what, s)
	}
	return int(v), nil
}

// unescape decodes the kernel's \ooo octal escapes (space, tab, newline and
// backslash are written \040 \011 \012 \134). Any other backslash is
// malformed.
func unescape(s string) (string, error) {
	if !strings.Contains(s, `\`) {
		return s, nil
	}
	var b strings.Builder
	for i := 0; i < len(s); {
		if s[i] != '\\' {
			b.WriteByte(s[i])
			i++
			continue
		}
		if i+4 > len(s) {
			return "", fmt.Errorf("truncated escape in %q", s)
		}
		v := 0
		for _, d := range []byte(s[i+1 : i+4]) {
			if d < '0' || d > '7' {
				return "", fmt.Errorf("bad escape %q in %q", s[i:i+4], s)
			}
			v = v*8 + int(d-'0')
		}
		if v > 0xff {
			return "", fmt.Errorf("escape %q in %q is out of range", s[i:i+4], s)
		}
		b.WriteByte(byte(v))
		i += 4
	}
	return b.String(), nil
}
