package observe

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
)

// Table is a parsed mount table, with the queries the reconciler needs.
type Table []Mount

// At returns the top-most mount at exactly mountPoint: when several are
// stacked there, the one with the highest mount ID.
func (t Table) At(mountPoint string) (Mount, bool) {
	mountPoint = filepath.Clean(mountPoint)
	var best Mount
	found := false
	for _, m := range t {
		if m.MountPoint != mountPoint {
			continue
		}
		if !found || m.ID > best.ID {
			best, found = m, true
		}
	}
	return best, found
}

// Under returns every mount at or below the directory prefix, deepest first,
// so that unmounting the result in order never meets a busy parent. Mounts
// stacked at one path come top-most first.
func (t Table) Under(prefix string) Table {
	prefix = filepath.Clean(prefix)
	var out Table
	for _, m := range t {
		if within(prefix, m.MountPoint) {
			out = append(out, m)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		di, dj := depth(out[i].MountPoint), depth(out[j].MountPoint)
		if di != dj {
			return di > dj
		}
		return out[i].ID > out[j].ID
	})
	return out
}

// IsBindOf reports whether the mount at mountPoint is a bind of exactly
// sourceDir: the same device as the mount that contains sourceDir, and a Root
// equal to sourceDir's path inside that filesystem. A mount that is a bind of
// a bind answers true for the original directory too, because the kernel
// reports both with the same device and root.
//
// It is false, with no error, when nothing is mounted at mountPoint, and when
// the only mount that could answer is the one containing sourceDir itself. The
// mount that contains sourceDir is the top-most one at the longest mount point
// that is a prefix of sourceDir; if the table has none, or either path is not
// absolute, the answer is an error. Both paths must already be resolved:
// symlinks are not followed.
func (t Table) IsBindOf(mountPoint, sourceDir string) (bool, error) {
	if !filepath.IsAbs(mountPoint) {
		return false, fmt.Errorf("observe: mount point %q is not absolute", mountPoint)
	}
	if !filepath.IsAbs(sourceDir) {
		return false, fmt.Errorf("observe: source directory %q is not absolute", sourceDir)
	}
	mountPoint, sourceDir = filepath.Clean(mountPoint), filepath.Clean(sourceDir)

	target, ok := t.At(mountPoint)
	if !ok {
		return false, nil
	}
	container, ok := t.containing(sourceDir)
	if !ok {
		return false, fmt.Errorf("observe: no mount contains %q", sourceDir)
	}
	if container.ID == target.ID {
		return false, nil
	}
	rel, err := filepath.Rel(container.MountPoint, sourceDir)
	if err != nil {
		return false, fmt.Errorf("observe: %w", err)
	}
	want := filepath.Join(container.Root, rel)
	return target.Major == container.Major && target.Minor == container.Minor && target.Root == want, nil
}

// containing returns the top-most mount at the longest mount point that
// contains path.
func (t Table) containing(path string) (Mount, bool) {
	var best Mount
	found := false
	for _, m := range t {
		if !within(m.MountPoint, path) {
			continue
		}
		switch {
		case !found,
			len(m.MountPoint) > len(best.MountPoint),
			len(m.MountPoint) == len(best.MountPoint) && m.ID > best.ID:
			best, found = m, true
		}
	}
	return best, found
}

// within reports whether path is dir or lies below it. Both are clean.
func within(dir, path string) bool {
	if path == dir || dir == "/" {
		return true
	}
	return strings.HasPrefix(path, dir+"/")
}

func depth(p string) int {
	if p == "/" {
		return 0
	}
	return strings.Count(p, "/")
}
