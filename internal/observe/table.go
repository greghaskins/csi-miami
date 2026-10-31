package observe

import (
	"fmt"
	"path/filepath"
	"strings"
)

// Table is a parsed mount table, in mountinfo order, which is the kernel's
// creation order. Its queries follow the mount tree (ParentID), not mount IDs:
// the kernel reuses IDs, so an overmount can have a lower ID than the mount it
// covers.
type Table []Mount

// Resolve returns the mount the kernel resolves path through: from the root of
// the tree, repeatedly take the child mount whose mount point is the longest
// prefix of path on a path boundary. A child mounted at exactly its parent's
// mount point is an overmount that covers the parent's root, and wins over the
// parent's other children, which it hides; chains of overmounts are followed
// to the top. It is false only when the table has no mount that contains path.
// The result's MountPoint equals path only when path is itself a visible mount
// point.
func (t Table) Resolve(path string) (Mount, bool) {
	path = filepath.Clean(path)
	known := make(map[int]bool, len(t))
	for _, m := range t {
		known[m.ID] = true
	}
	var roots []Mount
	for _, m := range t {
		if m.ParentID == m.ID || !known[m.ParentID] {
			roots = append(roots, m)
		}
	}
	cur, ok := pick(roots, path, "")
	if !ok {
		return Mount{}, false
	}
	// The bound only guards against a malformed (cyclic) table.
	for i := 0; i <= len(t); i++ {
		var kids []Mount
		for _, m := range t {
			if m.ParentID == cur.ID && m.ID != cur.ID {
				kids = append(kids, m)
			}
		}
		next, ok := pick(kids, path, cur.MountPoint)
		if !ok {
			return cur, true
		}
		cur = next
	}
	return cur, true
}

// pick chooses among candidate mounts for path. Under a mount at curMP (empty
// for the roots of the tree), a candidate at curMP itself is an overmount and
// wins; otherwise the candidate with the longest mount point that contains
// path does. Ties go to the one later in the table, which was mounted later.
func pick(cands []Mount, path, curMP string) (Mount, bool) {
	var best Mount
	found := false
	if curMP != "" {
		for _, m := range cands {
			if m.MountPoint == curMP {
				best, found = m, true
			}
		}
		if found {
			return best, true
		}
	}
	for _, m := range cands {
		if !within(m.MountPoint, path) {
			continue
		}
		if !found || len(m.MountPoint) >= len(best.MountPoint) {
			best, found = m, true
		}
	}
	return best, found
}

// At returns the mount that is visible at exactly mountPoint: the top of any
// stack of overmounts there. It is false when mountPoint is not a mount point,
// including when a mount there is hidden under an overmount of a parent
// directory.
func (t Table) At(mountPoint string) (Mount, bool) {
	mountPoint = filepath.Clean(mountPoint)
	m, ok := t.Resolve(mountPoint)
	if !ok || m.MountPoint != mountPoint {
		return Mount{}, false
	}
	return m, true
}

// Under returns every mount whose mount point is prefix or below it, hidden
// ones included, newest first. A cover is always newer than what it covers and
// a child than its parent, so unmounting the result in order never meets a
// busy mount. Unmounting by path removes the kernel's top mount at that path,
// so a mount hidden under another is reached only after the one above it is
// gone: a garbage pass that unmounts from Under must repeat, re-reading the
// table, until a pass makes no progress.
func (t Table) Under(prefix string) Table {
	return t.collect(prefix, true)
}

// Below is Under without a mount at prefix itself: only mounts strictly below
// the directory. Use it when the mount at the root of the tree being cleaned
// must never be touched.
func (t Table) Below(prefix string) Table {
	return t.collect(prefix, false)
}

func (t Table) collect(prefix string, includeAt bool) Table {
	prefix = filepath.Clean(prefix)
	var out Table
	for i := len(t) - 1; i >= 0; i-- {
		m := t[i]
		if within(prefix, m.MountPoint) && (includeAt || m.MountPoint != prefix) {
			out = append(out, m)
		}
	}
	return out
}

// IsBindOf reports whether the mount visible at mountPoint is a bind of exactly
// sourceDir: the same device as the mount that contains sourceDir (Resolve),
// and a Root equal to sourceDir's path inside that filesystem. A mount that is
// a bind of a bind answers true for the original directory too, because the
// kernel reports both with the same device and root.
//
// It is false, with no error, when nothing is visible at mountPoint, when
// either mount's root directory has been deleted, and when the only mount that
// could answer is the one containing sourceDir itself. If the table has no
// mount containing sourceDir, or either path is not absolute, the answer is an
// error. Both paths must already be resolved: symlinks are not followed.
//
// Device and root identify the same directory on one superblock only. For NFS
// the source string a mount reports can differ from the one it was given (the
// kernel rewrites it), and two exports of one server can share a superblock,
// so a caller adopting a transport must verify the source string and super
// options itself as well; that is the transport's job, not this table's.
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
	container, ok := t.Resolve(sourceDir)
	if !ok {
		return false, fmt.Errorf("observe: no mount contains %q", sourceDir)
	}
	if container.ID == target.ID || target.Deleted || container.Deleted {
		return false, nil
	}
	rel, err := filepath.Rel(container.MountPoint, sourceDir)
	if err != nil {
		return false, fmt.Errorf("observe: %w", err)
	}
	want := filepath.Join(container.Root, rel)
	return target.Major == container.Major && target.Minor == container.Minor && target.Root == want, nil
}

// within reports whether path is dir or lies below it. Both are clean.
func within(dir, path string) bool {
	if path == dir || dir == "/" {
		return true
	}
	return strings.HasPrefix(path, dir+"/")
}
