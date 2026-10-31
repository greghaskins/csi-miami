package observe

import (
	"fmt"
	"path/filepath"
	"strings"
)

// Table is a parsed mount table, in mountinfo order, which is the kernel's
// creation order. Resolve, At and IsBindOf follow the mount tree (ParentID),
// not mount IDs: the kernel reuses IDs, so an overmount can have a lower ID
// than the mount it covers. Under and Below are plain scans of the table by
// mount point.
type Table []Mount

// Resolve returns the mount the kernel resolves path through: from the root of
// the tree, at each mount take the child whose mount point is the first the
// path crosses, that is the shortest mount point containing it. A mount made
// later at a deeper path under an earlier covering mount is the cover's child,
// not a sibling, and is found on the next step; a sibling made earlier at the
// deeper path is hidden by the cover and never reached. A child mounted at
// exactly its parent's mount point is an overmount that covers the parent's
// root and wins over the parent's other children, which it hides; chains of
// overmounts are followed to the top. It is false only when the table has no
// mount that contains path. The result's MountPoint equals path only when path
// is itself a visible mount point.
//
// Roots (mounts whose parent is not in the table) are not siblings under a
// visible parent, so among them the longest mount point containing path wins.
// A normal table has one root. Cost is linear in the table plus the depth of
// the walk; a cyclic table, which the kernel never produces, ends the walk
// after at most len(t) steps.
func (t Table) Resolve(path string) (Mount, bool) {
	path = filepath.Clean(path)
	known := make(map[int]bool, len(t))
	for _, m := range t {
		known[m.ID] = true
	}
	var roots []Mount
	children := make(map[int][]Mount, len(t))
	for _, m := range t {
		if m.ParentID == m.ID || !known[m.ParentID] {
			roots = append(roots, m)
		} else {
			children[m.ParentID] = append(children[m.ParentID], m)
		}
	}
	cur, ok := pickRoot(roots, path)
	if !ok {
		return Mount{}, false
	}
	for i := 0; i <= len(t); i++ {
		next, ok := pickChild(children[cur.ID], path, cur.MountPoint)
		if !ok {
			return cur, true
		}
		cur = next
	}
	return cur, true
}

// pickRoot chooses among the roots of the tree: the longest mount point that
// contains path, the later one in the table on a tie.
func pickRoot(roots []Mount, path string) (Mount, bool) {
	var best Mount
	found := false
	for _, m := range roots {
		if within(m.MountPoint, path) && (!found || len(m.MountPoint) >= len(best.MountPoint)) {
			best, found = m, true
		}
	}
	return best, found
}

// pickChild chooses among the children of a mount at curMP. A child at curMP
// itself is an overmount and wins (the later one, if several); otherwise the
// child with the shortest mount point that contains path does, the later one
// on a tie.
func pickChild(kids []Mount, path, curMP string) (Mount, bool) {
	var best Mount
	found := false
	for _, m := range kids {
		if m.MountPoint == curMP {
			best, found = m, true
		}
	}
	if found {
		return best, true
	}
	for _, m := range kids {
		if within(m.MountPoint, path) && (!found || len(m.MountPoint) <= len(best.MountPoint)) {
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
// ones included, newest first by mountinfo line. That is usually children
// before parents and covers before what they cover, but not always: a moved
// mount (MS_MOVE) can be older than its new parent, and a propagated mount can
// be tucked beneath an existing one. So unmounting the result in order is a
// good first pass, not a guarantee. Unmount by path removes the kernel's top
// mount at that path only, and a mount hidden under another is reached only
// after the one above it is gone: a garbage pass must repeat, re-reading the
// table, until a pass makes no progress, and At is the authority for what is
// visible.
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
	return t.isBindOf(mountPoint, sourceDir, false)
}

// IsBindOfIncludingDeleted is IsBindOf for the one question it must answer
// differently: whether a mount is still a bind of a directory that has since
// been removed. When a directory that is bind-mounted is removed, the kernel
// marks the mount's root deleted, and IsBindOf then says false for it, which
// is right for deciding whether a mount is usable. A caller deciding whether
// a mount is the one it made, so that it may unmount it, needs the other
// answer: the mount still has the same device and the same root path as the
// source had, deleted or not, and nothing else could have been mounted there
// under that path since. The Deleted flag is ignored on both sides; everything
// else, including the rule that the mount containing the source is not a bind
// of it, is as in IsBindOf. For a removed directory the answer is "same device
// and same last-known root path", not a proof of identity: a different
// directory renamed to that path before the removal would answer true.
func (t Table) IsBindOfIncludingDeleted(mountPoint, sourceDir string) (bool, error) {
	return t.isBindOf(mountPoint, sourceDir, true)
}

func (t Table) isBindOf(mountPoint, sourceDir string, deletedOK bool) (bool, error) {
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
	if container.ID == target.ID || !deletedOK && (target.Deleted || container.Deleted) {
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
