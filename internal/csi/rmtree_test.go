package csi

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"

	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/mount/fake"
	"github.com/greghaskins/csi-miami/internal/observe"
)

// tree is a mount directory with the layout every test here starts from:
//
//	<mount>/teams/<nameA>/{a.txt, sub/b.txt, sub/link -> outside}
//	<outside>/<nameA>/precious
type tree struct {
	mount, outside, dir string
	elems               []string
}

func newTree(t *testing.T) tree {
	t.Helper()
	tr := tree{mount: resolved(t, t.TempDir()), outside: resolved(t, t.TempDir())}
	tr.dir = filepath.Join(tr.mount, "teams", nameA)
	tr.elems = []string{"teams", nameA}
	writeFile(t, filepath.Join(tr.dir, "a.txt"), "a")
	writeFile(t, filepath.Join(tr.dir, "sub", "b.txt"), "b")
	mustSymlink(t, tr.outside, filepath.Join(tr.dir, "sub", "link"))
	writeFile(t, filepath.Join(tr.outside, nameA, "precious"), "x")
	return tr
}

func (tr tree) precious(t *testing.T) {
	t.Helper()
	mustExist(t, filepath.Join(tr.outside, nameA, "precious"))
}

func TestRemoveBeneathRemovesTheTreeAndNothingElse(t *testing.T) {
	t.Parallel()
	tr := newTree(t)
	sibling := filepath.Join(tr.mount, "teams", nameB, "keep")
	writeFile(t, sibling, "k")

	removed, err := (&treeRemover{}).removeBeneath(context.Background(), tr.mount, tr.elems, wantOf(t, tr.mount))
	if err != nil || !removed {
		t.Fatalf("removed = %v, error = %v", removed, err)
	}
	mustNotExist(t, tr.dir)
	mustExist(t, sibling)
	tr.precious(t) // the symlink inside the tree was unlinked, not followed
	mustExist(t, filepath.Join(tr.mount, "teams"))
}

func TestRemoveBeneathMissingPathIsNothingToRemove(t *testing.T) {
	t.Parallel()
	tr := newTree(t)
	for _, elems := range [][]string{{"nope", nameA}, {"teams", nameB}, {nameA}} {
		removed, err := (&treeRemover{}).removeBeneath(context.Background(), tr.mount, elems, wantOf(t, tr.mount))
		if err != nil || removed {
			t.Errorf("%v: removed = %v, error = %v; want false, nil", elems, removed, err)
		}
	}
	mustExist(t, filepath.Join(tr.dir, "a.txt"))
}

func TestRemoveBeneathNeedsAnExistingMount(t *testing.T) {
	t.Parallel()
	tr := newTree(t)
	_, err := (&treeRemover{}).removeBeneath(context.Background(), filepath.Join(tr.mount, "missing"), tr.elems, transportMount{})
	if codeOf(err) != codes.Unavailable {
		t.Errorf("code = %v (%v), want Unavailable", codeOf(err), err)
	}
	_, err = (&treeRemover{}).removeBeneath(context.Background(), filepath.Join(tr.dir, "a.txt"), tr.elems, transportMount{})
	if codeOf(err) != codes.Unavailable {
		t.Errorf("a file as the mount: code = %v (%v), want Unavailable", codeOf(err), err)
	}
}

// A kernel without openat2 (before Linux 5.6) removes nothing, and says why.
func TestRemoveBeneathWithoutOpenat2RemovesNothing(t *testing.T) {
	t.Parallel()
	tr := newTree(t)
	r := &treeRemover{open: func(int, string, *unix.OpenHow) (int, error) { return -1, unix.ENOSYS }}
	removed, err := r.removeBeneath(context.Background(), tr.mount, tr.elems, wantOf(t, tr.mount))
	if codeOf(err) != codes.Unavailable || !strings.Contains(err.Error(), "Linux 5.6") || removed {
		t.Fatalf("removed = %v, error = %v; want Unavailable naming Linux 5.6", removed, err)
	}
	mustExist(t, filepath.Join(tr.dir, "a.txt"))
	mustExist(t, filepath.Join(tr.dir, "sub", "b.txt"))
}

// Each case changes the world at the moment the removal is about to open
// something, which is the window the path checks cannot close. The removal
// must stop with Unavailable and leave what it was kept from alone.
func TestRemoveBeneathStopsWhenThePathChangesUnderIt(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		// swap runs when the removal is about to open name in parent.
		swap func(t *testing.T, tr tree, parent, name string)
		when func(parent, name string, tr tree) bool
	}{
		{
			name: "an ancestor becomes a symlink",
			when: func(parent, name string, tr tree) bool { return parent == tr.mount && name == "teams" },
			swap: func(t *testing.T, tr tree, _, _ string) {
				if err := os.Rename(filepath.Join(tr.mount, "teams"), filepath.Join(tr.mount, "teams.moved")); err != nil {
					t.Fatal(err)
				}
				mustSymlink(t, tr.outside, filepath.Join(tr.mount, "teams"))
			},
		},
		{
			name: "the volume directory becomes a symlink",
			when: func(parent, name string, tr tree) bool {
				return name == nameA && parent == filepath.Join(tr.mount, "teams")
			},
			swap: func(t *testing.T, tr tree, _, _ string) {
				if err := os.Rename(tr.dir, tr.dir+".moved"); err != nil {
					t.Fatal(err)
				}
				mustSymlink(t, filepath.Join(tr.outside, nameA), tr.dir)
			},
		},
		{
			name: "a subdirectory becomes a symlink",
			when: func(_, name string, _ tree) bool { return name == "sub" },
			swap: func(t *testing.T, tr tree, _, _ string) {
				if err := os.Rename(filepath.Join(tr.dir, "sub"), filepath.Join(tr.dir, "sub.moved")); err != nil {
					t.Fatal(err)
				}
				mustSymlink(t, tr.outside, filepath.Join(tr.dir, "sub"))
			},
		},
		{
			name: "a subdirectory becomes a file",
			when: func(_, name string, _ tree) bool { return name == "sub" },
			swap: func(t *testing.T, tr tree, _, _ string) {
				if err := os.Rename(filepath.Join(tr.dir, "sub"), filepath.Join(tr.dir, "sub.moved")); err != nil {
					t.Fatal(err)
				}
				writeFile(t, filepath.Join(tr.dir, "sub"), "now a file")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tr := newTree(t)
			r := &treeRemover{beforeOpen: func(parent, name string) {
				if tt.when(parent, name, tr) {
					tt.swap(t, tr, parent, name)
				}
			}}
			_, err := r.removeBeneath(context.Background(), tr.mount, tr.elems, wantOf(t, tr.mount))
			if codeOf(err) != codes.Unavailable || !strings.Contains(err.Error(), "changed under the removal") {
				t.Fatalf("error = %v, want Unavailable saying the path changed", err)
			}
			tr.precious(t)
			mustExist(t, filepath.Join(tr.outside, nameA))
		})
	}
}

func TestRemoveBeneathStopsWhenTheContextEnds(t *testing.T) {
	t.Parallel()
	tr := newTree(t)
	ctx, cancel := context.WithCancel(context.Background())
	r := &treeRemover{beforeOpen: func(_, name string) {
		if name == "sub" {
			cancel()
		}
	}}
	_, err := r.removeBeneath(ctx, tr.mount, tr.elems, wantOf(t, tr.mount))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	tr.precious(t)
}

func TestRemoveBeneathRefusesATreeThatIsTooDeep(t *testing.T) {
	t.Parallel()
	tr := newTree(t)
	fd, err := unix.Open(tr.dir, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i <= maxTreeDepth+1; i++ {
		if err := unix.Mkdirat(fd, "d", 0o750); err != nil {
			t.Fatal(err)
		}
		next, err := unix.Openat(fd, "d", unix.O_RDONLY|unix.O_DIRECTORY, 0)
		if err != nil {
			t.Fatal(err)
		}
		_ = unix.Close(fd)
		fd = next
	}
	_ = unix.Close(fd)

	_, err = (&treeRemover{}).removeBeneath(context.Background(), tr.mount, tr.elems, wantOf(t, tr.mount))
	if codeOf(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "nested more than") {
		t.Fatalf("error = %v, want FailedPrecondition about the depth, which a retry will not change", err)
	}
	mustExist(t, tr.dir) // the too-deep tree is still there, and so is what is outside
	tr.precious(t)
}

// The whole path through removeItemDir, with the remover given: the checks
// that give clear messages run first and the descriptor-relative removal
// still decides.
func TestRemoveItemDirUsesTheDescriptorRemoval(t *testing.T) {
	t.Parallel()
	tr := newTree(t)
	r := &treeRemover{open: func(int, string, *unix.OpenHow) (int, error) { return -1, unix.ENOSYS }}
	removed, err := removeItemDirWith(context.Background(), mounterWithMount(t, tr.mount), r, tr.mount, tr.dir, nameA)
	if codeOf(err) != codes.Unavailable || !strings.Contains(err.Error(), "Linux 5.6") || removed {
		t.Fatalf("removed = %v, error = %v", removed, err)
	}
	mustExist(t, filepath.Join(tr.dir, "a.txt"))

	removed, err = removeItemDirWith(context.Background(), mounterWithMount(t, tr.mount), &treeRemover{}, tr.mount, tr.dir, nameA)
	if err != nil || !removed {
		t.Fatalf("removed = %v, error = %v", removed, err)
	}
	mustNotExist(t, tr.dir)
	tr.precious(t)
}

// The transport mount is opened by path; what the removal got must be what the
// mount table showed for the transport, by device and by mount ID.
func TestRemoveBeneathRefusesADirectoryThatIsNotTheTransportInTheTable(t *testing.T) {
	t.Parallel()
	good := func(tr tree) transportMount { return wantOf(t, tr.mount) }
	tests := []struct {
		name string
		want func(tr tree) transportMount
	}{
		{"another major", func(tr tree) transportMount { w := good(tr); w.major++; return w }},
		{"another minor", func(tr tree) transportMount { w := good(tr); w.minor++; return w }},
		{"another mount ID", func(tr tree) transportMount { w := good(tr); w.id++; return w }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tr := newTree(t)
			removed, err := (&treeRemover{}).removeBeneath(context.Background(), tr.mount, tr.elems, tt.want(tr))
			if codeOf(err) != codes.Unavailable || !strings.Contains(err.Error(), "not the mount the mount table showed") || removed {
				t.Fatalf("removed = %v, error = %v; want Unavailable saying the root is not the mount in the table", removed, err)
			}
			mustExist(t, filepath.Join(tr.dir, "a.txt"))
			tr.precious(t)
		})
	}
}

// If the table shows no mount at the transport's directory, the transport was
// unmounted and what is there is not it. Before this check the elements were
// simply missing, and the delete "succeeded" while the directory leaked.
func TestRemoveItemDirRefusesATransportThatIsNotInTheTable(t *testing.T) {
	t.Parallel()
	tr := newTree(t)
	for name, m := range map[string]*fake.Mounter{
		"an empty table":               {},
		"a mount elsewhere only":       mounterWithMount(t, tr.outside),
		"a mount below, not at, mount": mounterWithMount(t, filepath.Join(tr.mount, "teams")),
	} {
		removed, err := removeItemDirWith(context.Background(), m, &treeRemover{}, tr.mount, tr.dir, nameA)
		if codeOf(err) != codes.Unavailable || !strings.Contains(err.Error(), "not mounted") || removed {
			t.Errorf("%s: removed = %v, error = %v; want Unavailable saying the transport is not mounted", name, removed, err)
		}
		mustExist(t, filepath.Join(tr.dir, "a.txt"))
	}
}

// unmountedAfter is a Mounter whose table shows the mount at dir for the first n
// reads and not after: the transport is unmounted between DeleteVolume
// mounting it and the removal looking.
type unmountedAfter struct {
	mount.Mounter
	dir   string
	n     int
	mu    sync.Mutex
	reads int
}

func (m *unmountedAfter) Mounts(ctx context.Context) (observe.Table, error) {
	table, err := m.Mounter.Mounts(ctx)
	m.mu.Lock()
	m.reads++
	gone := m.reads > m.n
	m.mu.Unlock()
	if err != nil || !gone {
		return table, err
	}
	var kept observe.Table
	for _, e := range table {
		if e.MountPoint != m.dir {
			kept = append(kept, e)
		}
	}
	return kept, nil
}

func TestDeleteVolumeWhenTheTransportIsUnmountedBetweenMountAndRemoval(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id, dir, sibling := volumeWithContent(t, e)
	gone := &unmountedAfter{Mounter: e.mounter, dir: e.mountDir(), n: 1}
	ctrl, err := NewController(ControllerOptions{Root: e.root, Mounter: gone, PVs: e.pvs})
	if err != nil {
		t.Fatal(err)
	}

	_, err = ctrl.DeleteVolume(context.Background(), deleteReq(id))
	if codeOf(err) != codes.Unavailable || !strings.Contains(err.Error(), "not mounted") {
		t.Fatalf("error = %v, want Unavailable saying the transport is not mounted", err)
	}
	if gone.reads < 2 {
		t.Fatalf("the table was read %d times; the removal's own read was not reached", gone.reads)
	}
	mustExist(t, filepath.Join(dir, "a.txt"))
	mustExist(t, filepath.Join(sibling, "keep.txt"))
}

// CreateVolume walks the path relative to descriptors too: an ancestor swapped
// for a symlink to outside the transport, right before it is opened, must not
// let the new directory be made outside.
func TestEnsureBeneathNeverCreatesOutsideTheTransport(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		swap func(t *testing.T, tr tree)
		code codes.Code
	}{
		{"an ancestor becomes a symlink", func(t *testing.T, tr tree) {
			if err := os.Rename(filepath.Join(tr.mount, "teams"), filepath.Join(tr.mount, "teams.moved")); err != nil {
				t.Fatal(err)
			}
			mustSymlink(t, tr.outside, filepath.Join(tr.mount, "teams"))
		}, codes.FailedPrecondition},
		{"an ancestor becomes a file", func(t *testing.T, tr tree) {
			if err := os.Rename(filepath.Join(tr.mount, "teams"), filepath.Join(tr.mount, "teams.moved")); err != nil {
				t.Fatal(err)
			}
			writeFile(t, filepath.Join(tr.mount, "teams"), "x")
		}, codes.FailedPrecondition},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tr := newTree(t)
			swapped := false
			r := &treeRemover{beforeOpen: func(parent, name string) {
				if parent == tr.mount && name == "teams" && !swapped {
					swapped = true
					tt.swap(t, tr)
				}
			}}
			created, err := ensureItemDirWith(r, tr.mount, filepath.Join(tr.mount, "teams", nameB), 0o755)
			if codeOf(err) != tt.code || created {
				t.Fatalf("created = %v, error = %v; want %v", created, err, tt.code)
			}
			mustNotExist(t, filepath.Join(tr.outside, nameB))
			tr.precious(t)
		})
	}
}

func TestEnsureBeneathCreatesTheMissingElementsAndSetsTheMode(t *testing.T) {
	t.Parallel()
	tr := newTree(t)
	dir := filepath.Join(tr.mount, "a", "b", nameB)
	created, err := ensureItemDirWith(&treeRemover{}, tr.mount, dir, 0o755)
	if err != nil || !created {
		t.Fatalf("created = %v, error = %v", created, err)
	}
	for _, d := range []string{filepath.Join(tr.mount, "a"), filepath.Join(tr.mount, "a", "b"), dir} {
		fi, err := os.Lstat(d)
		if err != nil || !fi.IsDir() || fi.Mode().Perm() != 0o755 {
			t.Errorf("%s: %v, %v; want a directory of mode 755", d, fi, err)
		}
	}
	// Again: adopted, not created.
	created, err = ensureItemDirWith(&treeRemover{}, tr.mount, dir, 0o755)
	if err != nil || created {
		t.Errorf("again: created = %v, error = %v; want adopted", created, err)
	}
}

// Without openat2 nothing is made.
func TestEnsureBeneathWithoutOpenat2CreatesNothing(t *testing.T) {
	t.Parallel()
	tr := newTree(t)
	r := &treeRemover{open: func(int, string, *unix.OpenHow) (int, error) { return -1, unix.ENOSYS }}
	created, err := ensureItemDirWith(r, tr.mount, filepath.Join(tr.mount, "x", nameB), 0o755)
	if codeOf(err) != codes.Unavailable || !strings.Contains(err.Error(), "Linux 5.6") || created {
		t.Fatalf("created = %v, error = %v", created, err)
	}
	mustNotExist(t, filepath.Join(tr.mount, "x"))
}

func TestEnsureBeneathRefusesWhatIsNotADirectory(t *testing.T) {
	t.Parallel()
	tr := newTree(t)
	// The final element is a file: AlreadyExists. A middle one is a file or a
	// symlink: FailedPrecondition.
	writeFile(t, filepath.Join(tr.mount, "afile"), "x")
	mustSymlink(t, tr.outside, filepath.Join(tr.mount, "alink"))
	tests := []struct {
		elems []string
		code  codes.Code
	}{
		{[]string{"afile"}, codes.AlreadyExists},
		{[]string{"alink"}, codes.AlreadyExists},
		{[]string{"afile", nameB}, codes.FailedPrecondition},
		{[]string{"alink", nameB}, codes.FailedPrecondition},
	}
	for _, tt := range tests {
		_, err := (&treeRemover{}).ensureBeneath(tr.mount, tt.elems, 0o755)
		if codeOf(err) != tt.code {
			t.Errorf("%v: code = %v (%v), want %v", tt.elems, codeOf(err), err, tt.code)
		}
	}
	mustNotExist(t, filepath.Join(tr.outside, nameB))
}

// The last element takes the mode given, whatever the umask, and the elements
// above it 0755. It sets the process umask, so it is not parallel.
func TestEnsureBeneathLastElementTakesTheMode(t *testing.T) {
	old := unix.Umask(0o077)
	defer unix.Umask(old)
	tr := newTree(t)
	dir := filepath.Join(tr.mount, "a", nameB)
	created, err := ensureItemDirWith(&treeRemover{}, tr.mount, dir, 0o2775)
	if err != nil || !created {
		t.Fatalf("created = %v, error = %v", created, err)
	}
	fi, err := os.Lstat(dir)
	if err != nil || fi.Mode()&(os.ModePerm|os.ModeSetgid) != 0o775|os.ModeSetgid {
		t.Errorf("last element: %v, %v; want 2775", fi, err)
	}
	if fi, err := os.Lstat(filepath.Join(tr.mount, "a")); err != nil || fi.Mode().Perm() != 0o755 {
		t.Errorf("parent: %v, %v; want 755", fi, err)
	}
}
