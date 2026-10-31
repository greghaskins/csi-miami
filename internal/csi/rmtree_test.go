package csi

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"

	"github.com/greghaskins/csi-miami/internal/mount/fake"
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

	removed, err := (&treeRemover{}).removeBeneath(context.Background(), tr.mount, tr.elems)
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
		removed, err := (&treeRemover{}).removeBeneath(context.Background(), tr.mount, elems)
		if err != nil || removed {
			t.Errorf("%v: removed = %v, error = %v; want false, nil", elems, removed, err)
		}
	}
	mustExist(t, filepath.Join(tr.dir, "a.txt"))
}

func TestRemoveBeneathNeedsAnExistingMount(t *testing.T) {
	t.Parallel()
	tr := newTree(t)
	_, err := (&treeRemover{}).removeBeneath(context.Background(), filepath.Join(tr.mount, "missing"), tr.elems)
	if codeOf(err) != codes.Unavailable {
		t.Errorf("code = %v (%v), want Unavailable", codeOf(err), err)
	}
	_, err = (&treeRemover{}).removeBeneath(context.Background(), filepath.Join(tr.dir, "a.txt"), tr.elems)
	if codeOf(err) != codes.Unavailable {
		t.Errorf("a file as the mount: code = %v (%v), want Unavailable", codeOf(err), err)
	}
}

// A kernel without openat2 (before Linux 5.6) removes nothing, and says why.
func TestRemoveBeneathWithoutOpenat2RemovesNothing(t *testing.T) {
	t.Parallel()
	tr := newTree(t)
	r := &treeRemover{open: func(int, string, *unix.OpenHow) (int, error) { return -1, unix.ENOSYS }}
	removed, err := r.removeBeneath(context.Background(), tr.mount, tr.elems)
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
			_, err := r.removeBeneath(context.Background(), tr.mount, tr.elems)
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
	_, err := r.removeBeneath(ctx, tr.mount, tr.elems)
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

	_, err = (&treeRemover{}).removeBeneath(context.Background(), tr.mount, tr.elems)
	if codeOf(err) != codes.Unavailable || !strings.Contains(err.Error(), "nested more than") {
		t.Fatalf("error = %v, want Unavailable about the depth", err)
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
	removed, err := removeItemDirWith(context.Background(), &fake.Mounter{}, r, tr.mount, tr.dir, nameA)
	if codeOf(err) != codes.Unavailable || !strings.Contains(err.Error(), "Linux 5.6") || removed {
		t.Fatalf("removed = %v, error = %v", removed, err)
	}
	mustExist(t, filepath.Join(tr.dir, "a.txt"))

	removed, err = removeItemDirWith(context.Background(), &fake.Mounter{}, &treeRemover{}, tr.mount, tr.dir, nameA)
	if err != nil || !removed {
		t.Fatalf("removed = %v, error = %v", removed, err)
	}
	mustNotExist(t, tr.dir)
	tr.precious(t)
}
