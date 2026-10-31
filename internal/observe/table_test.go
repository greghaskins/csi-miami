package observe

import (
	"os"
	"reflect"
	"testing"
)

func ids(t Table) []int {
	out := make([]int, len(t))
	for i, m := range t {
		out[i] = m.ID
	}
	return out
}

// The fixture is built like a real table: an overmount's parent is the mount
// it covers; mount 59 covers mount 60 and has the lower ID, as happens when
// the kernel reuses a freed ID; and mount 72 was made on mount 71 before
// mount 73 covered 71, so 72's mount point is hidden.

func TestAt(t *testing.T) {
	t.Parallel()
	tbl := loadFixture(t)
	tests := []struct {
		name   string
		path   string
		wantID int // 0: not found
	}{
		{"plain nfs", "/mnt/nfs", 30},
		{"a bind", "/mnt/stage/pvc-1", 31},
		{"unclean path is cleaned", "/mnt/stage//pvc-1/", 31},
		{"escaped name decoded", "/mnt/publish/my pod", 35},
		{"stacked tmpfs: the top of the stack", "/mnt/scratch", 37},
		{"tmpfs over a bind hides it", "/mnt/stage/pvc-2", 41},
		{"overmount with the lower ID is the top", "/mnt/reuse", 59},
		{"tmpfs over a transport", "/mnt/hid", 73},
		{"mount point hidden by an overmount of its parent", "/mnt/hid/vols", 0},
		{"mount made on top of a stack", "/mnt/scratch/inner", 75},
		{"below a mount is not a mount", "/mnt/nfs/vols", 0},
		{"prefix of a mount point is not a mount", "/mnt/stage", 0},
		{"root", "/", 20},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m, ok := tbl.At(tt.path)
			if ok != (tt.wantID != 0) || m.ID != tt.wantID {
				t.Errorf("At(%q) = ID %d, %v; want ID %d", tt.path, m.ID, ok, tt.wantID)
			}
		})
	}
}

func TestResolve(t *testing.T) {
	t.Parallel()
	tbl := loadFixture(t)
	tests := []struct {
		name   string
		path   string
		wantID int
	}{
		{"inside a mount", "/mnt/nfs/vols/pvc-1", 30},
		{"inside a stack goes to its top", "/mnt/scratch/file", 37},
		{"inside a mount made on a stack", "/mnt/scratch/inner/x", 75},
		{"through a covered mount point goes to the cover", "/mnt/hid/vols/x", 73},
		{"through a covered mount point itself", "/mnt/hid/vols", 73},
		{"through the cover with the lower ID", "/mnt/reuse/x", 59},
		{"outside every mount but the root", "/nonexistent/x", 20},
		{"a longer name sharing a string prefix", "/mnt/nfsx", 20},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m, ok := tbl.Resolve(tt.path)
			if !ok || m.ID != tt.wantID {
				t.Errorf("Resolve(%q) = ID %d, %v; want ID %d", tt.path, m.ID, ok, tt.wantID)
			}
		})
	}

	t.Run("empty table", func(t *testing.T) {
		t.Parallel()
		if m, ok := (Table{}).Resolve("/x"); ok {
			t.Errorf("Resolve on an empty table = %v, true", m)
		}
	})
	t.Run("no mount contains the path", func(t *testing.T) {
		t.Parallel()
		if m, ok := (Table{{ID: 1, ParentID: 0, MountPoint: "/elsewhere"}}).Resolve("/x"); ok {
			t.Errorf("Resolve = %v, true", m)
		}
	})
	t.Run("a cyclic table terminates", func(t *testing.T) {
		t.Parallel()
		cyc := Table{
			{ID: 1, ParentID: 0, MountPoint: "/"},
			{ID: 2, ParentID: 3, MountPoint: "/a"},
			{ID: 3, ParentID: 2, MountPoint: "/a"},
			{ID: 4, ParentID: 4, MountPoint: "/b"}, // its own parent: a root
		}
		if m, ok := cyc.Resolve("/a/x"); !ok || m.ID != 1 {
			t.Errorf("Resolve(/a/x) = %v, %v; want the root", m.ID, ok)
		}
		if m, ok := cyc.Resolve("/b/x"); !ok || m.ID != 4 {
			t.Errorf("Resolve(/b/x) = %v, %v; want mount 4", m.ID, ok)
		}
		loop := Table{
			{ID: 1, ParentID: 1, MountPoint: "/"},
			{ID: 2, ParentID: 1, MountPoint: "/"},
			{ID: 3, ParentID: 2, MountPoint: "/"},
			{ID: 2, ParentID: 3, MountPoint: "/"},
		}
		_, _ = loop.Resolve("/") // must return
	})
}

func TestUnderAndBelow(t *testing.T) {
	t.Parallel()
	tbl := loadFixture(t)
	tests := []struct {
		name   string
		prefix string
		under  []int
		below  []int
	}{
		{"one level, newest first", "/mnt/publish", []int{35, 34, 33, 32}, []int{35, 34, 33, 32}},
		{"stage: the cover first, then what it covers", "/mnt/stage", []int{41, 40, 31}, []int{41, 40, 31}},
		{"scratch: a stack and a mount made on it", "/mnt/scratch", []int{75, 37, 36}, []int{75}},
		{"reuse: the cover is first although its ID is lower", "/mnt/reuse", []int{59, 60}, nil},
		{"hid: hidden mounts are included", "/mnt/hid", []int{73, 72, 71}, []int{72}},
		{"a single mount point", "/mnt/nfs", []int{30}, nil},
		{"nothing there", "/nonexistent", nil, nil},
		{"a name that only shares a string prefix", "/mnt/stage/pvc", nil, nil},
		{"unclean prefix", "/mnt//publish/", []int{35, 34, 33, 32}, []int{35, 34, 33, 32}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := ids(tbl.Under(tt.prefix)); !(len(got) == 0 && len(tt.under) == 0) && !reflect.DeepEqual(got, tt.under) {
				t.Errorf("Under(%q) = %v, want %v", tt.prefix, got, tt.under)
			}
			got := ids(tbl.Below(tt.prefix))
			if !(len(got) == 0 && len(tt.below) == 0) && !reflect.DeepEqual(got, tt.below) {
				t.Errorf("Below(%q) = %v, want %v", tt.prefix, got, tt.below)
			}
		})
	}

	t.Run("a directory with mounts under it, all of them newest first", func(t *testing.T) {
		t.Parallel()
		got := ids(tbl.Under("/mnt"))
		want := []int{75, 73, 72, 71, 59, 60, 41, 40, 38, 37, 36, 35, 34, 33, 32, 31, 30}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("Under(/mnt) = %v, want %v", got, want)
		}
	})

	t.Run("root: everything, newest first, the root mount last", func(t *testing.T) {
		t.Parallel()
		got := tbl.Under("/")
		if len(got) != len(tbl) {
			t.Fatalf("Under(/) has %d mounts, want %d", len(got), len(tbl))
		}
		if got[0].ID != 75 || got[len(got)-1].ID != 20 {
			t.Errorf("Under(/) runs from %d to %d, want 75 to 20", got[0].ID, got[len(got)-1].ID)
		}
		if below := tbl.Below("/"); len(below) != len(tbl)-1 {
			t.Errorf("Below(/) has %d mounts, want %d: all but the root mount", len(below), len(tbl)-1)
		}
	})
}

func TestIsBindOf(t *testing.T) {
	t.Parallel()
	tbl := loadFixture(t)
	tests := []struct {
		name       string
		mountPoint string
		source     string
		want       bool
	}{
		{"bind of a subdirectory of the nfs mount", "/mnt/stage/pvc-1", "/mnt/nfs/vols/pvc-1", true},
		{"bind of the bind", "/mnt/publish/pod-a", "/mnt/stage/pvc-1", true},
		{"bind of the bind answers for the original directory too", "/mnt/publish/pod-a", "/mnt/nfs/vols/pvc-1", true},
		{"read-only remounted bind is still a bind of the same dir", "/mnt/publish/pod-b", "/mnt/stage/pvc-1", true},
		{"a bind of a sibling directory is not a bind of this one", "/mnt/publish/pod-c", "/mnt/stage/pvc-1", false},
		{"sibling directory as the source", "/mnt/stage/pvc-1", "/mnt/nfs/vols/pvc-2", false},
		{"parent directory as the source", "/mnt/stage/pvc-1", "/mnt/nfs/vols", false},
		{"child directory as the source", "/mnt/stage/pvc-1", "/mnt/nfs/vols/pvc-1/sub", false},
		{"names with spaces", "/mnt/publish/my pod", "/mnt/nfs/vols/my volume", true},
		{"unclean paths are cleaned", "/mnt/publish//pod-a/", "/mnt/nfs/vols/../vols/pvc-1/", true},
		{"nothing mounted at the mount point", "/mnt/publish/pod-z", "/mnt/stage/pvc-1", false},
		{"tmpfs over the bind hides it", "/mnt/stage/pvc-2", "/mnt/nfs/vols/pvc-2", false},
		{"overmount with the lower ID: the top is what counts", "/mnt/reuse", "/mnt/nfs/vols/b", true},
		{"overmount with the lower ID: the covered bind is not it", "/mnt/reuse", "/mnt/nfs/vols/a", false},
		{"different device with the same root", "/mnt/extra", "/mnt/nfs", false},
		{"the mount containing the source is not a bind of it", "/mnt/nfs", "/mnt/nfs", false},
		{"a source under a covered mount point is in the cover", "/mnt/hid", "/mnt/hid/vols", false},
		{"a hidden mount point is not a mount point", "/mnt/hid/vols", "/mnt/nfs", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := tbl.IsBindOf(tt.mountPoint, tt.source)
			if err != nil {
				t.Fatalf("IsBindOf(%q, %q): %v", tt.mountPoint, tt.source, err)
			}
			if got != tt.want {
				t.Errorf("IsBindOf(%q, %q) = %v, want %v", tt.mountPoint, tt.source, got, tt.want)
			}
		})
	}
}

func TestIsBindOfContainerIsWhatTheKernelResolves(t *testing.T) {
	t.Parallel()
	tbl := loadFixture(t)
	bind := func(id, minor int, root, mp string) Mount {
		return Mount{
			ID: id, ParentID: 20, Major: 0, Minor: minor, Root: root, MountPoint: mp,
			Options: []string{"rw"}, FSType: "tmpfs", Source: "tmpfs",
		}
	}
	tbl = append(tbl,
		// /mnt/scratch is a stack: 36 (device 0:52) under 37 (device 0:53).
		// A directory in it lives in the top, 37.
		bind(80, 53, "/sub", "/mnt/bound-top"),
		bind(81, 52, "/sub", "/mnt/bound-covered"),
		// A directory below /mnt/scratch/inner lives in mount 75 (0:56), made
		// on top of the stack.
		bind(82, 56, "/d", "/mnt/bound-inner"),
		bind(83, 53, "/inner/d", "/mnt/bound-wrong"),
	)
	tests := []struct {
		mountPoint, source string
		want               bool
	}{
		{"/mnt/bound-top", "/mnt/scratch/sub", true},
		{"/mnt/bound-covered", "/mnt/scratch/sub", false},
		{"/mnt/bound-inner", "/mnt/scratch/inner/d", true},
		{"/mnt/bound-wrong", "/mnt/scratch/inner/d", false},
	}
	for _, tt := range tests {
		got, err := tbl.IsBindOf(tt.mountPoint, tt.source)
		if err != nil || got != tt.want {
			t.Errorf("IsBindOf(%s, %s) = %v, %v; want %v", tt.mountPoint, tt.source, got, err, tt.want)
		}
	}
}

func TestIsBindOfDeletedRoot(t *testing.T) {
	t.Parallel()
	tbl := loadFixture(t)
	f, err := os.Open("testdata/mountinfo_odd.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	odd, err := Parse(f)
	if err != nil {
		t.Fatal(err)
	}
	tbl = append(tbl, odd...)

	// Mount 94 is a bind of /vols/gone whose directory has been removed.
	got, err := tbl.IsBindOf("/mnt/gone", "/mnt/nfs/vols/gone")
	if err != nil || got {
		t.Errorf("deleted root: IsBindOf = %v, %v; want false", got, err)
	}
	for i := range tbl {
		if tbl[i].ID == 94 {
			tbl[i].Deleted = false
		}
	}
	got, err = tbl.IsBindOf("/mnt/gone", "/mnt/nfs/vols/gone")
	if err != nil || !got {
		t.Errorf("same mount, not deleted: IsBindOf = %v, %v; want true", got, err)
	}
}

func TestIsBindOfErrors(t *testing.T) {
	t.Parallel()
	tbl := loadFixture(t)
	if _, err := tbl.IsBindOf("mnt/stage", "/mnt/nfs"); err == nil {
		t.Error("relative mount point: want an error")
	}
	if _, err := tbl.IsBindOf("/mnt/stage/pvc-1", "mnt/nfs"); err == nil {
		t.Error("relative source: want an error")
	}
	// A table with no root mount cannot say where /mnt/nfs/x lives.
	empty := Table{{ID: 1, MountPoint: "/elsewhere"}}
	if _, err := empty.IsBindOf("/elsewhere", "/mnt/nfs/x"); err == nil {
		t.Error("source contained by no mount: want an error")
	}
}
