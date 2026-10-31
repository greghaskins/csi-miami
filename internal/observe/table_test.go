package observe

import (
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
		{"stacked tmpfs: highest ID wins", "/mnt/scratch", 37},
		{"tmpfs over a bind hides it", "/mnt/stage/pvc-2", 41},
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

func TestUnder(t *testing.T) {
	t.Parallel()
	tbl := loadFixture(t)
	tests := []struct {
		name   string
		prefix string
		want   []int
	}{
		{"publish dir, one level, highest ID first", "/mnt/publish", []int{35, 34, 33, 32}},
		{"stage dir, stacked top-most first", "/mnt/stage", []int{41, 40, 31}},
		{"scratch, stacked", "/mnt/scratch", []int{37, 36}},
		{"a single mount point itself", "/mnt/nfs", []int{30}},
		{"nothing there", "/nonexistent", nil},
		{"a name that only shares a string prefix", "/mnt/stage/pvc", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := ids(tbl.Under(tt.prefix))
			if len(got) == 0 && len(tt.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Under(%q) = %v, want %v", tt.prefix, got, tt.want)
			}
		})
	}

	t.Run("deepest first across levels", func(t *testing.T) {
		t.Parallel()
		got := ids(tbl.Under("/mnt"))
		// Depth 3: stage/pvc-1, stage/pvc-2 x2, publish/* x4; depth 2: nfs,
		// scratch x2, extra.
		want := []int{41, 40, 35, 34, 33, 32, 31, 38, 37, 36, 30}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("Under(/mnt) = %v, want %v", got, want)
		}
	})

	t.Run("root includes everything, root last", func(t *testing.T) {
		t.Parallel()
		got := tbl.Under("/")
		if len(got) != len(tbl) {
			t.Fatalf("Under(/) has %d mounts, want %d", len(got), len(tbl))
		}
		if got[len(got)-1].ID != 20 {
			t.Errorf("last mount under / is ID %d, want the root mount 20", got[len(got)-1].ID)
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
		{"tmpfs stacked over the bind hides it", "/mnt/stage/pvc-2", "/mnt/nfs/vols/pvc-2", false},
		{"different device with the same root", "/mnt/extra", "/mnt/nfs", false},
		{"the mount containing the source is not a bind of it", "/mnt/nfs", "/mnt/nfs", false},
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

func TestIsBindOfContainerIsTheTopMostAtTheLongestPrefix(t *testing.T) {
	t.Parallel()
	tbl := loadFixture(t)
	// A directory inside the stacked tmpfs at /mnt/scratch belongs to the
	// top-most tmpfs (ID 37, device 0:53), not to the one under it or to /.
	tbl = append(tbl, Mount{
		ID: 50, ParentID: 37, Major: 0, Minor: 53, Root: "/inner", MountPoint: "/mnt/bound",
		Options: []string{"rw"}, FSType: "tmpfs", Source: "tmpfs",
	})
	got, err := tbl.IsBindOf("/mnt/bound", "/mnt/scratch/inner")
	if err != nil || !got {
		t.Errorf("IsBindOf(/mnt/bound, /mnt/scratch/inner) = %v, %v; want true", got, err)
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
