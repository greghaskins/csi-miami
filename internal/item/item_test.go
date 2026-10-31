package item

import (
	"context"
	"errors"
	"io/fs"
	"testing"

	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/mount/fake"
	"github.com/greghaskins/csi-miami/internal/observe"
)

const (
	mountPoint = "/stage/x"
	source     = "/export/data/pvc-1"
)

var errConflict = errors.New("test conflict")

func rootMount() observe.Mount {
	return observe.Mount{ID: 1, Major: 8, Minor: 1, Root: "/", MountPoint: "/", FSType: "ext4", Source: "/dev/sda1"}
}

// bindOf is a mount at mountPoint of the given root on the root file system.
func bindOf(id int, root string) observe.Mount {
	return observe.Mount{ID: id, ParentID: 1, Major: 8, Minor: 1, Root: root, MountPoint: mountPoint, FSType: "ext4", Source: "/dev/sda1"}
}

// popper removes the top mount at a path per Unmount, like the kernel, and
// records what it was asked to remove.
type popper struct {
	*fake.Mounter
	table observe.Table
}

func newPopper(ms ...observe.Mount) *popper {
	p := &popper{Mounter: &fake.Mounter{}, table: ms}
	p.SetTable(p.table)
	return p
}

func (p *popper) Unmount(ctx context.Context, target string, flags int) error {
	if err := p.Mounter.Unmount(ctx, target, flags); err != nil {
		return err
	}
	top, ok := p.table.At(target)
	if !ok {
		return nil
	}
	var keep observe.Table
	for _, m := range p.table {
		if m.ID != top.ID {
			keep = append(keep, m)
		}
	}
	p.table = keep
	p.SetTable(keep)
	return nil
}

func (p *popper) unmounts() int {
	n := 0
	for _, c := range p.Calls() {
		if c.Op == fake.OpUnmount {
			n++
			if c.Flags != 0 {
				panic("a lazy unmount")
			}
		}
	}
	return n
}

func TestUnmountBind(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		table    []observe.Mount
		failOn   error
		want     error // nil: success
		unmounts int
		left     int // mounts left at mountPoint
	}{
		{"nothing mounted", []observe.Mount{rootMount()}, nil, nil, 0, 0},
		{"our bind", []observe.Mount{rootMount(), bindOf(2, source)}, nil, nil, 1, 0},
		{"a bind of another directory is not ours", []observe.Mount{rootMount(), bindOf(2, "/export/data/pvc-2")}, nil, errConflict, 0, 1},
		{"a foreign file system is not ours", []observe.Mount{rootMount(), {ID: 2, ParentID: 1, Major: 0, Minor: 40, Root: "/", MountPoint: mountPoint, FSType: "tmpfs", Source: "t"}}, nil, errConflict, 0, 1},
		// Our bind with another mount stacked on it: the top is not ours, so
		// nothing is removed and the stack stays.
		{"something stacked on ours", []observe.Mount{rootMount(), bindOf(2, source), {ID: 3, ParentID: 2, Major: 0, Minor: 41, Root: "/", MountPoint: mountPoint, FSType: "tmpfs", Source: "t"}}, nil, errConflict, 0, 2},
		// Ours with a copy of ours beneath it: one layer goes, and the one
		// left is reported, not popped.
		{"our bind stacked twice", []observe.Mount{rootMount(), bindOf(2, source), bindOf(3, source)}, nil, errConflict, 1, 1},
		{"a busy mount", []observe.Mount{rootMount(), bindOf(2, source)}, mount.ErrBusy, mount.ErrBusy, 1, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := newPopper(tt.table...)
			if tt.failOn != nil {
				p.FailOp(fake.OpUnmount, mountPoint, tt.failOn)
			}
			err := UnmountBind(context.Background(), p, mountPoint, source, errConflict)
			if tt.want == nil && err != nil || tt.want != nil && !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
			if got := p.unmounts(); got != tt.unmounts {
				t.Errorf("%d unmounts, want %d", got, tt.unmounts)
			}
			n := 0
			for _, m := range p.table {
				if m.MountPoint == mountPoint {
					n++
				}
			}
			if n != tt.left {
				t.Errorf("%d mounts left at the path, want %d", n, tt.left)
			}
		})
	}
}

func TestUnmountBindErrors(t *testing.T) {
	t.Parallel()
	p := newPopper(rootMount(), bindOf(2, source))
	p.SetMountsError(errors.New("boom"))
	if err := UnmountBind(context.Background(), p, mountPoint, source, errConflict); err == nil {
		t.Error("an unreadable table must be an error")
	}
	// No mount contains the source: the table cannot say, so it is an error
	// and nothing is unmounted.
	p = newPopper(bindOf(2, source))
	if err := UnmountBind(context.Background(), p, mountPoint, source, errConflict); err == nil || errors.Is(err, errConflict) {
		t.Errorf("got %v, want an error that is not a conflict", err)
	}
	if p.unmounts() != 0 {
		t.Error("unmounted without being able to tell what it was")
	}
}

func TestSentinels(t *testing.T) {
	t.Parallel()
	if !errors.Is(ErrItemMissing, fs.ErrNotExist) {
		t.Error("ErrItemMissing must wrap fs.ErrNotExist")
	}
	if !errors.Is(ErrStageConflict, mount.ErrConflict) {
		t.Error("ErrStageConflict must wrap mount.ErrConflict")
	}
}
