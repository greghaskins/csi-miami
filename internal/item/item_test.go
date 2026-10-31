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

// stackMounter pops one layer of a stack of mounts per Unmount.
type stackMounter struct {
	*fake.Mounter
	layers int
	path   string
}

func (s *stackMounter) set() {
	t := observe.Table{{ID: 1, Root: "/", MountPoint: "/"}}
	for i := range s.layers {
		t = append(t, observe.Mount{ID: 10 + i, ParentID: 1, Root: "/", MountPoint: s.path})
	}
	s.SetTable(t)
}

func (s *stackMounter) Unmount(ctx context.Context, target string, flags int) error {
	if err := s.Mounter.Unmount(ctx, target, flags); err != nil {
		return err
	}
	s.layers--
	s.set()
	return nil
}

func TestUnmountVisible(t *testing.T) {
	t.Parallel()
	const path = "/stage/x"
	tests := []struct {
		name    string
		layers  int
		failOn  error
		mountsE error
		wantErr bool
		unmount int
	}{
		{"nothing mounted", 0, nil, nil, false, 0},
		{"one layer", 1, nil, nil, false, 1},
		{"a stack is popped", 3, nil, nil, false, 3},
		{"more layers than the bound", maxLayers + 2, nil, nil, true, maxLayers},
		{"a busy mount", 1, mount.ErrBusy, nil, true, 1},
		{"the table cannot be read", 1, nil, errors.New("boom"), true, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := &stackMounter{Mounter: &fake.Mounter{}, layers: tt.layers, path: path}
			s.set()
			if tt.failOn != nil {
				s.FailOp(fake.OpUnmount, path, tt.failOn)
			}
			if tt.mountsE != nil {
				s.SetMountsError(tt.mountsE)
			}
			err := UnmountVisible(context.Background(), s, path)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v", err)
			}
			if tt.failOn != nil && !errors.Is(err, tt.failOn) {
				t.Errorf("err = %v", err)
			}
			n := 0
			for _, c := range s.Calls() {
				if c.Op == fake.OpUnmount {
					n++
					if c.Flags != 0 {
						t.Errorf("a lazy unmount: %+v", c)
					}
				}
			}
			if tt.failOn == nil && n != tt.unmount {
				t.Errorf("%d unmounts, want %d", n, tt.unmount)
			}
		})
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
