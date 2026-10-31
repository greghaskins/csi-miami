package reconcile

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/greghaskins/csi-miami/internal/intent"
)

// Publish and Unpublish take a path from a request. It must not let an
// explicit drop reach a transport or another volume's stage, nor let a
// publish sit where garbage collection owns the ground.
func TestTargetsOutsideTheDriversScopeAreRefused(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	a := e.vol("a", "pvc-1")
	b := e.vol("a", "pvc-2")
	mustNil(t, "Stage a", e.r.Stage(bg, a))
	mustNil(t, "Stage b", e.r.Stage(bg, b))
	tdir := e.tdir("a")

	tests := []struct {
		name   string
		target string
	}{
		{"the transports root", e.tRoot},
		{"a transport's mount point", tdir},
		{"inside a transport", filepath.Join(tdir, "pvc-1", "x")},
		{"the staging root", e.sRoot},
		{"below the staging root", filepath.Join(e.sRoot, "pod-target")},
		{"another volume's stage path", b.StagePath},
		{"inside another volume's stage path", filepath.Join(b.StagePath, "sub")},
		{"a relative path", "pods/p1/mount"},
		{"an uncleaned path", e.target("pod1") + "/../x"},
		{"an empty path", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := e.sim.table()
			e.sim.Reset()

			err := e.r.Unpublish(bg, "directory:pvc-x", tt.target)
			if !errors.Is(err, ErrTargetOutsideScope) {
				t.Errorf("Unpublish = %v, want ErrTargetOutsideScope", err)
			}
			err = e.r.Publish(bg, a.VolumeID, intent.Publish{TargetPath: tt.target})
			if !errors.Is(err, ErrTargetOutsideScope) {
				t.Errorf("Publish = %v, want ErrTargetOutsideScope", err)
			}
			if c := e.sim.mutating(); len(c) != 0 {
				t.Errorf("a refused request changed mounts: %+v", c)
			}
			if after := e.sim.table(); len(after) != len(before) {
				t.Errorf("the table changed: %d mounts before, %d after", len(before), len(after))
			}
		})
	}
	if got, _ := e.intentOf(a.VolumeID); len(got.Publishes) != 0 {
		t.Errorf("a refused Publish was recorded: %+v", got.Publishes)
	}
}

// Two volumes cannot be bound at one path, and an unpublish for one is not an
// unpublish of the other's.
func TestATargetAnotherVolumeClaimsIsAConflict(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	a := e.vol("a", "pvc-1")
	b := e.vol("a", "pvc-2")
	target := e.stageAndPublish(a, "pod1", false)
	mustNil(t, "Stage b", e.r.Stage(bg, b))
	e.sim.Reset()

	err := e.r.Unpublish(bg, b.VolumeID, target)
	if !errors.Is(err, ErrPublishConflict) {
		t.Fatalf("Unpublish = %v, want ErrPublishConflict", err)
	}
	if want := a.VolumeID; !strings.Contains(err.Error(), want) {
		t.Errorf("the error does not name the claiming volume: %v", err)
	}
	if _, ok := e.visible(target); !ok {
		t.Error("the other volume's target was unmounted")
	}
	if err := e.r.Publish(bg, b.VolumeID, intent.Publish{TargetPath: target}); !errors.Is(err, ErrPublishConflict) {
		t.Errorf("Publish = %v, want ErrPublishConflict", err)
	}
	if got, _ := e.intentOf(b.VolumeID); len(got.Publishes) != 0 {
		t.Errorf("b's record gained %+v", got.Publishes)
	}
	if c := e.sim.mutating(); len(c) != 0 {
		t.Errorf("calls = %+v", c)
	}
	// The claimant itself can still unpublish it.
	mustNil(t, "Unpublish by the owner", e.r.Unpublish(bg, a.VolumeID, target))
	if _, ok := e.visible(target); ok {
		t.Error("the owner's unpublish did not unmount")
	}
}

func TestTargetChecksNeedTheStore(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", "pvc-1")
	mustNil(t, "Stage", e.r.Stage(bg, in))
	mustNil(t, "remove the store", os.RemoveAll(e.store.Dir()))
	if err := e.r.Unpublish(bg, in.VolumeID, e.target("pod1")); !errors.Is(err, ErrIntentsUnknown) {
		t.Errorf("Unpublish = %v, want ErrIntentsUnknown", err)
	}
}
