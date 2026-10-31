package reconcile

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
		{"the directory above the transports root", filepath.Dir(e.tRoot)},
		{"the directory above the staging root", filepath.Dir(e.sRoot)},
		{"the root of the file system", "/"},
		{"the directory above another volume's stage path", filepath.Dir(b.StagePath)},
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

// The directory above a root is refused because it contains the root, not
// because it happens to contain a stage path. With the two roots on separate
// branches and no records at all, nothing but the root rule can refuse the
// parent of either, or the one above that.
func TestTheDirectoryAboveEitherRootIsRefusedWithNoRecords(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	tr := filepath.Join(e.base, "branch-a", "x", "miami")
	sr := filepath.Join(e.base, "branch-b", "y", "stage")
	r, err := New(e.store, e.sim, Options{TransportsRoot: tr, StageRoot: sr})
	mustNil(t, "New", err)

	// No loop runs here, so a request that is not refused would wait for a
	// pass for ever; the deadline turns that into a failure.
	ctx, cancel := context.WithTimeout(bg, 50*time.Millisecond)
	defer cancel()

	for name, target := range map[string]string{
		"the parent of the transports root":      filepath.Dir(tr),
		"the grandparent of the transports root": filepath.Dir(filepath.Dir(tr)),
		"the parent of the staging root":         filepath.Dir(sr),
		"the grandparent of the staging root":    filepath.Dir(filepath.Dir(sr)),
		"the common ancestor":                    e.base,
	} {
		for op, err := range map[string]error{
			"Unpublish": r.Unpublish(ctx, "directory:pvc-x", target),
			"Publish":   r.Publish(ctx, "directory:pvc-x", intent.Publish{TargetPath: target}),
		} {
			if !errors.Is(err, ErrTargetOutsideScope) {
				t.Errorf("%s of %s: %v, want ErrTargetOutsideScope", op, name, err)
			}
		}
	}
	// A sibling of the roots is none of that: it is accepted, and then waits
	// for a pass, so the deadline ends it.
	if err := r.Unpublish(ctx, "directory:pvc-x", filepath.Join(e.base, "branch-a", "pods", "p1")); errors.Is(err, ErrTargetOutsideScope) || !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("a path beside the roots: %v, want it accepted and waiting", err)
	}
}
