package reconcile

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/mount/fake"
)

// Every sentinel has one class, however it is wrapped.
func TestClassifyEverySentinel(t *testing.T) {
	t.Parallel()
	tests := []struct {
		err  error
		want Class
	}{
		{nil, ClassOK},
		{context.Canceled, ClassCanceled},
		{context.DeadlineExceeded, ClassDeadlineExceeded},
		{ErrTargetOutsideScope, ClassInvalidArgument},
		{ErrStagePathOutsideRoot, ClassInvalidArgument},
		{ErrInvalidRequest, ClassInvalidArgument},
		{ErrItemMissing, ClassNotFound},
		{ErrNoOpenat2, ClassUnavailable},
		{fmt.Errorf("x: %w", ErrNoOpenat2), ClassUnavailable},
		{fmt.Errorf("x: %w", ErrItemMissing), ClassNotFound},
		{ErrUnknownVolume, ClassNotFound},
		{ErrStageConflict, ClassFailedPrecondition},
		{ErrPublishConflict, ClassFailedPrecondition},
		{ErrTransportConflict, ClassFailedPrecondition},
		{ErrItemNotDirectory, ClassFailedPrecondition},
		{mount.ErrConflict, ClassFailedPrecondition},
		{mount.ErrNotResolved, ClassFailedPrecondition},
		{errTargetGone, ClassFailedPrecondition},
		{ErrSuperseded, ClassAborted},
		{mount.ErrTimeout, ClassDeadlineExceeded},
		{ErrIntentsUnknown, ClassUnavailable},
		{ErrTransportUnavailable, ClassUnavailable},
		{mount.ErrBusy, ClassUnavailable},
		{ErrStopped, ClassUnavailable},
		{unix.EBUSY, ClassUnavailable},
		{errors.New("something else"), ClassInternal},
		{fs.ErrNotExist, ClassInternal}, // only ErrItemMissing means NotFound
	}
	for _, tt := range tests {
		if got := Classify(tt.err); got != tt.want {
			t.Errorf("Classify(%v) = %v, want %v", tt.err, got, tt.want)
		}
		if tt.err != nil {
			if got := Classify(fmt.Errorf("volume x: %w", tt.err)); got != tt.want {
				t.Errorf("Classify(wrapped %v) = %v, want %v", tt.err, got, tt.want)
			}
		}
	}
}

// Errors that match two sentinels the table maps differently are resolved by
// the order, and the two that can arise are pinned here.
func TestClassifyMixedErrors(t *testing.T) {
	t.Parallel()
	timeout := realMountTimeout(t)
	// A real mount timeout wraps the context's deadline error as well, which
	// is what makes the order matter: without the rule that the caller's own
	// deadline is one that is not a mount's, every timeout below would be
	// taken for the caller's at step 1.
	if !errors.Is(timeout, mount.ErrTimeout) || !errors.Is(timeout, context.DeadlineExceeded) {
		t.Fatalf("the fixture is not a real mount timeout: %v", timeout)
	}
	// A transport that did not answer within the deadline: unavailable, and a
	// timeout. The timeout is the more specific answer.
	hung := fmt.Errorf("%w: mounting x: %w", ErrTransportUnavailable, timeout)
	if got := Classify(hung); got != ClassDeadlineExceeded {
		t.Errorf("an unavailable transport that timed out = %v, want DeadlineExceeded", got)
	}
	// ...and one that is reported busy because an earlier call is stuck.
	busy := fmt.Errorf("%w: mounting x: %w", ErrTransportUnavailable, mount.ErrBusy)
	if got := Classify(busy); got != ClassUnavailable {
		t.Errorf("an unavailable transport that is busy = %v, want Unavailable", got)
	}
	// A conflict joined with a timeout is the conflict: a person has to act.
	both := errors.Join(fmt.Errorf("%w: at x", ErrStageConflict), timeout)
	if got := Classify(both); got != ClassFailedPrecondition {
		t.Errorf("a conflict joined with a timeout = %v, want FailedPrecondition", got)
	}
	// The caller's own deadline is not a mount timeout, and goes first.
	callers := fmt.Errorf("%w: %w", ErrStageConflict, context.DeadlineExceeded)
	if got := Classify(callers); got != ClassDeadlineExceeded {
		t.Errorf("the caller's deadline over a conflict = %v, want DeadlineExceeded", got)
	}
}

// realMountTimeout is a timeout as the Mounter reports it, with the context's
// deadline error wrapped beside mount.ErrTimeout, from a call that hangs.
func realMountTimeout(t *testing.T) error {
	t.Helper()
	f := &fake.Mounter{DefaultTimeout: 10 * time.Millisecond}
	f.Block("/blocked")
	err := f.Bind(bg, "/source", "/blocked/target", false)
	if err == nil {
		t.Fatal("a blocked bind returned")
	}
	return err
}

// The errors the package actually produces classify as the table says, so no
// path builds a combination the table would get wrong.
func TestTheErrorsThePackageProducesClassifyAsDocumented(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.sim.DefaultTimeout = 30 * time.Millisecond
	e.start()
	a := e.vol("a", pvc1)

	// Request errors.
	bad := a
	bad.VolumeID = pvc1
	outside := a
	outside.StagePath = filepath.Join(e.base, "elsewhere")
	for name, tc := range map[string]struct {
		err  error
		want Class
	}{
		"a malformed volume ID":     {e.r.Stage(bg, bad), ClassInvalidArgument},
		"a stage path outside":      {e.r.Stage(bg, outside), ClassInvalidArgument},
		"a target outside":          {e.r.Publish(bg, a.VolumeID, intent.Publish{TargetPath: e.sRoot}), ClassInvalidArgument},
		"a publish of an unstaged":  {e.r.Publish(bg, "directory:pvc-nope", intent.Publish{TargetPath: e.target("p")}), ClassNotFound},
		"a stage that works":        {e.r.Stage(bg, a), ClassOK},
		"an unpublish that works":   {e.r.Unpublish(bg, a.VolumeID, e.target("q")), ClassOK},
		"an unstage of the unknown": {e.r.Unstage(bg, "directory:pvc-nope"), ClassOK},
	} {
		if got := Classify(tc.err); got != tc.want {
			t.Errorf("%s: %v classifies as %v, want %v", name, tc.err, got, tc.want)
		}
	}

	check := func(what string, err error, want Class) {
		t.Helper()
		if got := Classify(err); got != want {
			t.Errorf("%s: %v classifies as %v, want %v", what, err, got, want)
		}
	}

	// A busy mounter.
	e.sim.FailOp(fake.OpBind, e.target("busy"), mount.ErrBusy)
	e.mkTargetParent("busy")
	check("a busy bind", e.r.Publish(bg, a.VolumeID, intent.Publish{TargetPath: e.target("busy")}), ClassUnavailable)
	e.sim.FailOp(fake.OpBind, e.target("busy"), nil)
	mustNil(t, "unpublish", e.r.Unpublish(bg, a.VolumeID, e.target("busy")))

	// An unmount something still holds open.
	t1 := e.stageAndPublish(a, "pod1", false)
	e.sim.FailOp(fake.OpUnmount, t1, unix.EBUSY)
	check("an unmount that is busy", e.r.Unpublish(bg, a.VolumeID, t1), ClassUnavailable)
	e.sim.FailOp(fake.OpUnmount, t1, nil)

	// A foreign mount where a publish goes, and over the stage of another.
	e.mkTargetParent("foreign")
	e.sim.addMount(e.target("foreign"), "tmpfs", "t")
	check("a publish over a foreign mount", e.r.Publish(bg, a.VolumeID, intent.Publish{TargetPath: e.target("foreign")}), ClassFailedPrecondition)
	b := e.vol("a", pvc2)
	e.sim.addMount(b.StagePath, "tmpfs", "t")
	check("a stage over a foreign mount", e.r.Stage(bg, b), ClassFailedPrecondition)

	// An item that is not a directory. (A missing one is made by the node for a
	// local volume, decision 23, and is the controller's for nfs3: the class of
	// ErrItemMissing is checked above.)
	d := e.vol("a", pvc4)
	mustNil(t, "rm", os.Remove(filepath.Join(e.host("a"), pvc4)))
	mustNil(t, "file", os.WriteFile(filepath.Join(e.host("a"), pvc4), nil, 0o600))
	check("an item that is a file", e.r.Stage(bg, d), ClassFailedPrecondition)

	// A hung transport: a timeout, and then busy while the call is stuck.
	f := e.vol("hung", pvc5)
	e.sim.Block(e.tdir("hung"))
	check("a transport that hangs", e.r.Stage(bg, f), ClassDeadlineExceeded)
	check("the same transport, still stuck", e.r.Stage(bg, f), ClassUnavailable)
	e.sim.Unblock(e.tdir("hung"))

	// An unreadable record.
	e.breakRecord(a.VolumeID)
	check("an unreadable record", e.r.Unpublish(bg, a.VolumeID, t1), ClassUnavailable)
}

// An unstage that cannot unmount stops at the first failing publish, so the
// error that reaches the caller is that one and not a join of several.
func TestAnUnstageFailureIsOneError(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", pvc1)
	t1 := e.stageAndPublish(in, "pod1", false)
	e.mkTargetParent("pod2")
	t2 := e.target("pod2")
	mustNil(t, "Publish", e.r.Publish(bg, in.VolumeID, intent.Publish{TargetPath: t2}))
	e.sim.addMount(t1, "tmpfs", "t") // a foreign mount on the first
	e.sim.DefaultTimeout = 30 * time.Millisecond
	e.sim.Block(t2) // and the second hangs

	err := e.r.Unstage(bg, in.VolumeID)
	if !errors.Is(err, ErrPublishConflict) {
		t.Fatalf("Unstage = %v, want the first publish's conflict", err)
	}
	if errors.Is(err, mount.ErrTimeout) {
		t.Errorf("the error also carries a timeout: %v", err)
	}
	if got := Classify(err); got != ClassFailedPrecondition {
		t.Errorf("classifies as %v, want FailedPrecondition", got)
	}
}

func TestCallerContextsAndStopClassify(t *testing.T) {
	t.Parallel()
	e := newEnv(t) // no loop: a request waits
	in := e.vol("a", pvc1)

	ctx, cancel := context.WithCancel(bg)
	res := make(chan error, 1)
	go func() { res <- e.r.Stage(ctx, in) }()
	waitUntil(t, "the request to queue", func() bool { return e.r.queued() == 1 })
	cancel()
	if got := Classify(<-res); got != ClassCanceled {
		t.Errorf("a cancelled caller = %v", got)
	}

	dctx, dcancel := context.WithTimeout(bg, 20*time.Millisecond)
	defer dcancel()
	if got := Classify(e.r.Stage(dctx, in)); got != ClassDeadlineExceeded {
		t.Errorf("a caller whose deadline passed = %v", got)
	}

	e.r.stop()
	if got := Classify(e.r.Stage(bg, in)); got != ClassUnavailable {
		t.Errorf("a stopped reconciler = %v", got)
	}
}

func TestTheSupersededAndTheClassString(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	in := e.vol("a", pvc1)
	mustNil(t, "save", e.store.Save(in))
	e.pass()
	unstage := &request{kind: reqUnstage, volume: in.VolumeID, done: make(chan error, 1)}
	stage := &request{kind: reqStage, volume: in.VolumeID, done: make(chan error, 1)}
	e.r.runPass(bg, []*request{unstage, stage})
	if got := Classify(<-unstage.done); got != ClassAborted {
		t.Errorf("a superseded unstage = %v", got)
	}
	for c, want := range map[Class]string{ClassInternal: "Internal", ClassOK: "OK", ClassAborted: "Aborted", ClassUnavailable: "Unavailable", Class(99): "Class(99)"} {
		if c.String() != want {
			t.Errorf("%d.String() = %q, want %q", int(c), c.String(), want)
		}
	}
}
