package reconcile

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/item"
	"github.com/greghaskins/csi-miami/internal/item/directory"
	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/mount/fake"
)

// statErr, when set, is what a stat of an item directory returns; the item
// otherwise looks at the host directory the transport stands for.
type statErr struct{ err atomic.Pointer[error] }

func (s *statErr) set(err error) {
	if err == nil {
		s.err.Store(nil)
		return
	}
	s.err.Store(&err)
}

// withStatErr gives the environment a directory item whose stat can be made to
// fail with a chosen error, as a stat on a restarted NFS server does.
func (e *env) withStatErr(t *testing.T) *statErr {
	t.Helper()
	s := &statErr{}
	d, err := directory.New(directory.Config{
		Root: e.tRoot, Mounter: e.sim, Timeout: 2 * time.Second,
		Stat: func(p string) (fs.FileInfo, error) {
			if perr := s.err.Load(); perr != nil {
				return nil, &fs.PathError{Op: "stat", Path: p, Err: *perr}
			}
			return os.Stat(e.hostPath(p))
		},
	})
	mustNil(t, "item", err)
	e.r = e.newReconciler(func(o *Options) { o.Items = []item.Item{d} })
	return s
}

// A stat of the item directory that returns ESTALE is the transport's
// condition: unavailable and retryable, an event under the transport's reason,
// and nothing unmounted or mounted. The next pass looks again and the volume
// recovers.
func TestAStaleItemDirectoryIsTheTransportsCondition(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	stat := e.withStatErr(t)
	in := e.vol("a", pvc1)
	e.mkTargetParent("pod1")
	target := e.target("pod1")
	in.Publishes = []intent.Publish{{TargetPath: target}}
	mustNil(t, "save", e.store.Save(in))
	mustNil(t, "healthy", e.converge(in.VolumeID))
	if !e.isBind(target, e.srcOf(in)) {
		t.Fatal("the publish is not bound")
	}
	if got := e.metrics.conditionCount(ConditionOK); got != 1 {
		t.Fatalf("ok volumes = %d, want 1", got)
	}
	before := len(e.sim.mutating())

	stat.set(syscall.ESTALE)
	err := e.converge(in.VolumeID)
	if !errors.Is(err, ErrTransportUnavailable) || !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("got %v, want ErrTransportUnavailable wrapping ESTALE", err)
	}
	if got := Classify(err); got != ClassUnavailable {
		t.Errorf("Classify = %v, want Unavailable (never Internal)", got)
	}
	if evs := e.events.reasons(ReasonTransportUnavailable); len(evs) != 1 || evs[0].Volume != in.VolumeID {
		t.Errorf("TransportUnavailable events = %+v, want one for the volume", evs)
	}
	if evs := e.events.reasons(ReasonMountFailed); len(evs) != 0 {
		t.Errorf("MountFailed events = %+v, want none", evs)
	}
	if got := e.metrics.conditionCount(ConditionTransportUnavailable); got != 1 {
		t.Errorf("transport-unavailable volumes = %d, want 1", got)
	}
	if got := e.metrics.conditionCount(ConditionOK); got != 0 {
		t.Errorf("ok volumes = %d, want 0", got)
	}
	// Nothing was unmounted, remounted or rebound, and the publish stands.
	if after := e.sim.mutating(); len(after) != before {
		t.Errorf("calls that change mounts during the outage: %+v", after[before:])
	}
	if !e.isBind(target, e.srcOf(in)) {
		t.Error("the publish bind was removed")
	}

	// It stays one event while the condition lasts, and the next pass retries.
	for range 3 {
		_ = e.converge(in.VolumeID)
	}
	if evs := e.events.reasons(ReasonTransportUnavailable); len(evs) != 1 {
		t.Errorf("%d events over four passes of one outage, want 1", len(evs))
	}

	stat.set(nil)
	mustNil(t, "recovered", e.converge(in.VolumeID))
	if got := e.metrics.conditionCount(ConditionOK); got != 1 {
		t.Errorf("ok volumes after recovery = %d, want 1", got)
	}
	if got := e.metrics.conditionCount(ConditionTransportUnavailable); got != 0 {
		t.Errorf("transport-unavailable volumes after recovery = %d, want 0", got)
	}
	if after := e.sim.mutating(); len(after) != before {
		t.Errorf("calls that change mounts across the outage: %+v", after[before:])
	}
}

// A timeout or a busy stat of the item directory is the same condition, as is
// any other way the transport can fail to answer; an error that is not one of
// those (permission) is not.
func TestItemStatFailuresThatAreTheTransportsCondition(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		err       error
		transport bool
		class     Class
	}{
		{"stale handle", syscall.ESTALE, true, ClassUnavailable},
		{"timeout", fmt.Errorf("stat: %w", mount.ErrTimeout), true, ClassDeadlineExceeded},
		{"busy", fmt.Errorf("stat: %w", mount.ErrBusy), true, ClassUnavailable},
		{"permission", os.ErrPermission, false, ClassInternal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			stat := e.withStatErr(t)
			in := e.vol("a", pvc1)
			mustNil(t, "save", e.store.Save(in))
			stat.set(tt.err)
			err := e.converge(in.VolumeID)
			if got := errors.Is(err, ErrTransportUnavailable); got != tt.transport {
				t.Errorf("ErrTransportUnavailable = %v, want %v (%v)", got, tt.transport, err)
			}
			if got := Classify(err); got != tt.class {
				t.Errorf("Classify = %v, want %v (%v)", got, tt.class, err)
			}
			wantReason := ReasonMountFailed
			wantCondition := ConditionFailed
			if tt.transport {
				wantReason, wantCondition = ReasonTransportUnavailable, ConditionTransportUnavailable
			}
			if evs := e.events.reasons(wantReason); len(evs) != 1 {
				t.Errorf("%s events = %+v, want one", wantReason, e.events.all())
			}
			if got := e.metrics.conditionCount(wantCondition); got != 1 {
				t.Errorf("%s volumes = %d, want 1", wantCondition, got)
			}
			if calls := e.sim.mutating(); len(calls) != 1 || calls[0].Op != fake.OpBind {
				t.Errorf("calls that change mounts = %+v, want only the transport's own mount (a local transport is a bind)", calls)
			}
		})
	}
}

// Every pass probes each transport the intents reference, once: 1 when the
// statfs answers, 0 when it fails, and a change is logged once.
func TestTransportUpFollowsTheProbe(t *testing.T) {
	t.Parallel()
	lg, buf := logger()
	e := newEnv(t, func(o *Options) { o.Logger = lg })
	in := e.vol("a", pvc1)
	in2 := e.vol("a", pvc2)
	mustNil(t, "save", e.store.Save(in))
	mustNil(t, "save", e.store.Save(in2))
	spec := in.Transport
	tdir := e.tdir("a")
	statfsCalls := func() int {
		n := 0
		for _, c := range e.sim.Calls() {
			if c.Op == fake.OpStatfs && c.Target == tdir {
				n++
			}
		}
		return n
	}

	e.pass()
	if up, ok := e.metrics.upOf(spec.Kind, spec.ID()); !ok || !up {
		t.Fatalf("up = %v (reported %v), want 1 once the transport is mounted and answers", up, ok)
	}
	if n := statfsCalls(); n != 1 {
		t.Errorf("%d statfs of the transport in a pass with two volumes on it, want 1", n)
	}
	if strings.Contains(buf.String(), "stopped answering") || strings.Contains(buf.String(), "answers again") {
		t.Errorf("a healthy transport logged a transition:\n%s", buf.String())
	}

	// It stops answering with an error that is not a timeout.
	e.sim.FailOp(fake.OpStatfs, tdir, syscall.ESTALE)
	for range 3 {
		e.pass()
	}
	if up, _ := e.metrics.upOf(spec.Kind, spec.ID()); up {
		t.Error("up = 1 while statfs fails")
	}
	if n := strings.Count(buf.String(), "transport stopped answering"); n != 1 {
		t.Errorf("%d down lines over three failing passes, want 1:\n%s", n, buf.String())
	}
	if e.metrics.errCount("probe") != 3 {
		t.Errorf("probe errors = %d, want 3", e.metrics.errCount("probe"))
	}

	e.sim.FailOp(fake.OpStatfs, tdir, nil)
	for range 3 {
		e.pass()
	}
	if up, _ := e.metrics.upOf(spec.Kind, spec.ID()); !up {
		t.Error("up = 0 after statfs answers again")
	}
	if n := strings.Count(buf.String(), "transport answers again"); n != 1 {
		t.Errorf("%d up lines over three healthy passes, want 1:\n%s", n, buf.String())
	}
	if n := strings.Count(buf.String(), "transport stopped answering"); n != 1 {
		t.Errorf("%d down lines in all, want 1", n)
	}
}

// A statfs that hangs costs one deadline per transport per pass: the volumes
// on the transport wait for the next pass, and the abandoned call is not
// repeated while it has not returned (busy).
func TestAHungProbeCostsOneDeadlinePerTransportPerPass(t *testing.T) {
	t.Parallel()
	e := newEnv(t, func(o *Options) { o.StatTimeout = 30 * time.Millisecond })
	e.sim.DefaultTimeout = 30 * time.Millisecond
	a1, a2, b1 := e.vol("a", pvc1), e.vol("a", pvc2), e.vol("b", pvc3)
	for _, in := range []intent.Intent{a1, a2, b1} {
		mustNil(t, "save", e.store.Save(in))
	}
	e.pass() // mounts both transports
	tdirA := e.tdir("a")
	e.sim.Block(tdirA)

	first := e.converge(a1.VolumeID)
	upA, _ := e.metrics.upOf(a1.Transport.Kind, a1.Transport.ID())
	upB, _ := e.metrics.upOf(b1.Transport.Kind, b1.Transport.ID())
	if upA || !upB {
		t.Errorf("up of the hung transport = %v, of the healthy one = %v; want 0 and 1", upA, upB)
	}
	statfsOfA := 0
	for _, c := range e.sim.Calls() {
		if c.Op == fake.OpStatfs && c.Target == tdirA {
			statfsOfA++
		}
	}
	if statfsOfA != 2 {
		t.Errorf("%d statfs of the hung transport over two passes, want 2 (one each)", statfsOfA)
	}
	if !errors.Is(first, ErrTransportUnavailable) || !errors.Is(first, mount.ErrTimeout) {
		t.Errorf("the volume on the hung transport: %v", first)
	}
	if e.metrics.conditionCount(ConditionTransportUnavailable) != 2 || e.metrics.conditionCount(ConditionOK) != 1 {
		t.Errorf("conditions = %v, want 2 transport-unavailable and 1 ok", e.metrics.conditions)
	}

	// While the abandoned call has not returned the next pass is refused
	// instead of stacking another.
	if again := e.converge(a1.VolumeID); !errors.Is(again, mount.ErrBusy) {
		t.Errorf("the pass after: %v, want busy", again)
	}
	if up, _ := e.metrics.upOf(a1.Transport.Kind, a1.Transport.ID()); up {
		t.Error("up = 1 while the probe is held busy")
	}

	e.sim.Unblock(tdirA)
	e.pass()
	if up, _ := e.metrics.upOf(a1.Transport.Kind, a1.Transport.ID()); !up {
		t.Error("up = 0 after the transport answers again")
	}
	if e.metrics.conditionCount(ConditionOK) != 3 {
		t.Errorf("conditions after recovery = %v, want 3 ok", e.metrics.conditions)
	}
}

// A transport that is not mounted is not up, and is not reported as having
// stopped answering.
func TestAnUnmountedTransportIsNotUp(t *testing.T) {
	t.Parallel()
	lg, buf := logger()
	e := newEnv(t, func(o *Options) { o.Logger = lg })
	in := e.vol("a", pvc1)
	mustNil(t, "save", e.store.Save(in))
	e.sim.FailOp(fake.OpBind, e.tdir("a"), syscall.ECONNREFUSED)
	e.pass()
	if up, reported := e.metrics.upOf(in.Transport.Kind, in.Transport.ID()); !reported || up {
		t.Errorf("up = %v (reported %v), want reported as 0", up, reported)
	}
	if strings.Contains(buf.String(), "stopped answering") {
		t.Errorf("an unmounted transport logged that it stopped answering:\n%s", buf.String())
	}
	for _, c := range e.sim.Calls() {
		if c.Op == fake.OpStatfs {
			t.Errorf("statfs of a transport that is not mounted: %+v", c)
		}
	}
}

// The volume condition gauge counts the volumes of the latest pass in each
// condition, with every condition present.
func TestVolumeConditionsAreCountedAfterEveryPass(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ok1, ok2, missing := e.vol("a", pvc1), e.vol("a", pvc2), e.vol("b", pvc3)
	conflict := e.vol("c", pvc4)
	for _, in := range []intent.Intent{ok1, ok2, missing, conflict} {
		mustNil(t, "save", e.store.Save(in))
	}
	mustNil(t, "remove", os.Remove(e.hostPath(e.itemDir("b", pvc3))))
	e.sim.addMount(conflict.StagePath, "tmpfs", "t")

	e.pass()
	e.metrics.mu.Lock()
	got := map[string]int{}
	for k, v := range e.metrics.conditions {
		got[k] = v
	}
	e.metrics.mu.Unlock()
	want := map[string]int{
		ConditionOK: 2, ConditionItemMissing: 1, ConditionStageConflict: 1,
	}
	for _, c := range Conditions {
		v, present := got[c]
		if !present {
			t.Errorf("condition %q is absent from the report", c)
		}
		if v != want[c] {
			t.Errorf("condition %q = %d, want %d", c, v, want[c])
		}
	}
	if len(got) != len(Conditions) {
		t.Errorf("report has %d keys, want %d: %v", len(got), len(Conditions), got)
	}

	// The conflict is cleared and the missing directory comes back.
	e.sim.remove(conflict.StagePath)
	mustNil(t, "mkdir", os.MkdirAll(e.hostPath(e.itemDir("b", pvc3)), 0o750))
	e.pass()
	if e.metrics.conditionCount(ConditionOK) != 4 || e.metrics.conditionCount(ConditionItemMissing) != 0 || e.metrics.conditionCount(ConditionStageConflict) != 0 {
		t.Errorf("after repair: %v, want 4 ok", e.metrics.conditions)
	}

	// A volume that is unstaged is no longer counted.
	mustNil(t, "unstage", e.store.Delete(ok1.VolumeID))
	e.pass()
	if e.metrics.conditionCount(ConditionOK) != 3 {
		t.Errorf("after one volume went: %v, want 3 ok", e.metrics.conditions)
	}
}

// A failing publish puts its volume in the publish's condition, and a volume is
// counted once however many things are wrong with it.
func TestAVolumeIsInOneConditionAFailedPublishIncluded(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	in := e.vol("a", pvc1)
	e.mkTargetParent("pod1")
	e.mkTargetParent("pod2")
	t1, t2 := e.target("pod1"), e.target("pod2")
	e.sim.addMount(t1, "tmpfs", "t")
	in.Publishes = []intent.Publish{{TargetPath: t1}, {TargetPath: t2}}
	mustNil(t, "save", e.store.Save(in))
	e.pass()
	total := 0
	for _, c := range Conditions {
		total += e.metrics.conditionCount(c)
	}
	if total != 1 || e.metrics.conditionCount(ConditionPublishConflict) != 1 {
		t.Errorf("conditions = %v, want the one volume in publish-conflict", e.metrics.conditions)
	}
}

// A shutdown pass reports nothing: its failures are cancellations, not
// conditions.
func TestShutdownReportsNoConditions(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	in := e.vol("a", pvc1)
	mustNil(t, "save", e.store.Save(in))
	ctx, cancel := context.WithCancel(bg)
	cancel()
	e.r.runPass(ctx, nil)
	if e.metrics.conditions != nil {
		t.Errorf("conditions reported during shutdown: %v", e.metrics.conditions)
	}
	if _, reported := e.metrics.upOf(in.Transport.Kind, in.Transport.ID()); reported {
		t.Error("up reported during shutdown")
	}
}

// A transport that failed to mount and then mounted and answered did not
// "answer again": nothing had stopped. A transport that answered, stopped and
// answered again logs one line of each.
func TestUpTransitionsAreLoggedOnlyForAMountedTransport(t *testing.T) {
	t.Parallel()
	t.Run("mount fails, then mounts and answers", func(t *testing.T) {
		t.Parallel()
		lg, buf := logger()
		e := newEnv(t, func(o *Options) { o.Logger = lg })
		in := e.vol("a", pvc1)
		mustNil(t, "save", e.store.Save(in))
		e.sim.FailOp(fake.OpBind, e.tdir("a"), syscall.ECONNREFUSED)
		e.pass()
		e.sim.FailOp(fake.OpBind, e.tdir("a"), nil)
		e.pass()
		e.pass()
		if up, _ := e.metrics.upOf(in.Transport.Kind, in.Transport.ID()); !up {
			t.Error("up = 0 once mounted and answering")
		}
		for _, line := range []string{"stopped answering", "answers again"} {
			if strings.Contains(buf.String(), line) {
				t.Errorf("logged %q for a transport that was never mounted and then answering:\n%s", line, buf.String())
			}
		}
	})
	t.Run("answers, stops, answers again", func(t *testing.T) {
		t.Parallel()
		lg, buf := logger()
		e := newEnv(t, func(o *Options) { o.Logger = lg })
		in := e.vol("a", pvc1)
		mustNil(t, "save", e.store.Save(in))
		tdir := e.tdir("a")
		e.pass()
		e.sim.FailOp(fake.OpStatfs, tdir, syscall.EIO)
		e.pass()
		e.pass()
		e.sim.FailOp(fake.OpStatfs, tdir, nil)
		e.pass()
		e.pass()
		for _, line := range []string{"transport stopped answering", "transport answers again"} {
			if n := strings.Count(buf.String(), line); n != 1 {
				t.Errorf("%d %q lines, want 1:\n%s", n, line, buf.String())
			}
		}
	})
}
