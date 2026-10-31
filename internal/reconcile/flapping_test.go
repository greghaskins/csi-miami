package reconcile

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/item"
	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/mount/fake"
)

// logger returns a text logger on a buffer, for counting lines. Passes in
// these tests run on the test goroutine, so the buffer needs no lock.
func logger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})), &buf
}

// A transport that stays hung is a timeout on the pass that finds out and
// "busy" on every pass after, because the abandoned call has not returned. It
// is one condition: one event, one warning.
func TestAHungTransportRaisesOneEventAcrossManyPasses(t *testing.T) {
	t.Parallel()
	lg, buf := logger()
	e := newEnv(t, func(o *Options) { o.Logger = lg })
	e.sim.DefaultTimeout = 20 * time.Millisecond
	in := e.vol("a", pvc1)
	mustNil(t, "save", e.store.Save(in))
	e.sim.Block(e.tdir("a"))

	var first, rest []error
	for i := 0; i < 6; i++ {
		err := e.converge(in.VolumeID)
		if i == 0 {
			first = append(first, err)
		} else {
			rest = append(rest, err)
		}
	}
	if !errors.Is(first[0], mount.ErrTimeout) {
		t.Fatalf("the first pass = %v, want a timeout", first[0])
	}
	for _, err := range rest {
		if !errors.Is(err, mount.ErrBusy) {
			t.Fatalf("a later pass = %v, want busy (the abandoned call has not returned)", err)
		}
	}
	if evs := e.events.all(); len(evs) != 1 {
		t.Errorf("%d events over six passes of one persistent hang, want 1: %+v", len(evs), evs)
	}
	if n := strings.Count(buf.String(), "volume did not converge"); n != 1 {
		t.Errorf("%d warnings over six passes, want 1", n)
	}
	// Errors are still counted every pass; only the noise is deduplicated.
	if n := e.metrics.errCount("transport"); n != 6 {
		t.Errorf("transport errors = %d, want 6", n)
	}

	// When it recovers and breaks again, that is news.
	e.sim.Unblock(e.tdir("a"))
	mustNil(t, "recovered", e.converge(in.VolumeID))
	e.sim.Block(e.tdir("a"))
	e.sim.remove(e.tdir("a"))
	_ = e.converge(in.VolumeID)
	if evs := e.events.all(); len(evs) != 2 {
		t.Errorf("%d events after a recovery and a new hang, want 2", len(evs))
	}
}

// The same for a stat of the item directory that never returns.
func TestAHungItemStatRaisesOneEventAcrossManyPasses(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	e := newEnv(t)
	d, err := e.dirItem(20*time.Millisecond, func() { <-release })
	mustNil(t, "item", err)
	e.r = e.newReconciler(func(o *Options) { o.Items = []item.Item{d} })
	in := e.vol("a", pvc1)
	mustNil(t, "save", e.store.Save(in))

	for range 6 {
		_ = e.converge(in.VolumeID)
	}
	if evs := e.events.all(); len(evs) != 1 {
		t.Errorf("%d events over six passes, want 1: %+v", len(evs), evs)
	}
}

// A mount that cannot be removed is one warning until the reason changes.
func TestABusyStrayWarnsOncePerKindOfFailure(t *testing.T) {
	t.Parallel()
	lg, buf := logger()
	e := newEnv(t, func(o *Options) { o.Logger = lg })
	stray := filepath.Join(e.sRoot, "stray")
	e.sim.addMount(stray, "tmpfs", "tmpfs")
	e.sim.FailOp(fake.OpUnmount, stray, unix.EBUSY)

	for range 5 {
		e.pass()
	}
	count := func() int { return strings.Count(buf.String(), "garbage mount could not be removed") }
	if count() != 1 {
		t.Fatalf("%d warnings over five passes of one busy mount, want 1", count())
	}
	if e.metrics.errCount("garbage") != 5 {
		t.Errorf("garbage errors = %d, want 5", e.metrics.errCount("garbage"))
	}
	// A different reason is worth another line.
	e.sim.FailOp(fake.OpUnmount, stray, mount.ErrBusy)
	e.pass()
	e.pass()
	if count() != 2 {
		t.Errorf("%d warnings after the reason changed, want 2", count())
	}
	// Once it comes off, a later failure of a new mount at the path is news.
	e.sim.FailOp(fake.OpUnmount, stray, nil)
	e.pass()
	e.sim.addMount(stray, "tmpfs", "tmpfs")
	e.sim.FailOp(fake.OpUnmount, stray, unix.EBUSY)
	e.pass()
	if count() != 3 {
		t.Errorf("%d warnings after removal and a new failure, want 3", count())
	}
}

// Shutdown cancels the steps of a pass in flight. A volume whose steps were
// cancelled is not broken, and gets no event.
func TestShutdownRaisesNoEvents(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	in := e.vol("a", pvc1)
	mustNil(t, "save", e.store.Save(in))

	ctx, cancel := context.WithCancel(bg)
	cancel()
	e.r.runPass(ctx, nil)
	if evs := e.events.all(); len(evs) != 0 {
		t.Fatalf("events during shutdown: %+v", evs)
	}
	// Not vacuous: the same volume with a real failure does raise one.
	e.sim.FailOp(fake.OpBind, e.tdir("a"), mount.ErrBusy)
	e.pass()
	if len(e.events.all()) != 1 {
		t.Errorf("events = %+v, want the real failure", e.events.all())
	}
}

// The publish half of the event key: a publish that stays in conflict is one
// event however many passes find it, and one that fails differently is
// another.
func TestAPersistentPublishFailureRaisesOneEventUntilItChanges(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	in := e.vol("a", pvc1)
	e.mkTargetParent("pod1")
	target := e.target("pod1")
	e.sim.addMount(target, "tmpfs", "t")
	in.Publishes = []intent.Publish{{TargetPath: target}}
	mustNil(t, "save", e.store.Save(in))

	for range 5 {
		e.pass()
	}
	if evs := e.events.reasons(ReasonMountFailed); len(evs) != 1 {
		t.Fatalf("%d events for a publish that stays in conflict, want 1: %+v", len(evs), evs)
	}
	if n := e.metrics.errCount("publish"); n != 5 {
		t.Errorf("publish errors = %d, want one per pass", n)
	}

	// The foreign mount goes; now the bind itself reports busy. A new
	// condition, a new event, and then quiet again.
	e.sim.remove(target)
	e.sim.FailOp(fake.OpBind, target, mount.ErrBusy)
	for range 4 {
		e.pass()
	}
	if evs := e.events.reasons(ReasonMountFailed); len(evs) != 2 {
		t.Fatalf("%d events after the publish failed differently, want 2: %+v", len(evs), evs)
	}

	// The stage is fine throughout, and says so: a Stage hears nothing of it.
	if res := e.converge(in.VolumeID); res != nil {
		t.Errorf("the stage answer = %v, want nil: the publish is not its business", res)
	}
}
