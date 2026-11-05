package reconcile

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/item"
)

// drainItem is a countItem that can drain: it reports the orphans it is given
// and records the volumes it was asked to drain.
type drainItem struct {
	*countItem
	e *env

	dmu     sync.Mutex
	orphans []intent.Intent
	drained []string
	fail    error
	mounted []bool   // whether the volume's transport was mounted at each Drain
	arrive  func()   // runs inside Orphans, to play a request landing meanwhile
	order   []string // "orphans", "drain:<id>"
}

func (d *drainItem) Orphans(context.Context) ([]intent.Intent, error) {
	d.dmu.Lock()
	defer d.dmu.Unlock()
	d.order = append(d.order, "orphans")
	if d.arrive != nil {
		d.arrive()
	}
	return slices.Clone(d.orphans), nil
}

func (d *drainItem) Drain(_ context.Context, in intent.Intent) error {
	d.dmu.Lock()
	defer d.dmu.Unlock()
	d.drained = append(d.drained, in.VolumeID)
	d.order = append(d.order, "drain:"+in.VolumeID)
	tdir, _ := in.Transport.MountPoint(d.e.tRoot)
	_, ok := d.e.sim.table().At(tdir)
	d.mounted = append(d.mounted, ok)
	if d.fail != nil {
		return d.fail
	}
	d.orphans = slices.DeleteFunc(d.orphans, func(o intent.Intent) bool { return o.VolumeID == in.VolumeID })
	return nil
}

func (d *drainItem) drains() []string {
	d.dmu.Lock()
	defer d.dmu.Unlock()
	return slices.Clone(d.drained)
}

func newDrainEnv(t *testing.T) (*env, *drainItem) {
	t.Helper()
	di := &drainItem{countItem: &countItem{}}
	e := newEnv(t, func(o *Options) { o.Items = []item.Item{di} })
	di.e = e
	return e, di
}

func drainStuckCount(e *env, id string) int {
	n := 0
	for _, ev := range e.events.all() {
		if ev.Reason == ReasonDrainStuck && ev.Volume == id {
			n++
		}
	}
	return n
}

func TestOrphanWithNoIntentIsDrainedWithItsTransportMounted(t *testing.T) {
	t.Parallel()
	e, di := newDrainEnv(t)
	orphan := e.vol("a", pvc1)
	di.orphans = []intent.Intent{orphan}
	e.r.drainDue = true
	e.pass()
	if got := di.drains(); !slices.Equal(got, []string{orphan.VolumeID}) {
		t.Fatalf("drained = %v", got)
	}
	if len(di.mounted) != 1 || !di.mounted[0] {
		t.Errorf("the transport was mounted at the drain: %v", di.mounted)
	}
	// Done: not asked again.
	e.r.drainDue = true
	e.pass()
	if got := di.drains(); len(got) != 1 {
		t.Errorf("drained again: %v", got)
	}
}

// Only the first pass (the start) and the timer passes drain: a pass a kubelet
// request woke is not held up behind a final generation.
func TestDrainRunsAtStartAndOnTimerPassesOnly(t *testing.T) {
	t.Parallel()
	e, di := newDrainEnv(t)
	di.orphans = []intent.Intent{e.vol("a", pvc1)}
	e.pass() // woken by a request, say
	if got := di.drains(); len(got) != 0 {
		t.Fatalf("a request pass drained: %v", got)
	}

	// Run's own schedule: the first pass, and every tick.
	ticks := make(chan time.Time)
	e.r.newTicker = func(time.Duration) (<-chan time.Time, func()) { return ticks, func() {} }
	e.start()
	e.waitPasses(1)
	if got := di.drains(); len(got) != 1 {
		t.Fatalf("the start pass did not drain: %v", got)
	}
}

func TestDrainTicksFromRun(t *testing.T) {
	t.Parallel()
	e, di := newDrainEnv(t)
	di.fail = errors.New("not yet")
	di.orphans = []intent.Intent{e.vol("a", pvc1)}
	ticks := make(chan time.Time)
	e.r.newTicker = func(time.Duration) (<-chan time.Time, func()) { return ticks, func() {} }
	e.start()
	first := len(di.drains())
	if first != 1 {
		t.Fatalf("start pass drains = %d", first)
	}
	e.clock.Advance(time.Hour)
	before := e.passes()
	e.r.nudge() // a request-style wake: no drain
	e.waitPasses(before + 1)
	if got := len(di.drains()); got != first {
		t.Errorf("a woken pass drained (%d -> %d)", first, got)
	}
	ticks <- e.clock.Now() // a tick: drain
	e.waitPasses(before + 2)
	if got := len(di.drains()); got != first+1 {
		t.Errorf("a timer pass did not drain (%d -> %d)", first, got)
	}
}

func TestStagedAndMarkedVolumesAreNotOrphans(t *testing.T) {
	t.Parallel()
	e, di := newDrainEnv(t)
	staged := e.vol("a", pvc1)
	marked := e.vol("a", pvc2)
	marked.Unstaging, marked.UnstagingSince = true, e.clock.Now()
	mustNil(t, "save", e.store.Save(staged))
	mustNil(t, "save", e.store.Save(marked))
	free := e.vol("a", pvc3)
	di.orphans = []intent.Intent{staged, marked, free}
	e.r.drainDue = true
	e.pass()
	if got := di.drains(); !slices.Equal(got, []string{free.VolumeID}) {
		t.Errorf("drained = %v, want only the volume with no record", got)
	}
}

// A request lands between the pass's load and the drain: its record is there
// when the drain looks, so the volume is left to the stage.
func TestAVolumeStagedMeanwhileIsNotDrained(t *testing.T) {
	t.Parallel()
	e, di := newDrainEnv(t)
	in := e.vol("a", pvc1)
	di.orphans = []intent.Intent{in}
	di.arrive = func() { mustNil(t, "save", e.store.Save(in)) }
	e.r.drainDue = true
	e.pass()
	if got := di.drains(); len(got) != 0 {
		t.Errorf("drained a volume that was staged meanwhile: %v", got)
	}
}

// A record that did not load might be this volume's.
func TestNothingIsDrainedWhileARecordIsUnreadable(t *testing.T) {
	t.Parallel()
	e, di := newDrainEnv(t)
	di.orphans = []intent.Intent{e.vol("a", pvc1)}
	if err := os.WriteFile(filepath.Join(e.store.Dir(), "broken.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	e.r.drainDue = true
	e.pass()
	if got := di.drains(); len(got) != 0 {
		t.Errorf("drained with an unreadable record in the store: %v", got)
	}
}

func TestFailedDrainBacksOffAndWarnsAfterTheMarkBound(t *testing.T) {
	t.Parallel()
	e, di := newDrainEnv(t)
	orphan := e.vol("a", pvc1)
	di.orphans = []intent.Intent{orphan}
	di.fail = item.ErrUnstageIncomplete

	attempt := func() int {
		e.r.drainDue = true
		e.pass()
		return len(di.drains())
	}
	if n := attempt(); n != 1 {
		t.Fatalf("first pass attempts = %d", n)
	}
	if n := attempt(); n != 1 {
		t.Fatalf("retried at once: %d", n)
	}
	e.clock.Advance(time.Minute) // MarkRetry
	if n := attempt(); n != 2 {
		t.Fatalf("no retry after the first delay: %d", n)
	}
	e.clock.Advance(time.Minute) // the delay has doubled to 2m
	if n := attempt(); n != 2 {
		t.Fatalf("retried before the doubled delay: %d", n)
	}
	e.clock.Advance(time.Minute)
	if n := attempt(); n != 3 {
		t.Fatalf("no retry after the doubled delay: %d", n)
	}
	if got := drainStuckCount(e, orphan.VolumeID); got != 0 {
		t.Fatalf("warned after %d minutes: %d", 4, got)
	}
	// Past MarkWarnAfter (30m by default): one warning, then no more.
	for range 12 {
		e.clock.Advance(10 * time.Minute)
		attempt()
	}
	if got := drainStuckCount(e, orphan.VolumeID); got != 1 {
		t.Errorf("stuck events = %d, want 1", got)
	}
	// The delay is capped at MarkRetryMax: ten minutes pass, it tries again.
	n := len(di.drains())
	e.clock.Advance(11 * time.Minute)
	if attempt() != n+1 {
		t.Error("the backoff grew past MarkRetryMax")
	}
	if c := e.metrics.conditionCount(ConditionFailed); c != 1 {
		t.Errorf("volumes in condition %s = %d, want the orphan", ConditionFailed, c)
	}

	// It recovers: drained, forgotten, nothing left to retry.
	di.dmu.Lock()
	di.fail = nil
	di.dmu.Unlock()
	e.clock.Advance(11 * time.Minute)
	attempt()
	if len(di.orphans) != 0 {
		t.Error("the orphan was not drained once the failure cleared")
	}
	if _, ok := e.r.orphans[orphan.VolumeID]; ok {
		t.Error("the backoff state of a drained volume is kept")
	}
}
