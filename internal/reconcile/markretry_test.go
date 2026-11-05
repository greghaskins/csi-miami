package reconcile

import (
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/item"
	"github.com/greghaskins/csi-miami/internal/mount/fake"
)

func stuckCount(e *env, id string) int {
	n := 0
	for _, ev := range e.events.all() {
		if ev.Reason == ReasonUnstageStuck && ev.Volume == id {
			n++
		}
	}
	return n
}

// A Stage or a Publish that clears the mark after the pass loaded the record
// must not be answered with a teardown and a restage: the retry re-reads the
// record just before it tears down.
func TestMarkedRetrySkipsWhenTheMarkWasClearedAfterTheLoad(t *testing.T) {
	t.Parallel()
	e, ci, in := newMarked(t)
	_, base := ci.counts()
	e.clock.Advance(time.Hour)

	cleared := false
	e.setHook(func(fake.Call) {
		if cleared {
			return
		}
		cleared = true
		mustNil(t, "clear", e.store.Update(in.VolumeID, func(r *intent.Intent) error {
			r.Unstaging = false
			r.UnstagingSince = time.Time{}
			return nil
		}))
	})
	e.pass()
	if !cleared {
		t.Skip("the pass made no call to hook; the test would prove nothing")
	}
	if _, n := ci.counts(); n != base {
		t.Errorf("the retry tore down a volume whose mark was cleared: %d teardowns after %d", n, base)
	}
	if _, ok := e.intentOf(in.VolumeID); !ok {
		t.Error("the record was deleted")
	}
}

// The age of the unstage is persisted with the mark: a plugin that restarts
// more often than the warning bound still warns.
func TestUnstageStuckSurvivesARestart(t *testing.T) {
	t.Parallel()
	e, ci, in := newMarked(t)
	rec, _ := e.intentOf(in.VolumeID)
	if rec.UnstagingSince.IsZero() {
		t.Fatal("the mark carries no start time")
	}
	e.clock.Advance(45 * time.Minute)
	r2 := e.newReconciler(func(o *Options) { o.Items = []item.Item{ci} })
	e.passOn(r2)
	if got := stuckCount(e, in.VolumeID); got != 1 {
		t.Errorf("stuck events after a restart = %d, want 1 on the first pass", got)
	}
}

// The clock is the node's wall clock: a start time in the future counts as now.
func TestUnstagingSinceInTheFutureCountsAsNow(t *testing.T) {
	t.Parallel()
	e, ci, in := newMarked(t)
	mustNil(t, "future", e.store.Update(in.VolumeID, func(r *intent.Intent) error {
		r.UnstagingSince = e.clock.Now().Add(time.Hour)
		return nil
	}))
	r2 := e.newReconciler(func(o *Options) { o.Items = []item.Item{ci} })
	e.clock.Advance(time.Minute)
	e.passOn(r2)
	e.clock.Advance(10 * time.Minute)
	e.passOn(r2)
	if got := stuckCount(e, in.VolumeID); got != 0 {
		t.Fatalf("warned %d times for an unstage first seen 11 minutes ago", got)
	}
	e.clock.Advance(25 * time.Minute) // 36 minutes after first seen, still before the stored time
	e.passOn(r2)
	if got := stuckCount(e, in.VolumeID); got != 1 {
		t.Errorf("stuck events = %d, want 1: a future start time must count as now", got)
	}
}

// A record from before the field has no start: the age runs from first seen.
func TestUnstagingWithoutASinceRunsFromFirstSeen(t *testing.T) {
	t.Parallel()
	e, ci, in := newMarked(t)
	mustNil(t, "old record", e.store.Update(in.VolumeID, func(r *intent.Intent) error {
		r.UnstagingSince = time.Time{}
		return nil
	}))
	e.clock.Advance(time.Hour)
	r2 := e.newReconciler(func(o *Options) { o.Items = []item.Item{ci} })
	e.passOn(r2)
	if got := stuckCount(e, in.VolumeID); got != 0 {
		t.Fatalf("warned at once for a record with no start time")
	}
	e.clock.Advance(40 * time.Minute)
	e.passOn(r2)
	if got := stuckCount(e, in.VolumeID); got != 1 {
		t.Errorf("stuck events = %d, want 1", got)
	}
}

func TestAPublishClearsTheUnstagingStart(t *testing.T) {
	t.Parallel()
	e, ci, in := newMarked(t)
	ci.mu.Lock()
	ci.src = e.itemDir("a", pvc1)
	ci.mu.Unlock()
	e.mkTargetParent("pod1")
	mustNil(t, "Publish", e.r.Publish(bg, in.VolumeID, intent.Publish{TargetPath: e.target("pod1")}))
	if rec, _ := e.intentOf(in.VolumeID); !rec.UnstagingSince.IsZero() {
		t.Errorf("the start survived the mark: %v", rec.UnstagingSince)
	}
}

// A failed timer retry shows in the volume's condition, not only in the log.
func TestFailedTimerRetryIsTheVolumesCondition(t *testing.T) {
	t.Parallel()
	e, _, in := newMarked(t)
	if _, ok := e.r.lastErr[in.VolumeID]; ok {
		t.Fatal("a condition before any retry")
	}
	e.clock.Advance(time.Minute)
	e.pass()
	if _, ok := e.r.lastErr[in.VolumeID]; !ok {
		t.Error("a failed retry left no last error")
	}
	if n := e.metrics.conditionCount(ConditionFailed); n != 1 {
		t.Errorf("volumes in condition %s = %d, want 1", ConditionFailed, n)
	}
}
