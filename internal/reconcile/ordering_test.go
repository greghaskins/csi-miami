package reconcile

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/mount/fake"
)

// abandonedUnstage queues an Unstage whose caller gives up before any pass
// runs, so it stays in the queue.
func (e *env) abandonedUnstage(volumeID string) {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(bg, 20*time.Millisecond)
	defer cancel()
	if err := e.r.Unstage(ctx, volumeID); !errors.Is(err, context.DeadlineExceeded) {
		e.t.Fatalf("Unstage = %v, want the caller's deadline", err)
	}
	if e.r.queued() != 1 {
		e.t.Fatalf("%d requests queued, want the abandoned Unstage", e.r.queued())
	}
}

// Kubelet gives up on an Unstage and then stages the volume again for a new
// pod. The Stage is the later word.
func TestAStageSupersedesAnAbandonedUnstage(t *testing.T) {
	t.Parallel()
	e := newEnv(t) // the loop starts after both requests are queued
	in := e.vol("a", "pvc-1")
	mustNil(t, "save", e.store.Save(in))
	e.pass()
	e.abandonedUnstage(in.VolumeID)

	res := make(chan error, 1)
	go func() { res <- e.r.Stage(bg, in) }()
	waitUntil(t, "the Stage to queue", func() bool { return e.r.queued() == 2 })
	e.start()

	if err := <-res; err != nil {
		t.Fatalf("Stage = %v", err)
	}
	if _, ok := e.intentOf(in.VolumeID); !ok {
		t.Error("the Stage's record was deleted by the stale Unstage")
	}
	if !e.isBind(in.StagePath, e.itemDir("a", "pvc-1")) {
		t.Error("the volume is not staged")
	}
}

func TestAPublishSupersedesAnAbandonedUnstage(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	in := e.vol("a", "pvc-1")
	mustNil(t, "save", e.store.Save(in))
	e.pass()
	e.abandonedUnstage(in.VolumeID)
	e.mkTargetParent("pod2")

	res := make(chan error, 1)
	go func() { res <- e.r.Publish(bg, in.VolumeID, intent.Publish{TargetPath: e.target("pod2")}) }()
	waitUntil(t, "the Publish to queue", func() bool { return e.r.queued() == 2 })
	e.start()

	if err := <-res; err != nil {
		t.Fatalf("Publish = %v (an abandoned Unstage must not take the volume from a new pod)", err)
	}
	if !e.isBind(e.target("pod2"), in.StagePath) {
		t.Error("the publish is not bound")
	}
}

// The superseded request itself is answered, so a caller that is still
// waiting learns what happened.
func TestTheSupersededUnstageIsAnswered(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	in := e.vol("a", "pvc-1")
	mustNil(t, "save", e.store.Save(in))
	e.pass()
	unstage := &request{kind: reqUnstage, volume: in.VolumeID, done: make(chan error, 1)}
	stage := &request{kind: reqStage, volume: in.VolumeID, done: make(chan error, 1)}
	e.r.runPass(bg, []*request{unstage, stage})

	if err := <-unstage.done; !errors.Is(err, ErrSuperseded) {
		t.Errorf("Unstage = %v, want ErrSuperseded", err)
	}
	mustNil(t, "Stage", <-stage.done)
	if _, ok := e.intentOf(in.VolumeID); !ok {
		t.Error("the record was deleted")
	}
}

// The other order is the other way round: a Stage the Unstage came after is
// the one that loses.
func TestAnUnstageAfterAStageWins(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	in := e.vol("a", "pvc-1")
	mustNil(t, "save", e.store.Save(in))
	e.pass()
	stage := &request{kind: reqStage, volume: in.VolumeID, done: make(chan error, 1)}
	unstage := &request{kind: reqUnstage, volume: in.VolumeID, done: make(chan error, 1)}
	e.r.runPass(bg, []*request{stage, unstage})

	mustNil(t, "Unstage", <-unstage.done)
	if err := <-stage.done; !errors.Is(err, ErrSuperseded) {
		t.Errorf("Stage = %v, want ErrSuperseded", err)
	}
	if _, ok := e.intentOf(in.VolumeID); ok {
		t.Error("the record survived the Unstage")
	}
}

// A Publish recorded after the pass loaded the record is invisible to the
// teardown running in that pass. The record must survive it.
func TestUnstageDoesNotDeleteARecordChangedSinceItWasLoaded(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", "pvc-1")
	mustNil(t, "Stage", e.r.Stage(bg, in))
	e.mkTargetParent("pod9")
	late := e.target("pod9")

	e.setHook(func(c fake.Call) {
		if c.Op == fake.OpUnmount && c.Target == in.StagePath {
			mustNil(t, "late publish", e.store.Update(in.VolumeID, func(r *intent.Intent) error {
				r.Publishes = append(r.Publishes, intent.Publish{TargetPath: late})
				return nil
			}))
		}
	})
	err := e.r.Unstage(bg, in.VolumeID)
	if !errors.Is(err, ErrSuperseded) {
		t.Fatalf("Unstage = %v, want ErrSuperseded", err)
	}
	got, ok := e.intentOf(in.VolumeID)
	if !ok || len(got.Publishes) != 1 || got.Publishes[0].TargetPath != late {
		t.Fatalf("the newer record was lost: %+v, %v", got, ok)
	}
	// The next pass brings the volume back, stage and the late publish.
	e.hook.Store(nil)
	n := e.passes()
	e.r.nudge()
	e.waitPasses(n + 1)
	if !e.isBind(in.StagePath, e.itemDir("a", "pvc-1")) || !e.isBind(late, in.StagePath) {
		t.Error("the volume was not restored from the newer record")
	}
}
