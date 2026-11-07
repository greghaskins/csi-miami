package reconcile

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/blockd"
	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/item"
)

// stageGen is a genItem whose stage can be made to fail while its stack (the
// block server, device, mapping and mount) goes on: the state of a volume
// during a store outage, after an emergency detach and after a restart with
// the key gone. It counts the status reads.
type stageGen struct {
	genItem
	failStage atomic.Bool
	failTear  atomic.Bool
	statuses  atomic.Int32
}

func (s *stageGen) Converge(context.Context, intent.Intent, item.Observed) error {
	if s.failStage.Load() {
		return fmt.Errorf("converge: %w", ErrHolderContended)
	}
	return nil
}

func (s *stageGen) Teardown(context.Context, intent.Intent, item.Observed) error {
	if s.failTear.Load() {
		return errors.New("the final generation did not finish")
	}
	return nil
}

func (s *stageGen) Status(ctx context.Context, in intent.Intent) (blockd.Status, error) {
	s.statuses.Add(1)
	return s.genItem.Status(ctx, in)
}

// bothMetrics records the generation and the probe series together.
type bothMetrics struct {
	*genMetrics
	pm *probeMetrics
}

func (m *bothMetrics) SetVolumeProbe(id string, ok bool) { m.pm.SetVolumeProbe(id, ok) }
func (m *bothMetrics) DeleteVolumeProbe(id string)       { m.pm.DeleteVolumeProbe(id) }

type reportEnv struct {
	*probeEnv
	s  *stageGen
	gm *genMetrics
}

func newReportEnv(t *testing.T) *reportEnv {
	t.Helper()
	s := &stageGen{}
	rs := &readScript{}
	pm := &probeMetrics{ok: map[string]bool{}}
	gm := &genMetrics{vols: map[string]VolumeGeneration{}}
	e := newGenEnv(t, func(o *Options) {
		o.Items = []item.Item{s}
		o.Probe = &ProbeOptions{Read: rs.read, Timeout: 10 * time.Second}
	})
	s.clock = e.clock
	pm.recMetrics = e.metrics
	gm.recMetrics = e.metrics
	e.r.metrics = &bothMetrics{genMetrics: gm, pm: pm}
	return &reportEnv{probeEnv: &probeEnv{genEnv: e, rs: rs, pm: pm}, s: s, gm: gm}
}

// The reviewer's probe: a volume that was serving and whose stage then fails
// is still read, still measured and still judged for overdue.
func TestStackUpWithAFailingStageStillProbesAndMeasures(t *testing.T) {
	t.Parallel()
	e := newReportEnv(t)
	id := e.in.VolumeID
	e.s.set(func(g *genItem) { g.status.Dirty = 4096; g.status.LastPublish = e.clock.Now() })
	e.tick()
	e.tick()
	statuses, reads := e.s.statuses.Load(), e.rs.n()
	if statuses == 0 || reads == 0 {
		t.Fatalf("healthy passes: statuses = %d, probe reads = %d", statuses, reads)
	}

	e.s.failStage.Store(true)
	e.s.set(func(g *genItem) { g.status.Dirty = 8192 })
	e.tick()
	e.tick()
	if got := e.s.statuses.Load(); got <= statuses {
		t.Errorf("status reads stayed at %d while the stage failed", got)
	}
	if got := e.rs.n(); got <= reads {
		t.Errorf("probe reads stayed at %d while the stage failed", got)
	}
	if g, ok := e.gm.vol(id); !ok || g.DirtyBytes != 8192 {
		t.Errorf("gauges = %+v (present %v), want the dirty bytes of the failing pass", g, ok)
	}
	if n := e.metrics.conditionCount(ConditionHolderContended); n != 1 {
		t.Errorf("conditions = %v, want the stage's", e.metrics.conditions)
	}

	// The probe sees what the pod sees, with the store gone.
	e.rs.set(errors.New("input/output error"))
	e.tick()
	e.tick()
	e.tick()
	if abnormal, _ := e.r.VolumeCondition(id); !abnormal {
		t.Error("a failing device read went unnoticed while the stage failed")
	}
	if n := len(e.events.reasons(ReasonVolumeUnhealthy)); n != 1 {
		t.Errorf("VolumeUnhealthy events = %d, want 1", n)
	}
}

// A store outage with unpublished writes: every generation fails, the stage
// fails, and the volume still turns overdue and says so.
func TestOverdueIsReportedWhileTheStageFails(t *testing.T) {
	t.Parallel()
	e := newReportEnv(t)
	start := e.clock.Now()
	e.s.set(func(g *genItem) {
		g.status.Dirty = 4096
		g.status.LastPublish = start
		g.err = errors.New("store unreachable")
	})
	e.s.failStage.Store(true)
	e.tick()
	e.clock.Advance(2 * time.Hour)
	e.tick()
	e.tick()
	if n := len(e.events.reasons(ReasonGenerationOverdue)); n != 1 {
		t.Errorf("overdue events = %d, want 1", n)
	}
	e.tick()
	if n := len(e.events.reasons(ReasonGenerationOverdue)); n != 1 {
		t.Errorf("overdue events = %d after another pass, want 1", n)
	}
	if g, ok := e.gm.vol(e.in.VolumeID); !ok || g.Age < 2*time.Hour {
		t.Errorf("age gauge = %v (present %v), want it to keep growing", g.Age, ok)
	}
}

// A scheduled generation the store refuses raises the event once, whatever
// the number of ticks, and shows in the conditions; a success ends it.
func TestRefusedScheduledGenerationRaisesOneEventAndTheCondition(t *testing.T) {
	t.Parallel()
	e := newReportEnv(t)
	e.s.set(func(g *genItem) {
		g.status.Dirty = 4096
		g.status.LastPublish = e.clock.Now()
		g.err = fmt.Errorf("volume item: %w", item.ErrPublishRefused)
	})
	for range 4 {
		e.tick()
		e.clock.Advance(5 * time.Minute)
	}
	e.tick()
	if got := len(e.s.calls()); got < 3 {
		t.Fatalf("generations attempted = %d, want one per interval", got)
	}
	if n := len(e.events.reasons(ReasonPublishRefused)); n != 1 {
		t.Errorf("PublishRefused events = %d, want 1", n)
	}
	if n := e.metrics.conditionCount(ConditionPublishRefused); n != 1 {
		t.Errorf("conditions = %v, want publish-refused", e.metrics.conditions)
	}

	e.s.set(func(g *genItem) { g.err = nil })
	e.clock.Advance(5 * time.Minute)
	e.tick()
	e.tick()
	if n := e.metrics.conditionCount(ConditionPublishRefused); n != 0 {
		t.Errorf("conditions = %v after a success", e.metrics.conditions)
	}
}

// A refused generation on an old node whose stage is holder-contended: the
// spec 4.6 case.
func TestRefusedGenerationIsReportedWhileHolderContended(t *testing.T) {
	t.Parallel()
	e := newReportEnv(t)
	e.s.failStage.Store(true)
	e.s.set(func(g *genItem) {
		g.status.Dirty = 4096
		g.status.LastPublish = e.clock.Now()
		g.err = item.ErrPublishRefused
	})
	e.tick()
	e.clock.Advance(5 * time.Minute)
	e.tick()
	e.tick()
	if n := len(e.events.reasons(ReasonPublishRefused)); n != 1 {
		t.Errorf("PublishRefused events = %d, want 1", n)
	}
}

// A volume whose unstage began and has not finished gets no generation,
// whether or not its stack is up.
func TestMarkedVolumeGetsNoGenerationWhileItsStageFails(t *testing.T) {
	t.Parallel()
	e := newReportEnv(t)
	e.s.set(func(g *genItem) { g.status.Dirty = 4096 })
	e.tick()
	e.s.failStage.Store(true)
	e.s.failTear.Store(true)
	mustNil(t, "mark", e.store.Update(e.in.VolumeID, func(r *intent.Intent) error {
		r.Unstaging = true
		r.UnstagingSince = e.clock.Now()
		return nil
	}))
	before, reads, statuses := len(e.s.calls()), e.rs.n(), e.s.statuses.Load()
	e.clock.Advance(time.Hour)
	e.tick()
	e.tick()
	if n := len(e.s.calls()); n != before {
		t.Errorf("a marked volume was given %d generations", n-before)
	}
	if n := e.rs.n(); n != reads {
		t.Errorf("a marked volume was probed %d times", n-reads)
	}
	if n := e.s.statuses.Load(); n != statuses {
		t.Errorf("a marked volume had its status read %d times", n-statuses)
	}
}

// A server that does not answer is a stack that is down: no generation, no
// probe, and no series, whatever the stage says.
func TestStackDownGetsNoGenerationAndNoSeries(t *testing.T) {
	t.Parallel()
	e := newReportEnv(t)
	e.s.set(func(g *genItem) { g.status.Dirty = 4096 })
	e.tick()
	e.tick()
	if _, ok := e.gm.vol(e.in.VolumeID); !ok {
		t.Fatal("no gauges while healthy")
	}
	e.s.failStage.Store(true)
	e.s.set(func(g *genItem) { g.statusEr = errors.New("no answer") })
	before := len(e.s.calls())
	e.clock.Advance(time.Hour)
	e.tick() // the pass that finds the server down; its probe was decided before the read
	reads := e.rs.n()
	e.tick()
	e.tick()
	if n := len(e.s.calls()); n != before {
		t.Errorf("%d generations with the server down", n-before)
	}
	if n := e.rs.n(); n != reads {
		t.Errorf("%d probe reads with the server down", n-reads)
	}
	if _, ok := e.gm.vol(e.in.VolumeID); ok {
		t.Error("the gauges stayed while the server was down")
	}
	if _, ok := e.pm.get(e.in.VolumeID); ok {
		t.Error("the probe gauge stayed while the server was down")
	}
}
