package reconcile

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/probe"
)

// ProbeDevice makes the generation tests' stand-in item probeable; a
// reconciler probes only when Options.Probe is set.
func (g *genItem) ProbeDevice(in intent.Intent) (string, uint64, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return "/dev/probe-test", g.stack, in.Volume != nil
}

// probeMetrics records the probe gauge beside the usual metrics.
type probeMetrics struct {
	*recMetrics
	mu sync.Mutex
	ok map[string]bool
}

func (m *probeMetrics) SetVolumeProbe(volumeID string, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ok[volumeID] = ok
}

func (m *probeMetrics) DeleteVolumeProbe(volumeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.ok, volumeID)
}

func (m *probeMetrics) get(volumeID string) (ok, present bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ok, present = m.ok[volumeID]
	return ok, present
}

// readScript is a probe.Options.Read whose answers a test sets.
type readScript struct {
	mu    sync.Mutex
	err   error
	block chan struct{}
	calls []string
}

func (s *readScript) read(_ context.Context, path string) error {
	s.mu.Lock()
	s.calls = append(s.calls, path)
	err, block := s.err, s.block
	s.mu.Unlock()
	if block != nil {
		<-block
	}
	return err
}

func (s *readScript) set(err error) { s.mu.Lock(); s.err = err; s.mu.Unlock() }

func (s *readScript) n() int { s.mu.Lock(); defer s.mu.Unlock(); return len(s.calls) }

type probeEnv struct {
	*genEnv
	rs *readScript
	pm *probeMetrics
}

func newProbeEnv(t *testing.T) *probeEnv {
	t.Helper()
	rs := &readScript{}
	pm := &probeMetrics{ok: map[string]bool{}}
	e := newGenEnv(t, func(o *Options) {
		o.Probe = &ProbeOptions{Read: rs.read, Timeout: 10 * time.Second}
	})
	pm.recMetrics = e.metrics
	e.r.metrics = pm
	return &probeEnv{genEnv: e, rs: rs, pm: pm}
}

// tick runs a pass and waits for the probe reads it started.
func (e *probeEnv) tick() {
	e.t.Helper()
	e.pass()
	e.r.gens.wait()
	e.r.probes.Wait()
}

func TestConditionsIncludeNewOnes(t *testing.T) {
	t.Parallel()
	if !slices.Contains(Conditions, ConditionProbeFailed) {
		t.Fatalf("Conditions %v lack %q", Conditions, ConditionProbeFailed)
	}
	if ConditionProbeFailed != "probe-failed" {
		t.Errorf("ConditionProbeFailed = %q", ConditionProbeFailed)
	}
}

func TestTwoFailedProbesSetTheConditionAndOneEvent(t *testing.T) {
	t.Parallel()
	e := newProbeEnv(t)
	e.rs.set(errors.New("input/output error"))
	id := e.in.VolumeID

	e.tick() // probe 1 starts and fails
	if st, _ := e.r.VolumeCondition(id); st {
		t.Fatal("abnormal after one failed probe")
	}
	e.tick() // probe 2 starts and fails; the pass reported the state before it
	if n := e.metrics.conditionCount(ConditionProbeFailed); n != 0 {
		t.Fatalf("probe-failed volumes = %d after one failure, want 0", n)
	}
	e.tick() // this pass sees two failures
	abnormal, msg := e.r.VolumeCondition(id)
	if !abnormal || msg != probe.MessageFailed {
		t.Fatalf("VolumeCondition = %v, %q", abnormal, msg)
	}
	if n := e.metrics.conditionCount(ConditionProbeFailed); n != 1 {
		t.Errorf("probe-failed volumes = %d, want 1", n)
	}
	if n := e.metrics.conditionCount(ConditionOK); n != 0 {
		t.Errorf("ok volumes = %d, want 0", n)
	}
	if ok, present := e.pm.get(id); !present || ok {
		t.Errorf("probe gauge = %v (present %v), want 0", ok, present)
	}
	e.tick()
	e.tick()
	if n := len(e.events.reasons(ReasonVolumeUnhealthy)); n != 1 {
		t.Errorf("%d VolumeUnhealthy events, want 1 (a change, not every pass)", n)
	}

	// One success clears it.
	e.rs.set(nil)
	e.tick()
	e.tick()
	if abnormal, _ := e.r.VolumeCondition(id); abnormal {
		t.Error("still abnormal after a good probe")
	}
	if n := e.metrics.conditionCount(ConditionOK); n != 1 {
		t.Errorf("ok volumes = %d after recovery, want 1", n)
	}
	if ok, present := e.pm.get(id); !present || !ok {
		t.Errorf("probe gauge = %v (present %v), want 1", ok, present)
	}

	// And a second bout is a second event.
	e.rs.set(errors.New("input/output error"))
	for range 4 {
		e.tick()
	}
	if n := len(e.events.reasons(ReasonVolumeUnhealthy)); n != 2 {
		t.Errorf("%d VolumeUnhealthy events after a second bout, want 2", n)
	}
}

func TestProbeReadsTheDeviceTheItemNames(t *testing.T) {
	t.Parallel()
	e := newProbeEnv(t)
	e.tick()
	if e.rs.n() != 1 || e.rs.calls[0] != "/dev/probe-test" {
		t.Fatalf("reads = %v", e.rs.calls)
	}
}

func TestStuckProbeNeverBlocksThePassAndIsOneInFlight(t *testing.T) {
	t.Parallel()
	e := newProbeEnv(t)
	e.rs.block = make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 4 {
			e.pass()
			e.r.gens.wait()
			e.clock.Advance(6 * time.Second)
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("a pass waited for a read that does not return")
	}
	if n := e.rs.n(); n != 1 {
		t.Errorf("%d reads started for one volume, want 1 in flight", n)
	}
	// 24 s of fake time: the stuck read counted as two timeouts.
	e.pass()
	if abnormal, msg := e.r.VolumeCondition(e.in.VolumeID); !abnormal || msg != probe.MessageTimeout {
		t.Errorf("VolumeCondition = %v, %q after two timeouts", abnormal, msg)
	}
	close(e.rs.block)
	e.r.probes.Wait()
}

func TestProbeStateIsDroppedWhenTheVolumeIsUnstaged(t *testing.T) {
	t.Parallel()
	e := newProbeEnv(t)
	e.rs.set(errors.New("input/output error"))
	for range 3 {
		e.tick()
	}
	if abnormal, _ := e.r.VolumeCondition(e.in.VolumeID); !abnormal {
		t.Fatal("not abnormal")
	}
	req := &request{kind: reqUnstage, volume: e.in.VolumeID, done: make(chan error, 1)}
	e.r.runPass(context.Background(), []*request{req})
	mustNil(t, "Unstage", <-req.done)
	e.r.probes.Wait()
	if abnormal, _ := e.r.VolumeCondition(e.in.VolumeID); abnormal {
		t.Error("an unstaged volume still reports a condition")
	}
	if _, present := e.pm.get(e.in.VolumeID); present {
		t.Error("an unstaged volume still has a probe gauge")
	}
}

func TestNoProbeUnlessConfigured(t *testing.T) {
	t.Parallel()
	e := newGenEnv(t)
	e.tick()
	if abnormal, _ := e.r.VolumeCondition(e.in.VolumeID); abnormal {
		t.Error("a condition without a probe")
	}
}

// Decision 53: nothing is repaired automatically, so the abnormal condition
// tells the operator what to do, and says when the store is the reason.
func TestAbnormalConditionTellsTheOperatorToRestartThePod(t *testing.T) {
	t.Parallel()
	e := newProbeEnv(t)
	e.rs.set(errors.New("input/output error"))
	for range 3 {
		e.tick()
	}
	_, msg := e.r.VolumeCondition(e.in.VolumeID)
	for _, want := range []string{"restart the volume's pod", "store is reachable", "cache has drained", "read-only until it is remounted"} {
		if !strings.Contains(msg, want) {
			t.Errorf("message %q lacks %q", msg, want)
		}
	}
	if strings.Contains(msg, "unreachable") {
		t.Errorf("message %q blames the store while it answers", msg)
	}

	dir, err := e.in.Transport.MountPoint(e.r.opts.TransportsRoot)
	mustNil(t, "MountPoint", err)
	e.r.up[dir] = false
	if got := e.r.storeNote(e.in); got != StoreUnreachable {
		t.Errorf("storeNote = %q with the transport down", got)
	}
	e.r.probeNote.Store(e.in.VolumeID, StoreUnreachable)
	if _, msg := e.r.VolumeCondition(e.in.VolumeID); !strings.Contains(msg, StoreUnreachable) {
		t.Errorf("message %q does not say the store is unreachable", msg)
	}
}

// A stack rebuilt in place ends the verdict on the old one: a read still in
// flight on the old device must not count against the new.
func TestRebuiltStackStartsTheProbeClean(t *testing.T) {
	t.Parallel()
	e := newProbeEnv(t)
	e.rs.set(errors.New("input/output error"))
	for range 3 {
		e.tick()
	}
	if abnormal, _ := e.r.VolumeCondition(e.in.VolumeID); !abnormal {
		t.Fatal("not abnormal")
	}
	e.rs.set(nil)
	e.g.set(func(g *genItem) { g.stack++ })
	e.tick()
	if abnormal, _ := e.r.VolumeCondition(e.in.VolumeID); abnormal {
		t.Error("the old stack's failures were carried to the new one")
	}
}
