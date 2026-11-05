package reconcile

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/blockd"
	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/item"
	"github.com/greghaskins/csi-miami/internal/transport"
	"github.com/greghaskins/csi-miami/internal/volume"
)

const genCache = 8 << 20 // the cache bound of the test volumes; the default watermark is half

// genItem stands in for the volume item: Converge does nothing, and the
// generation and status calls are scripted and recorded.
type genItem struct {
	mu       sync.Mutex
	status   blockd.Status
	statusEr error
	err      error         // what GenerationFor returns
	block    chan struct{} // when set, GenerationFor waits for it to close
	triggers []string
	running  int
	// freezeBound is what FreezeBound reports. A generation that is cancelled
	// while blocked takes thawDelay to "thaw" (and waits for stuck, when set,
	// ignoring its context) and records it in thawed.
	freezeBound time.Duration
	thawDelay   time.Duration
	stuck       chan struct{}
	thawed      atomic.Bool
	maxRun      int
	clock       *fakeClock
}

func (g *genItem) Kind() volume.Kind                              { return volume.KindVolume }
func (g *genItem) PublishSource(in intent.Intent) (string, error) { return in.StagePath, nil }
func (g *genItem) Converge(context.Context, intent.Intent, item.Observed) error {
	return nil
}
func (g *genItem) Teardown(context.Context, intent.Intent, item.Observed) error { return nil }

func (g *genItem) FreezeBound() time.Duration { return g.freezeBound }

func (g *genItem) Status(context.Context, intent.Intent) (blockd.Status, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.status, g.statusEr
}

func (g *genItem) GenerationFor(ctx context.Context, _ intent.Intent, trigger string) error {
	g.mu.Lock()
	g.triggers = append(g.triggers, trigger)
	g.running++
	g.maxRun = max(g.maxRun, g.running)
	block := g.block
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		g.running--
		g.mu.Unlock()
	}()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			if g.stuck != nil {
				<-g.stuck
			}
			time.Sleep(g.thawDelay)
			g.thawed.Store(true)
			return ctx.Err()
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.err != nil {
		return g.err
	}
	// A real publish leaves nothing dirty and stamps the time.
	g.status.Dirty = 0
	g.status.LastPublish = g.clock.Now()
	return nil
}

func (g *genItem) calls() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return slices.Clone(g.triggers)
}

func (g *genItem) set(f func(*genItem)) {
	g.mu.Lock()
	defer g.mu.Unlock()
	f(g)
}

// genEnv is an env whose only item is a genItem, with one staged volume.
type genEnv struct {
	*env
	g  *genItem
	in intent.Intent
}

func newGenEnv(t *testing.T, mods ...func(*Options)) *genEnv {
	t.Helper()
	g := &genItem{}
	e := newEnv(t, append([]func(*Options){func(o *Options) { o.Items = []item.Item{g} }}, mods...)...)
	g.clock = e.clock
	in := intent.Intent{
		Version:   intent.Version,
		VolumeID:  "volume:" + pvc1,
		Kind:      volume.KindVolume,
		Transport: transport.Spec{Kind: transport.KindLocal, Path: e.host("a")},
		StagePath: e.stage(pvc1), AccessMode: "SINGLE_NODE_WRITER",
	}
	in.Volume = &volume.VolumeParams{ChunkSize: 1 << 20, CacheSize: genCache, Size: genCache, Encryption: true,
		Cipher: "xchacha12,aes-adiantum-plain64", KeySize: 256,
		Generations: volume.GenerationParams{Interval: 5 * time.Minute, MaxAge: time.Hour, Keep: 3}}
	mustNil(t, "save", e.store.Save(in))
	return &genEnv{env: e, g: g, in: in}
}

// tick runs a pass and waits for the generation goroutines it started.
func (e *genEnv) tick() {
	e.t.Helper()
	e.pass()
	e.r.gens.wait()
}

func TestIntervalTriggersGeneration(t *testing.T) {
	t.Parallel()
	e := newGenEnv(t)
	e.g.set(func(g *genItem) { g.status.Dirty = 4096 })

	e.tick() // first seen: the interval starts now
	if got := e.g.calls(); len(got) != 0 {
		t.Fatalf("generated at once: %v", got)
	}
	e.clock.Advance(4 * time.Minute)
	e.tick()
	if got := e.g.calls(); len(got) != 0 {
		t.Fatalf("generated before the interval: %v", got)
	}
	e.clock.Advance(time.Minute)
	e.tick()
	if got := e.g.calls(); !slices.Equal(got, []string{"interval"}) {
		t.Fatalf("calls = %v, want one interval generation", got)
	}
	// The next interval counts from the success.
	e.g.set(func(g *genItem) { g.status.Dirty = 4096 })
	e.clock.Advance(4 * time.Minute)
	e.tick()
	e.clock.Advance(time.Minute)
	e.tick()
	if got := e.g.calls(); !slices.Equal(got, []string{"interval", "interval"}) {
		t.Fatalf("calls = %v, want two", got)
	}
}

func TestWatermarkTriggersEarly(t *testing.T) {
	t.Parallel()
	e := newGenEnv(t)
	e.g.set(func(g *genItem) { g.status.Dirty = genCache/2 - 1 })
	e.tick()
	if got := e.g.calls(); len(got) != 0 {
		t.Fatalf("generated below the watermark: %v", got)
	}
	e.g.set(func(g *genItem) { g.status.Dirty = genCache / 2 })
	e.tick()
	if got := e.g.calls(); !slices.Equal(got, []string{"watermark"}) {
		t.Fatalf("calls = %v, want a watermark generation", got)
	}
}

func TestExplicitWatermarkOverridesTheDefault(t *testing.T) {
	t.Parallel()
	e := newGenEnv(t)
	in := e.in
	vp := *in.Volume
	vp.Generations.DirtyWatermark = 1 << 20
	in.Volume = &vp
	mustNil(t, "save", e.store.Save(in))
	e.g.set(func(g *genItem) { g.status.Dirty = 1 << 20 })
	e.tick()
	if got := e.g.calls(); !slices.Equal(got, []string{"watermark"}) {
		t.Fatalf("calls = %v", got)
	}
}

func TestNoGenerationWhenClean(t *testing.T) {
	t.Parallel()
	e := newGenEnv(t)
	e.tick()
	for range 3 {
		e.clock.Advance(10 * time.Minute)
		e.tick()
	}
	if got := e.g.calls(); len(got) != 0 {
		t.Fatalf("a clean volume generated: %v", got)
	}
	if n := e.metrics.conditionCount(ConditionGenerationOverdue); n != 0 {
		t.Errorf("a clean, long-idle volume is overdue (%d)", n)
	}
}

// The annotation asks for one generation per new value, dirty or not, and a
// value already seen asks for nothing again.
func TestAnnotationTriggersOnce(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	value := ""
	var asked []string
	annotation := func(_ context.Context, pv, key string) (string, error) {
		mu.Lock()
		defer mu.Unlock()
		asked = append(asked, pv+" "+key)
		return value, nil
	}
	setValue := func(v string) { mu.Lock(); value = v; mu.Unlock() }
	e := newGenEnv(t, func(o *Options) { o.PVAnnotation = annotation })

	e.tick() // no annotation: nothing
	if got := e.g.calls(); len(got) != 0 {
		t.Fatalf("calls = %v with no annotation", got)
	}
	e.clock.Advance(time.Minute)
	setValue("a")
	e.tick()
	e.clock.Advance(time.Minute)
	e.tick()
	e.clock.Advance(time.Minute)
	e.tick()
	if got := e.g.calls(); !slices.Equal(got, []string{"request"}) {
		t.Fatalf("calls = %v, want one request", got)
	}
	setValue("")
	e.clock.Advance(time.Minute)
	e.tick()
	setValue("a") // the same value coming back is a new request: it differs from the last seen
	e.clock.Advance(time.Minute)
	e.tick()
	setValue("b")
	e.clock.Advance(time.Minute)
	e.tick()
	if got := e.g.calls(); !slices.Equal(got, []string{"request", "request", "request"}) {
		t.Fatalf("calls = %v, want three requests", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if want := pvc1 + " " + AnnotationPublish; asked[0] != want {
		t.Errorf("asked %q, want %q", asked[0], want)
	}
}

// Nothing is persisted: a restart sees the standing value as new and asks for
// one more generation. A generation is idempotent and publishes nothing when
// clean, and the alternative loses a request made while the plugin was down.
func TestAnnotationRetriggersOnceAfterRestart(t *testing.T) {
	t.Parallel()
	annotation := func(context.Context, string, string) (string, error) { return "a", nil }
	e := newGenEnv(t, func(o *Options) { o.PVAnnotation = annotation })
	e.tick()
	e.tick()
	if got := e.g.calls(); len(got) != 1 {
		t.Fatalf("calls = %v", got)
	}
	e.r = e.newReconciler(func(o *Options) { o.Items = []item.Item{e.g}; o.PVAnnotation = annotation })
	e.tick()
	e.tick()
	if got := e.g.calls(); len(got) != 2 {
		t.Fatalf("calls = %v, want exactly one more after the restart", got)
	}
}

// A request that could not be carried out (a failed or skipped generation, an
// unreadable annotation) is not forgotten.
func TestAnnotationRequestSurvivesAFailedGeneration(t *testing.T) {
	t.Parallel()
	annotation := func(context.Context, string, string) (string, error) { return "a", nil }
	e := newGenEnv(t, func(o *Options) { o.PVAnnotation = annotation })
	e.g.set(func(g *genItem) { g.err = errors.New("transport down") })
	e.tick()
	e.g.set(func(g *genItem) { g.err = nil })
	e.tick()
	e.tick()
	e.tick()
	if got := e.g.calls(); !slices.Equal(got, []string{"request", "request"}) {
		t.Fatalf("calls = %v, want the request retried once, then done", got)
	}
}

func TestAnnotationErrorIsNotARequest(t *testing.T) {
	t.Parallel()
	annotation := func(context.Context, string, string) (string, error) { return "", errors.New("api down") }
	e := newGenEnv(t, func(o *Options) { o.PVAnnotation = annotation })
	e.tick()
	e.tick()
	if got := e.g.calls(); len(got) != 0 {
		t.Fatalf("calls = %v", got)
	}
}

func TestOverdueConditionAfterMaxAge(t *testing.T) {
	t.Parallel()
	e := newGenEnv(t)
	start := e.clock.Now()
	e.g.set(func(g *genItem) {
		g.status.Dirty = 4096
		g.status.LastPublish = start
		g.err = errors.New("transport down") // every generation fails
	})
	overdue := func() int { return e.metrics.conditionCount(ConditionGenerationOverdue) }

	e.tick() // first seen
	e.clock.Advance(59 * time.Minute)
	e.tick()
	e.tick()
	if overdue() != 0 {
		t.Fatal("overdue before the max age")
	}
	e.clock.Advance(2 * time.Minute)
	e.tick()
	e.tick() // the pass after the status read reports it
	if overdue() != 1 {
		t.Fatalf("overdue = %d after the max age", overdue())
	}
	if n := len(e.events.reasons(ReasonGenerationOverdue)); n != 1 {
		t.Errorf("overdue events = %d, want 1", n)
	}
	e.tick()
	if n := len(e.events.reasons(ReasonGenerationOverdue)); n != 1 {
		t.Errorf("overdue events = %d after another pass, want 1 (a change, not every pass)", n)
	}
	// The stage itself still answers: overdue is reported, not an error.
	mustNil(t, "Stage", e.converge(e.in.VolumeID))
	e.r.gens.wait()

	// The next success clears it.
	e.g.set(func(g *genItem) { g.err = nil })
	e.tick()
	e.tick()
	if overdue() != 0 {
		t.Fatalf("overdue = %d after a success", overdue())
	}
}

// A volume that never published since the server began is measured from when
// the reconciler first saw it staged.
func TestNeverPublishedIsOverdueFromFirstSeen(t *testing.T) {
	t.Parallel()
	e := newGenEnv(t)
	e.g.set(func(g *genItem) { g.status.Dirty = 4096; g.err = errors.New("down") })
	e.tick()
	e.clock.Advance(61 * time.Minute)
	e.tick()
	e.tick()
	if e.metrics.conditionCount(ConditionGenerationOverdue) != 1 {
		t.Fatal("not overdue")
	}
}

func TestGenerationFailureRetriesNextTick(t *testing.T) {
	t.Parallel()
	e := newGenEnv(t)
	e.g.set(func(g *genItem) { g.status.Dirty = genCache; g.err = errors.New("transport down") })
	e.tick()
	e.tick()
	if got := e.g.calls(); !slices.Equal(got, []string{"watermark", "watermark"}) {
		t.Fatalf("calls = %v, want a retry on each tick", got)
	}
	e.g.set(func(g *genItem) { g.err = nil })
	e.tick()
	e.tick()
	if got := e.g.calls(); len(got) != 3 {
		t.Fatalf("calls = %v, want no more once it succeeded and is clean", got)
	}
	if n := e.metrics.conditionCount(ConditionOK); n != 1 {
		t.Errorf("ok volumes = %d, a failed generation is not a stage failure", n)
	}
}

// The item says a publish is in flight: not a failure, retried next tick, no
// event and no condition.
func TestSkippedGenerationIsNotAFailure(t *testing.T) {
	t.Parallel()
	e := newGenEnv(t)
	e.g.set(func(g *genItem) {
		g.status.Dirty = genCache
		g.err = fmt.Errorf("volume item x: %w", item.ErrGenerationSkipped)
	})
	e.tick()
	e.tick()
	e.g.set(func(g *genItem) { g.err = nil })
	e.tick()
	if got := e.g.calls(); len(got) != 3 {
		t.Fatalf("calls = %v, want a retry per tick until it ran", got)
	}
	if n := len(e.events.all()); n != 0 {
		t.Errorf("events = %v", e.events.all())
	}
	if n := e.metrics.conditionCount(ConditionOK); n != 1 {
		t.Errorf("ok volumes = %d", n)
	}
}

// One generation per volume at a time, and a slow one never holds up a pass.
func TestGenerationsSerializedPerVolume(t *testing.T) {
	t.Parallel()
	e := newGenEnv(t)
	release := make(chan struct{})
	e.g.set(func(g *genItem) { g.status.Dirty = genCache; g.block = release })

	e.pass()
	waitUntil(t, "the generation to start", func() bool { return len(e.g.calls()) == 1 })
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 5 {
			e.clock.Advance(10 * time.Minute)
			e.pass() // must return while the generation is blocked
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("a pass waited for a generation")
	}
	if got := e.g.calls(); len(got) != 1 {
		t.Fatalf("calls = %v: a second generation started while one ran", got)
	}
	close(release)
	e.r.gens.wait()
	e.tick()
	e.g.mu.Lock()
	defer e.g.mu.Unlock()
	if e.g.maxRun != 1 {
		t.Errorf("max concurrent generations = %d", e.g.maxRun)
	}
}

func TestNoGenerationForAVolumeBeingUnstaged(t *testing.T) {
	t.Parallel()
	e := newGenEnv(t)
	e.g.set(func(g *genItem) { g.status.Dirty = genCache })

	// A volume with the unstaging mark.
	marked := e.in
	marked.Unstaging = true
	mustNil(t, "save", e.store.Save(marked))
	e.tick()
	if got := e.g.calls(); len(got) != 0 {
		t.Fatalf("a marked volume generated: %v", got)
	}

	// A volume with an unstage in this very batch.
	mustNil(t, "save", e.store.Save(e.in))
	req := &request{kind: reqUnstage, volume: e.in.VolumeID, done: make(chan error, 1)}
	e.r.runPass(context.Background(), []*request{req})
	e.r.gens.wait()
	if err := <-req.done; err != nil {
		t.Logf("Unstage: %v", err)
	}
	if got := e.g.calls(); len(got) != 0 {
		t.Fatalf("a volume being unstaged generated: %v", got)
	}
}

// A volume whose stage failed has nothing to freeze.
func TestNoGenerationWhenTheStageFailed(t *testing.T) {
	t.Parallel()
	e := newGenEnv(t, func(o *Options) {
		o.Items = []item.Item{&failingGen{}}
	})
	e.tick()
	e.clock.Advance(time.Hour)
	e.tick()
	if e.metrics.conditionCount(ConditionFailed) != 1 {
		t.Fatalf("conditions = %v", e.metrics.conditions)
	}
}

type failingGen struct{ genItem }

func (*failingGen) Kind() volume.Kind { return volume.KindVolume }
func (*failingGen) Converge(context.Context, intent.Intent, item.Observed) error {
	return errors.New("nope")
}
func (*failingGen) GenerationFor(context.Context, intent.Intent, string) error {
	panic("generation of a volume whose stage failed")
}

func TestGenerationOptionsAreValidated(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	for _, tc := range []struct {
		name string
		g    GenerationOptions
		want string
	}{
		{"interval under the floor", GenerationOptions{Interval: 59 * time.Second}, "at least 1m"},
		{"max age under the interval", GenerationOptions{Interval: 10 * time.Minute, MaxAge: 5 * time.Minute}, "MaxAge"},
		{"negative", GenerationOptions{Interval: -time.Minute}, "at least 1m"},
		{"floor", GenerationOptions{Interval: time.Minute}, ""},
		{"zero", GenerationOptions{}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := Options{TransportsRoot: e.tRoot, StageRoot: e.sRoot, Generations: tc.g}
			_, err := New(e.store, e.sim, opts)
			switch {
			case tc.want == "" && err != nil:
				t.Fatalf("err = %v", err)
			case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
				t.Fatalf("err = %v, want one saying %q", err, tc.want)
			}
		})
	}
}

// The options are the defaults of a volume whose own parameters say nothing.
func TestOptionsGenerationsAreDefaults(t *testing.T) {
	t.Parallel()
	e := newGenEnv(t, func(o *Options) { o.Generations = GenerationOptions{Interval: 2 * time.Minute} })
	in := e.in
	vp := *in.Volume
	vp.Generations = volume.GenerationParams{}
	in.Volume = &vp
	mustNil(t, "save", e.store.Save(in))
	e.g.set(func(g *genItem) { g.status.Dirty = 4096 })
	e.tick()
	e.clock.Advance(2 * time.Minute)
	e.tick()
	if got := e.g.calls(); !slices.Equal(got, []string{"interval"}) {
		t.Fatalf("calls = %v", got)
	}
}

// The annotation is a call to the API server: once a minute per volume, not on
// every pass, and a new value is still honoured within the minute.
func TestAnnotationIsReadAtMostOncePerMinute(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	reads, value := 0, ""
	e := newGenEnv(t, func(o *Options) {
		o.PVAnnotation = func(context.Context, string, string) (string, error) {
			mu.Lock()
			defer mu.Unlock()
			reads++
			return value, nil
		}
	})
	count := func() int { mu.Lock(); defer mu.Unlock(); return reads }
	for range 5 {
		e.tick()
		e.clock.Advance(10 * time.Second)
	}
	if n := count(); n != 1 {
		t.Fatalf("reads = %d in 50 s of passes, want 1", n)
	}
	mu.Lock()
	value = "go"
	mu.Unlock()
	e.clock.Advance(10 * time.Second) // a minute since the first read
	e.tick()
	if n := count(); n != 2 {
		t.Fatalf("reads = %d after a minute, want 2", n)
	}
	if got := e.g.calls(); !slices.Equal(got, []string{"request"}) {
		t.Fatalf("calls = %v, want the new value honoured on that read", got)
	}
}

// A record that cannot be read for a pass is not a volume that left: its
// interval and its overdue clock go on.
func TestTimersSurviveATransientLoadFailure(t *testing.T) {
	t.Parallel()
	e := newGenEnv(t)
	e.g.set(func(g *genItem) { g.status.Dirty = 4096 })
	e.tick() // first seen: due in 5 minutes
	e.clock.Advance(4 * time.Minute)
	good := e.breakRecord(e.in.VolumeID)
	e.tick() // unreadable for a pass
	e.restoreRecord(e.in.VolumeID, good)
	e.clock.Advance(time.Minute)
	e.tick()
	if got := e.g.calls(); !slices.Equal(got, []string{"interval"}) {
		t.Fatalf("calls = %v: the interval was restarted by the unreadable record", got)
	}
	// An unstage really ends the state.
	if err := e.r.runUnstageForTest(e.in.VolumeID); err != nil {
		t.Logf("unstage: %v", err)
	}
	e.r.gens.wait()
	e.r.gens.mu.Lock()
	defer e.r.gens.mu.Unlock()
	if _, ok := e.r.gens.vols[e.in.VolumeID]; ok {
		t.Error("state kept for a volume that was unstaged")
	}
}

// Run does not return while a generation may hold a file system frozen: it
// cancels them and waits for their thaw.
func TestShutdownWaitsForTheThaw(t *testing.T) {
	t.Parallel()
	e := newGenEnv(t)
	e.g.set(func(g *genItem) {
		g.status.Dirty = genCache
		g.block = make(chan struct{}) // never released: only the cancellation ends it
		g.freezeBound = 5 * time.Second
		g.thawDelay = 100 * time.Millisecond
	})
	e.start()
	waitUntil(t, "the generation to start", func() bool { return len(e.g.calls()) == 1 })
	e.stop() // cancels Run and waits for it to return
	if !e.g.thawed.Load() {
		t.Fatal("Run returned before the generation had thawed")
	}
}

// The wait is the item's own freeze bound plus a margin, not a constant of the
// reconciler's, and a generation that outlasts it does not hold Run forever.
func TestShutdownWaitUsesTheItemsFreezeBound(t *testing.T) {
	t.Parallel()
	e := newGenEnv(t)
	stuck := make(chan struct{})
	t.Cleanup(func() { close(stuck) })
	e.g.set(func(g *genItem) {
		g.status.Dirty = genCache
		g.block = make(chan struct{})
		g.freezeBound = 300 * time.Millisecond
		g.stuck = stuck
	})
	e.r.stopMargin = 200 * time.Millisecond
	e.start()
	waitUntil(t, "the generation to start", func() bool { return len(e.g.calls()) == 1 })
	began := time.Now()
	e.stop()
	if d := time.Since(began); d < 450*time.Millisecond || d > 5*time.Second {
		t.Errorf("Run took %s to return, want about the item's bound plus the margin (500ms)", d)
	}
	if e.g.thawed.Load() {
		t.Error("the stuck generation finished; the test did not exercise the bound")
	}
}
