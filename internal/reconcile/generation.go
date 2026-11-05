package reconcile

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/greghaskins/csi-miami/internal/blockd"
	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/item"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// Generator is what the generation timers need of an item: the volume item
// implements it. An item that does not is never asked for a generation.
type Generator interface {
	// GenerationFor makes one generation for the trigger (interval, watermark
	// or request). item.ErrGenerationSkipped is not a failure.
	GenerationFor(ctx context.Context, in intent.Intent, trigger string) error
	// FreezeBound is the longest the item leaves a file system frozen. Run
	// waits this long (plus a margin) for the generations to finish at exit.
	FreezeBound() time.Duration
	// Status reads the block server's status without changing anything.
	Status(ctx context.Context, in intent.Intent) (blockd.Status, error)
}

// annotationEvery is how often a volume's PV annotation is read.
const annotationEvery = time.Minute

// defaultStopMargin is added to the items' freeze bound when Run waits.
const defaultStopMargin = 2 * time.Second

// Generation triggers, as blockd.PublishArgs names them.
const (
	triggerInterval  = "interval"
	triggerWatermark = "watermark"
	triggerRequest   = "request"
)

// generations is the state of the generation timers: one entry per staged
// volume of a kind whose item can make generations.
//
// The pass starts a goroutine per volume that has none running and goes on; a
// generation is a freeze, a boundary and an upload to a transport that may be
// slow or dead, and none of it may hold up the pass, an unstage or a stage. A
// volume has at most one goroutine, which is the whole of the serialisation:
// a tick that finds it running does nothing, so a long generation also stops
// the volume's later triggers from piling up behind it.
type generations struct {
	mu   sync.Mutex
	vols map[string]*genState
	wg   sync.WaitGroup
}

// genState is what the timers remember of one volume. It is in memory only: a
// restart begins every interval afresh and sees a standing annotation value as
// new (see decide).
type genState struct {
	running bool
	since   time.Time // when the timers first saw the volume staged
	due     time.Time // when the interval next triggers
	// seen is the last value of the publish annotation read, "" for none.
	// requested holds a new value until a generation has carried it out.
	seen      string
	requested bool
	// annAt is when the annotation was last read; it is read at most once an
	// annotationEvery, so a request is honoured within about a minute.
	annAt time.Time
	// overdue is the verdict of the latest status read; reported is what the
	// loop last told the world.
	overdue, reported bool
	// lastErr is the text of the last failure, so a repeat is not logged
	// again at warn level.
	lastErr string
}

func newGenerations() *generations { return &generations{vols: map[string]*genState{}} }

// wait blocks until no generation goroutine runs. Tests use it.
func (g *generations) wait() { g.wg.Wait() }

// settings are a volume's timers: its record's, else the options'.
type genSettings struct {
	interval, maxAge time.Duration
	watermark        int64
}

func (r *Reconciler) genSettingsOf(in intent.Intent) genSettings {
	s := genSettings{interval: r.opts.Generations.Interval, maxAge: r.opts.Generations.MaxAge}
	if p := in.Volume; p != nil {
		if p.Generations.Interval > 0 {
			s.interval = p.Generations.Interval
		}
		if p.Generations.MaxAge > 0 {
			s.maxAge = p.Generations.MaxAge
		}
		s.watermark = p.Generations.DirtyWatermark
		if s.watermark == 0 {
			bound := p.CacheSize
			if bound <= 0 {
				bound = p.Size
			}
			s.watermark = bound / 2 // the design's default: half the cache bound
		}
	}
	// A volume never publishes more often than the floor, whatever its record
	// says, and never counts itself overdue before its next interval is up.
	s.interval = max(s.interval, volume.MinGenerationInterval)
	s.maxAge = max(s.maxAge, s.interval)
	return s
}

// generationTick is the generation half of a pass, after the volumes were
// converged: each volume that converged and whose item can make generations
// gets a goroutine unless one runs, and the verdict the goroutines reached
// about overdue volumes is reported. A volume being unstaged (a request in
// this batch, or the mark on its record) has none: the item refuses too, but
// nothing should be asked of it.
func (p *pass) generationTick() {
	r := p.r
	g := r.gens
	g.mu.Lock()
	defer g.mu.Unlock()
	for id := range g.vols {
		in, recorded := p.intents[id]
		_, leaving := p.unstaging[id]
		// A record that failed to load in this pass may be this volume's: its
		// clocks go on. The state ends when the volume is unstaged or gone.
		if (!recorded && !p.unreadable(id)) || in.Unstaging || leaving {
			delete(g.vols, id) // a restage starts its timers afresh
		}
	}
	if p.ctx.Err() != nil {
		return
	}
	now := r.opts.Now()
	for _, id := range sortedIDs(p.intents) {
		in := p.intents[id]
		res, ok := p.results[id]
		if !ok || res.stage != nil || in.Kind != volume.KindVolume {
			continue
		}
		gen, ok := r.items[in.Kind].(Generator)
		if !ok {
			continue
		}
		st := g.vols[id]
		if st == nil {
			st = &genState{since: now}
			st.due = now.Add(r.genSettingsOf(in).interval)
			g.vols[id] = st
		}
		p.reportOverdue(id, res, st)
		if st.running {
			continue
		}
		st.running = true
		g.wg.Add(1)
		go r.runGeneration(p.ctx, gen, in, st)
	}
}

// reportOverdue copies the goroutines' verdict into the volume's result, and
// raises an event and a log line when it changes.
func (p *pass) reportOverdue(id string, res *volumeResult, st *genState) {
	r := p.r
	res.overdue = st.overdue
	if st.overdue == st.reported || p.ctx.Err() != nil {
		return
	}
	st.reported = st.overdue
	if st.overdue {
		r.log.Warn("the newest generation of a volume is older than its maximum age and it holds unpublished writes", "volume", id)
		r.events.Event(id, ReasonGenerationOverdue)
		return
	}
	r.log.Info("a volume's generations are current again", "volume", id)
}

// runGeneration is a volume's goroutine: read the server's status, decide, and
// make at most one generation, all under one deadline.
func (r *Reconciler) runGeneration(parent context.Context, gen Generator, in intent.Intent, st *genState) {
	g := r.gens
	defer g.wg.Done()
	defer func() {
		g.mu.Lock()
		st.running = false
		g.mu.Unlock()
	}()
	timeout := r.opts.Generations.Timeout
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	status, err := gen.Status(ctx, in)
	if err != nil {
		// The server's being down is the item's condition, reported by Converge.
		r.log.Debug("could not read the block server's status for the generation timers", "volume", in.VolumeID, "error", err)
		return
	}
	set := r.genSettingsOf(in)
	now := r.opts.Now()

	// The annotation is a call to the API server, made outside the lock and
	// at most once an annotationEvery.
	g.mu.Lock()
	readAnn := r.opts.PVAnnotation != nil && now.Sub(st.annAt) >= annotationEvery
	if readAnn {
		st.annAt = now
	}
	g.mu.Unlock()
	var annotation string
	var annErr error
	if readAnn {
		annotation, annErr = r.readAnnotation(ctx, in)
	}

	g.mu.Lock()
	trigger := r.decide(st, set, status, now, readAnn, annotation, annErr)
	st.overdue = r.isOverdue(st, set, status, now)
	g.mu.Unlock()
	if trigger == "" {
		return
	}

	err = gen.GenerationFor(ctx, in, trigger)
	g.mu.Lock()
	defer g.mu.Unlock()
	switch {
	case err == nil:
		st.due = r.opts.Now().Add(set.interval)
		st.requested = false
		st.overdue = false
		st.lastErr = ""
	case errors.Is(err, item.ErrGenerationSkipped):
		// A publish is in flight (or the volume is leaving): not a failure,
		// and the next tick tries again.
		r.log.Debug("a generation was skipped", "volume", in.VolumeID, "trigger", trigger)
	default:
		text := err.Error()
		if text != st.lastErr {
			st.lastErr = text
			r.log.Warn("a generation failed; it is retried at the next tick", "volume", in.VolumeID, "trigger", trigger, "error", err)
		} else {
			r.log.Debug("a generation failed again", "volume", in.VolumeID, "trigger", trigger, "error", err)
		}
	}
}

// readAnnotation reads the publish annotation of the volume's PV. The PV has
// the volume's name (pvc-<uuid>).
func (r *Reconciler) readAnnotation(ctx context.Context, in intent.Intent) (string, error) {
	if r.opts.PVAnnotation == nil {
		return "", nil
	}
	id, err := in.ID()
	if err != nil {
		return "", err
	}
	v, err := r.opts.PVAnnotation(ctx, id.Name, AnnotationPublish)
	if err != nil {
		return "", fmt.Errorf("reading the publish annotation of %s: %w", id.Name, err)
	}
	return v, nil
}

// decide names the trigger that is due, "" for none; the caller holds g.mu.
//
// A value of the annotation that differs from the last seen asks for one
// generation, and the request stays until a generation has carried it out. The
// value is not persisted: after a restart a standing value reads as new and
// asks once more, which costs a freeze and publishes nothing when the volume
// is clean, where forgetting would lose a request made while the plugin was
// down. A removed annotation asks for nothing.
//
// A request publishes whatever the dirty count says, since someone asked. The
// watermark and the interval need dirty bytes: a clean volume has nothing to
// publish, and the count leaves out trimmed chunks, which is fine for a
// trigger (the final generation at unstage still takes them).
func (r *Reconciler) decide(st *genState, set genSettings, status blockd.Status, now time.Time, read bool, annotation string, annErr error) string {
	switch {
	case !read:
	case annErr != nil:
		r.log.Debug("could not read the publish annotation", "error", annErr)
	case annotation != st.seen:
		st.seen = annotation
		if annotation != "" {
			st.requested = true
		}
	}
	switch {
	case st.requested:
		return triggerRequest
	case set.watermark > 0 && status.Dirty >= set.watermark:
		return triggerWatermark
	case !now.Before(st.due):
		if status.Dirty > 0 {
			return triggerInterval
		}
		st.due = now.Add(set.interval) // clean: nothing to publish, look again later
	}
	return ""
}

// isOverdue reports whether the newest successful publish is older than the
// maximum age while there are writes it does not hold. A clean volume is never
// overdue, however long it has been idle. A server that has not published
// since it began (blockd keeps the time in memory) is measured from when the
// timers first saw the volume. On a cache resumed after a takeover LastPublish
// is the base manifest's time, written by whichever node published it, so a
// skewed clock there shifts the verdict by the skew.
func (r *Reconciler) isOverdue(st *genState, set genSettings, status blockd.Status, now time.Time) bool {
	if status.Dirty <= 0 {
		return false
	}
	last := status.LastPublish
	if last.IsZero() {
		last = st.since
	}
	return now.Sub(last) > set.maxAge
}

// validate checks the generation options and fills their defaults.
func (o *GenerationOptions) validate() error {
	switch {
	case o.Interval < 0 || (o.Interval > 0 && o.Interval < volume.MinGenerationInterval):
		return fmt.Errorf("reconcile: Generations.Interval must be at least %s (got %s)", volume.MinGenerationInterval, o.Interval)
	case o.MaxAge < 0:
		return errors.New("reconcile: Generations.MaxAge must not be negative")
	case o.Timeout < 0:
		return errors.New("reconcile: Generations.Timeout must not be negative")
	}
	if o.Interval == 0 {
		o.Interval = volume.DefaultGenerationEvery
	}
	if o.MaxAge == 0 {
		o.MaxAge = max(volume.DefaultGenerationMaxAge, o.Interval)
	}
	if o.MaxAge < o.Interval {
		return fmt.Errorf("reconcile: Generations.MaxAge (%s) must not be under Generations.Interval (%s)", o.MaxAge, o.Interval)
	}
	if o.Timeout == 0 {
		o.Timeout = DefaultGenerationTimeout
	}
	return nil
}

// waitForGenerations is Run's last step: the generations' contexts are already
// cancelled, and each goroutine thaws its file system as it returns, so the
// process must not exit before they have. It waits for the items' longest
// freeze bound plus a margin, and says so if that passes: a file system left
// frozen stays so until the next start thaws it.
func (r *Reconciler) waitForGenerations() {
	var bound time.Duration
	for _, it := range r.items {
		if g, ok := it.(Generator); ok {
			bound = max(bound, g.FreezeBound())
		}
	}
	done := make(chan struct{})
	go func() { r.gens.wait(); close(done) }()
	t := time.NewTimer(bound + r.stopMargin)
	defer t.Stop()
	select {
	case <-done:
	case <-t.C:
		r.log.Error("a generation was still running when the reconciler stopped; its file system may stay frozen until the next start", "waited", bound+r.stopMargin)
	}
}
