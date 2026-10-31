package reconcile

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/observe"
)

// pass is the state of one run through the loop. It lives on the loop
// goroutine only.
type pass struct {
	r   *Reconciler
	ctx context.Context

	// intents are the records this pass acts on: what Load returned, less the
	// volumes whose unstage succeeded in it.
	intents map[string]intent.Intent

	unstaging   map[string]struct{} // volumes with an unstage request in this pass
	teardownErr map[string]error    // by volume, for the unstage requests
	converged   map[string]error    // by volume, for the volumes that were converged
	dropErr     map[string]error    // by publish target, for the unpublish requests

	// collect is false in a pass where any record failed to load: only
	// explicit Unpublish drops are carried out (garbage.go).
	collect bool

	trs  map[string]*trResult // by transport mount point: one observation per pass
	hung map[string]error     // transport mount point → the timeout it showed in this pass
}

// trResult is what one pass found out about one transport.
type trResult struct {
	err     error
	mounted bool
}

// runPass runs one pass over every intent and then collects garbage, and
// answers the requests that were taken for it. It is the only place that
// changes mounts on behalf of the node's volumes.
//
// The order matters and is the reason for several decisions below:
//
//  1. Load the intents. If that fails, the intents are unknown, not empty:
//     nothing is converged, nothing is collected, and every request fails with
//     ErrIntentsUnknown. Collecting on an unreadable store would unmount every
//     live volume.
//  2. Tear down the volumes that asked to be unstaged, before converging, so a
//     volume being removed is not rebuilt in the same pass.
//  3. Converge every other intent, sequentially, a volume's failure stopping
//     that volume and nothing else.
//  4. Collect garbage (garbage.go), from the intents as they stand after 2.
func (r *Reconciler) runPass(ctx context.Context, batch []*request) {
	began := time.Now()
	defer func() { r.metrics.ObserveReconcile(time.Since(began)) }()

	loaded, err := r.store.Load()
	if err != nil {
		r.metrics.IncReconcileError("load")
		r.log.Error("reading the intent store failed; converging and collecting nothing", "error", err)
		wrapped := fmt.Errorf("%w: %w", ErrIntentsUnknown, err)
		for _, req := range batch {
			req.done <- wrapped
		}
		return
	}
	r.noteLoadFailures(loaded.Failures)

	p := &pass{
		r:           r,
		ctx:         ctx,
		intents:     loaded.Intents,
		unstaging:   map[string]struct{}{},
		teardownErr: map[string]error{},
		converged:   map[string]error{},
		dropErr:     map[string]error{},
		trs:         map[string]*trResult{},
		hung:        map[string]error{},
	}
	p.teardown(batch)
	p.convergeAll()
	p.reportIntents()
	p.collectGarbage(batch, len(loaded.Failures) == 0)
	p.answer(batch)
}

// answer delivers each request its result.
func (p *pass) answer(batch []*request) {
	for _, req := range batch {
		var err error
		switch req.kind {
		case reqConverge:
			e, ok := p.converged[req.volume]
			if !ok {
				e = fmt.Errorf("%w: %s", ErrUnknownVolume, req.volume)
			}
			err = e
		case reqUnpublish:
			err = p.dropErr[req.target]
		case reqUnstage:
			err = p.teardownErr[req.volume]
		}
		req.done <- err
	}
}

// noteLoadFailures reports the records that did not load: the gauge every
// pass, and a log line the first time each one is seen (the file stays on
// disk, so it would otherwise be logged every pass).
func (r *Reconciler) noteLoadFailures(failures []intent.LoadFailure) {
	r.metrics.SetIntentLoadFailures(len(failures))
	now := make(map[string]struct{}, len(failures))
	for _, f := range failures {
		now[f.Path] = struct{}{}
		if _, seen := r.failedLoad[f.Path]; !seen {
			r.log.Error("an intent record could not be loaded and is ignored; fix or remove the file",
				"path", f.Path, "volume", f.VolumeID, "error", f.Err)
		}
	}
	r.failedLoad = now
}

// reportIntents sets the intent gauges. A kind that had intents and has none
// reads zero.
func (p *pass) reportIntents() {
	r := p.r
	counts := map[string]int{}
	for _, in := range p.intents {
		counts[string(in.Kind)]++
	}
	for k := range r.kinds {
		if _, ok := counts[k]; !ok {
			r.metrics.SetIntents(k, 0)
		}
	}
	for k, n := range counts {
		r.metrics.SetIntents(k, n)
		r.kinds[k] = struct{}{}
	}
	for v := range r.lastErr {
		if _, ok := p.intents[v]; !ok {
			delete(r.lastErr, v)
		}
	}
}

// observe reads the mount table. Reading it does not touch any mounted file
// system, so it cannot hang on a dead server, but it runs under the Mounter's
// deadline like every other call.
func (r *Reconciler) observe(ctx context.Context) (observe.Table, error) {
	t, err := r.m.Mounts(ctx)
	if err != nil {
		r.metrics.IncReconcileError("observe")
		if errors.Is(err, mount.ErrTimeout) {
			r.metrics.IncObservationTimeout("observe")
		}
		return nil, fmt.Errorf("reading the mount table: %w", err)
	}
	return t, nil
}

// sortedIDs returns the keys of m in order, so passes are deterministic.
func sortedIDs(m map[string]intent.Intent) []string {
	ids := make([]string, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
