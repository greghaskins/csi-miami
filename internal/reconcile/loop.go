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

	failures    []intent.LoadFailure     // the records that did not load in this pass
	unstaging   map[string]struct{}      // volumes with an unstage request carried out in this pass
	superseded  map[*request]bool        // unstage requests overtaken by a later stage or publish
	teardownErr map[string]error         // by volume, for the unstage requests
	results     map[string]*volumeResult // by volume, for the volumes that were converged
	dropErr     map[string]error         // by publish target, for the unpublish requests

	// collect is false in a pass where any record failed to load: only
	// explicit Unpublish drops are carried out (garbage.go).
	collect bool

	// conflicts holds the publish targets this pass found held by a mount the
	// driver did not make, by path.
	conflicts map[string]error

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
	r.starts.Add(1)
	defer func() {
		r.metrics.ObserveReconcile(time.Since(began))
		r.ends.Add(1)
	}()

	loaded, err := r.load()
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
		failures:    loaded.Failures,
		unstaging:   map[string]struct{}{},
		superseded:  map[*request]bool{},
		conflicts:   map[string]error{},
		teardownErr: map[string]error{},
		results:     map[string]*volumeResult{},
		dropErr:     map[string]error{},
		trs:         map[string]*trResult{},
		hung:        map[string]error{},
	}
	p.teardown(batch)
	p.convergeAll()
	p.reportIntents()
	p.reportConditions()
	p.collectGarbage(batch, len(loaded.Failures) == 0)
	p.answer(batch)
}

// answer delivers each request the result of its own step. A Stage hears
// about the transport and the item, a Publish about its own target, an
// Unpublish about the unmount of its target and an Unstage about the
// teardown: none hears about another request's step, so a stale publish of
// another pod cannot fail them.
func (p *pass) answer(batch []*request) {
	for _, req := range batch {
		var err error
		switch req.kind {
		case reqStage:
			err = p.stageAnswer(req)
		case reqPublish:
			err = p.publishAnswer(req)
		case reqUnpublish:
			err = p.dropErr[req.target]
		case reqUnstage:
			if p.superseded[req] {
				err = fmt.Errorf("%w: a later request wants %s staged", ErrSuperseded, req.volume)
			} else {
				err = p.teardownErr[req.volume]
			}
		}
		req.done <- err
	}
}

// stageAnswer is the answer to a Stage.
func (p *pass) stageAnswer(req *request) error {
	if err := p.unreadableAnswer(req.volume); err != nil {
		return err
	}
	if _, gone := p.unstaging[req.volume]; gone {
		return fmt.Errorf("%w: a later request unstaged %s", ErrSuperseded, req.volume)
	}
	res, ok := p.results[req.volume]
	if !ok {
		return p.absent(req.volume)
	}
	return res.stage
}

// publishAnswer is the answer to a Publish: its own target's outcome, or the
// stage's failure when the stage was not there to bind from.
func (p *pass) publishAnswer(req *request) error {
	if err := p.unreadableAnswer(req.volume); err != nil {
		return err
	}
	if _, gone := p.unstaging[req.volume]; gone {
		return fmt.Errorf("%w: a later request unstaged %s", ErrSuperseded, req.volume)
	}
	res, ok := p.results[req.volume]
	if !ok {
		return p.absent(req.volume)
	}
	if res.stage != nil {
		return fmt.Errorf("%q was not attempted because the stage of %s is not ready: %w", req.target, req.volume, res.stage)
	}
	err, found := res.publish[req.target]
	if !found {
		return fmt.Errorf("%q is not in the record of %s", req.target, req.volume)
	}
	return err
}

// unreadableAnswer is ErrIntentsUnknown for a volume with no record in this
// pass because its record could not be read, and nil otherwise. It is checked
// before anything that depends on the record, such as an unstage that found
// nothing to do for lack of one.
func (p *pass) unreadableAnswer(volumeID string) error {
	if _, recorded := p.intents[volumeID]; !recorded && p.unreadable(volumeID) {
		return p.absent(volumeID)
	}
	return nil
}

// absent is the error for a volume that has no record in this pass: unknown
// if it never was staged, but not if its record is there and could not be
// read, which is not the same thing and is retryable. A load failure whose
// file name does not say which volume it was could be this one, so it counts.
func (p *pass) absent(volumeID string) error {
	if p.unreadable(volumeID) {
		return fmt.Errorf("%w: the record of %s could not be read in this pass", ErrIntentsUnknown, volumeID)
	}
	return fmt.Errorf("%w: %s", ErrUnknownVolume, volumeID)
}

// unreadable reports whether a record that failed to load in this pass may be
// the volume's.
func (p *pass) unreadable(volumeID string) bool {
	for _, f := range p.failures {
		if f.VolumeID == "" || f.VolumeID == volumeID {
			return true
		}
	}
	return false
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
			r.log.Error("an intent record could not be loaded and is ignored; repair the file or, if the volume is gone, remove it. Removing the record of a volume still in use unmounts it",
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
		return nil, fmt.Errorf("%w: %w", ErrObserve, err)
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
