package reconcile

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/greghaskins/csi-miami/internal/deadline"
	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/item"
	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/observe"
	"github.com/greghaskins/csi-miami/internal/transport"
)

// teardown is the unstage half of a pass: for each volume with an unstage
// request, unmount its publishes, then undo its item (item.Teardown; for a
// directory it checks that nothing is mounted at the stage path), then delete
// its record, stopping at the first failure so the record stays and
// the next Unstage retries. The transport is not touched. A volume with no
// record in this pass has nothing to undo.
//
// Order within a batch is the order the requests were made, and a later Stage
// or Publish of a volume supersedes an earlier Unstage of it that is still
// waiting: a caller whose context ended leaves its Unstage in the queue, and
// the Stage that follows it is kubelet saying the volume is wanted again. The
// superseded Unstage is not carried out and is answered ErrSuperseded.
//
// The record is deleted only if it is still the one this pass loaded
// (Store.DeleteIf). A Stage or Publish that was recorded after the Load is not
// in the batch, so nothing else would tell teardown about it; if the record
// changed, the stage and publishes that were just taken down are brought back
// by the next pass, the Unstage is answered ErrSuperseded, and no newer intent
// is lost.
//
// Every volume with an Unstage that is carried out is excluded from
// convergence in this pass, whether or not its teardown succeeded: converging
// it would bind the publishes that were just unmounted.
func (p *pass) teardown(batch []*request) {
	r := p.r
	for i, req := range batch {
		if req.kind != reqUnstage {
			continue
		}
		if wantedAgain(batch[i+1:], req.volume) {
			p.superseded[req] = true
			continue
		}
		if _, done := p.unstaging[req.volume]; done {
			continue
		}
		p.unstaging[req.volume] = struct{}{}
		in, ok := p.intents[req.volume]
		if !ok {
			// Nothing recorded, so nothing to undo; unless the record is
			// there and could not be read in this pass, which is not "done".
			if p.unreadable(req.volume) {
				p.teardownErr[req.volume] = p.absent(req.volume)
			} else {
				p.teardownErr[req.volume] = nil
			}
			continue
		}
		err := p.teardownVolume(in)
		if err == nil {
			err = p.deleteRecord(in)
		}
		if err != nil {
			r.metrics.IncReconcileError("teardown")
			if errors.Is(err, mount.ErrTimeout) {
				r.metrics.IncObservationTimeout("teardown")
			}
			r.log.Warn("unstage did not complete", "volume", in.VolumeID, "error", err)
		}
		p.teardownErr[req.volume] = err
	}
}

// wantedAgain reports whether any of the requests asks for the volume to be
// staged or published.
func wantedAgain(later []*request, volumeID string) bool {
	for _, q := range later {
		if q.volume == volumeID && (q.kind == reqStage || q.kind == reqPublish) {
			return true
		}
	}
	return false
}

// deleteRecord deletes the record of a volume whose mounts are gone, if it is
// still the record teardown acted on.
func (p *pass) deleteRecord(in intent.Intent) error {
	r := p.r
	deleted, err := r.store.DeleteIf(in.VolumeID, in)
	if err != nil {
		return fmt.Errorf("removing the intent of %s after its mounts were removed: %w", in.VolumeID, err)
	}
	if deleted {
		delete(p.intents, in.VolumeID)
		delete(r.lastErr, in.VolumeID)
		return nil
	}
	if _, ok, gerr := r.store.Get(in.VolumeID); gerr == nil && !ok {
		delete(p.intents, in.VolumeID) // already gone
		return nil
	}
	return fmt.Errorf("%w: the record of %s changed while it was being unstaged, so it was kept", ErrSuperseded, in.VolumeID)
}

// teardownVolume unmounts the publishes and then tears down the item. Each
// unmount is without MNT_DETACH: a busy mount stays and is an error, because a
// lazy detach would leave a pod writing into a mount nobody tracks. Each is
// also once, and only if the table shows the mount to be a bind of what the
// intent says it should be (item.UnmountIf): a publish target must be a bind
// of the volume's directory in its transport (publishOwnership). Anything
// else is ErrPublishConflict, left in place and reported, so an Unstage can
// never unmount a mount the driver did not make; the price is that it fails
// until a person clears the conflict. A directory item has no mount at the
// stage path to take down, and a mount found there is ErrStageConflict, left
// in place too. The first publish that fails ends the teardown with that one
// error, so what reaches the caller has one meaning; the item is only torn
// down when every publish is gone. The transport stays mounted.
func (p *pass) teardownVolume(in intent.Intent) error {
	r := p.r
	if tdir := r.transportDirOf(in); tdir != "" {
		// Judging a bind of the volume, a publish or an old stage, needs the
		// transport in the table (see unmountCandidate).
		t, err := r.observe(p.ctx)
		if err != nil {
			return err
		}
		if _, staged := t.At(in.StagePath); staged || len(in.Publishes) > 0 {
			if err := transportMounted(t, tdir); err != nil {
				return err
			}
		}
	}
	for _, pub := range in.Publishes {
		owned, what := r.publishOwnership(r.itemDirOf(in), false)
		err := item.UnmountIf(p.ctx, r.m, pub.TargetPath, owned, what, ErrPublishConflict)
		if err != nil {
			// The first failure ends it: the stage is not unmounted while a
			// bind of it is in the way, and the error that reaches the caller
			// is one error with one meaning, not a join of several (Classify).
			return fmt.Errorf("unmounting the publish target %q: %w", pub.TargetPath, err)
		}
	}
	it, ok := r.items[in.Kind]
	if !ok {
		return fmt.Errorf("no item handles kind %q", in.Kind)
	}
	obs, err := r.observe(p.ctx)
	if err != nil {
		return err
	}
	err = it.Teardown(p.ctx, in, item.Observed{Mounts: obs})
	return err
}

// volumeResult is what one pass found out about one volume, split by what a
// request waits for. A request is answered with the result of its own step
// and never with another's: one publish whose pod is gone must not fail the
// Stage of the volume or the Publish of a different pod.
type volumeResult struct {
	// stage is the outcome of the transport and the item.
	// When it is not nil the publishes were not attempted.
	stage error
	// publish is the outcome of each publish, by target path. A pruned
	// publish (errTargetGone) is recorded here too.
	publish map[string]error
}

// errTargetGone marks a publish whose target's parent directory no longer
// exists. It never reaches a caller except as the answer to a Publish of that
// very target.
var errTargetGone = errors.New("the publish target's parent directory does not exist")

// convergeAll drives every intent not being unstaged toward its desired
// state, one at a time in volume ID order.
func (p *pass) convergeAll() {
	for _, id := range sortedIDs(p.intents) {
		if _, skip := p.unstaging[id]; skip {
			continue
		}
		res := p.convergeVolume(p.intents[id])
		p.results[id] = res
		p.noteVolume(id, res)
	}
}

// convergeVolume brings one volume up: the transport, then the item (for a
// directory, that its directory exists and the stage path is clear), then each
// publish, each step a no-op when it
// is already true. A failing step ends the volume's stage convergence, and the
// publishes are then not attempted. The publishes are independent of each
// other: one that fails does not keep another from being bound.
//
// A publish whose target's parent directory does not exist is not a failure.
// Kubelet owns that directory, and creates it before it publishes, so its
// absence means the pod is gone (a NodePublish that failed is never followed
// by a NodeUnpublish). Such a publish is removed from the record, logged at
// info level, and is not counted as an error: left in, it would be retried and
// reported on every pass for as long as the volume stays staged.
func (p *pass) convergeVolume(in intent.Intent) *volumeResult {
	res := &volumeResult{publish: map[string]error{}}
	if res.stage = p.convergeStage(in); res.stage != nil {
		return res
	}
	if len(in.Publishes) == 0 {
		return res
	}
	obs, err := p.r.observe(p.ctx)
	if err != nil {
		res.stage = fmt.Errorf("volume %s: %w", in.VolumeID, err)
		return res
	}
	dir, _ := in.Transport.MountPoint(p.r.opts.TransportsRoot) // convergeStage validated it
	for _, pub := range in.Publishes {
		err := p.publish(in, pub, obs)
		switch {
		case errors.Is(err, errTargetGone):
			p.prune(in, pub.TargetPath)
		case err != nil:
			p.noteHung(dir, err)
			p.count("publish", err)
		}
		res.publish[pub.TargetPath] = err
	}
	return res
}

// convergeStage is the transport and the item: everything a Stage waits for.
func (p *pass) convergeStage(in intent.Intent) error {
	r := p.r
	it, ok := r.items[in.Kind]
	if !ok {
		return p.fail("item", fmt.Errorf("volume %s: no item handles kind %q", in.VolumeID, in.Kind))
	}
	dir, err := in.Transport.MountPoint(r.opts.TransportsRoot)
	if err != nil {
		return p.fail("transport", fmt.Errorf("volume %s: %w", in.VolumeID, err))
	}
	if err := p.ensureTransport(in, dir); err != nil {
		return fmt.Errorf("volume %s: %w", in.VolumeID, err)
	}
	if herr := p.hung[dir]; herr != nil {
		// Another volume on this transport timed out earlier in the pass.
		// Each attempt on a hung mount costs a full deadline and leaves a
		// goroutine stuck behind it, so the rest wait for the next pass.
		return p.fail("transport", fmt.Errorf("volume %s: %w: the transport stopped answering earlier in this pass and is not tried again until the next: %w", in.VolumeID, ErrTransportUnavailable, herr))
	}
	obs, err := r.observe(p.ctx)
	if err != nil {
		return fmt.Errorf("volume %s: %w", in.VolumeID, err)
	}
	if err := it.Converge(p.ctx, in, item.Observed{Mounts: obs}); err != nil {
		p.noteHung(dir, err)
		return p.fail("item", err)
	}
	if in.EnsureItem {
		p.clearEnsureItem(in.VolumeID)
	}
	return nil
}

// clearEnsureItem records that the item has converged once, so that a pass
// that later finds its directory missing reports it and does not create it. A
// crash before this keeps the permission; a failure to write it is logged and
// leaves the permission in place for the next pass to clear.
func (p *pass) clearEnsureItem(volumeID string) {
	err := p.r.store.Update(volumeID, func(rec *intent.Intent) error {
		if !rec.EnsureItem {
			return errNoChange
		}
		rec.EnsureItem = false
		return nil
	})
	if err != nil && !errors.Is(err, errNoChange) {
		p.r.log.Warn("could not clear the permission to create the item directory", "volume", volumeID, "error", err)
	}
}

// errNoChange aborts a store Update that has nothing to write.
var errNoChange = errors.New("no change")

// prune removes a publish from the volume's record because its target's
// parent directory is gone. The check is repeated inside the store's
// read-modify-write: a Publish for the same target that made the directory
// again and recorded it after this pass loaded the record is kept.
func (p *pass) prune(in intent.Intent, target string) {
	r := p.r
	err := r.store.Update(in.VolumeID, func(rec *intent.Intent) error {
		if !hasPublish(*rec, target) {
			return errNoChange
		}
		if _, err := os.Stat(filepath.Dir(target)); !errors.Is(err, fs.ErrNotExist) {
			return errNoChange
		}
		kept := rec.Publishes[:0]
		for _, q := range rec.Publishes {
			if q.TargetPath != target {
				kept = append(kept, q)
			}
		}
		rec.Publishes = kept
		return nil
	})
	switch {
	case err == nil:
		r.log.Info("removed a publish whose target directory is gone; its pod no longer exists", "volume", in.VolumeID, "target", target)
	case errors.Is(err, errNoChange):
	default:
		r.log.Warn("could not remove a publish whose target directory is gone", "volume", in.VolumeID, "target", target, "error", err)
	}
}

// count counts a failed step. A timeout is counted as one too.
func (p *pass) count(step string, err error) {
	p.r.metrics.IncReconcileError(step)
	if errors.Is(err, mount.ErrTimeout) {
		p.r.metrics.IncObservationTimeout(step)
	}
}

// fail counts a failed step and returns err.
func (p *pass) fail(step string, err error) error {
	p.count(step, err)
	return err
}

// noteHung records that a transport timed out, or is held busy by a call that
// did, in this pass.
func (p *pass) noteHung(dir string, err error) {
	if errors.Is(err, mount.ErrTimeout) || errors.Is(err, mount.ErrBusy) {
		p.hung[dir] = err
	}
}

// noteVolume logs and emits an event when a volume's condition changes: it
// starts failing, fails differently, or recovers. A volume that keeps failing
// the same way is logged at debug level only, so a persistent fault is one
// event and one warning, not one per pass.
func (p *pass) noteVolume(id string, res *volumeResult) {
	r := p.r
	if p.ctx.Err() != nil {
		// Shutting down: the steps that failed were cancelled, not broken.
		return
	}
	reason, err := res.problem()
	prev, had := r.lastErr[id]
	if err == nil {
		if had {
			delete(r.lastErr, id)
			r.log.Info("volume converged again", "volume", id)
		}
		return
	}
	key := res.key()
	if had && prev == key {
		r.log.Debug("volume still not converged", "volume", id, "error", err)
		return
	}
	r.lastErr[id] = key
	r.log.Warn("volume did not converge", "volume", id, "error", err)
	r.events.Event(id, reason, err.Error())
}

// key names the condition of a volume by the kind of each thing that is wrong
// and where, never by the wording of an error, which can change from pass to
// pass while the condition does not: a hung transport is a timeout on one pass
// and "a previous call has not returned" on the next, and is one condition.
// Events and warnings fire when the key changes.
func (res *volumeResult) key() string {
	set := map[string]struct{}{}
	if res.stage != nil {
		set["stage:"+conditionOf(res.stage)] = struct{}{}
	}
	for _, err := range res.publish {
		if err != nil && !errors.Is(err, errTargetGone) {
			set["publish:"+conditionOf(err)] = struct{}{}
		}
	}
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

// conditionOf classifies an error by the sentinel it wraps. A transport or a
// path that does not answer, however it is first reported (a timeout, or busy
// because an earlier call is still stuck), is one condition.
func conditionOf(err error) string {
	switch {
	case errors.Is(err, mount.ErrTimeout), errors.Is(err, mount.ErrBusy):
		return "unresponsive"
	case errors.Is(err, ErrTransportConflict):
		return "transport-conflict"
	case errors.Is(err, ErrStageConflict):
		return "stage-conflict"
	case errors.Is(err, ErrPublishConflict):
		return "publish-conflict"
	case errors.Is(err, ErrItemMissing):
		return "item-missing"
	case errors.Is(err, ErrItemNotDirectory):
		return "item-not-directory"
	case errors.Is(err, ErrTransportUnavailable):
		return "transport-unavailable"
	default:
		return "failed"
	}
}

// problem names the event reason for what is wrong with the volume and joins
// everything that is, for logs and events. A publish that was pruned is not
// wrong.
func (res *volumeResult) problem() (string, error) {
	reason := ReasonMountFailed
	var errs []error
	if res.stage != nil {
		errs = append(errs, res.stage)
		if errors.Is(res.stage, ErrTransportUnavailable) || errors.Is(res.stage, ErrTransportConflict) {
			reason = ReasonTransportUnavailable
		}
	}
	targets := make([]string, 0, len(res.publish))
	for t := range res.publish {
		targets = append(targets, t)
	}
	sort.Strings(targets)
	for _, t := range targets {
		if err := res.publish[t]; err != nil && !errors.Is(err, errTargetGone) {
			errs = append(errs, err)
		}
	}
	return reason, errors.Join(errs...)
}

// ensureTransport makes sure the transport at dir is mounted: observed
// ours (adopted, no call), mounted if the mount point is empty, a conflict if
// anything else is there (never stacked over), unavailable if it cannot be
// observed or mounted. The answer is computed once per transport per pass and
// shared by the volumes on it.
func (p *pass) ensureTransport(in intent.Intent, dir string) error {
	if res, ok := p.trs[dir]; ok {
		return res.err
	}
	res := &trResult{}
	p.trs[dir] = res
	r := p.r
	spec := in.Transport
	canonical := spec.Canonical()

	tr, err := r.transportFor(spec)
	if err != nil {
		res.err = err
		p.count("transport", err)
		return res.err
	}
	st, err := tr.State(p.ctx)
	switch {
	case err != nil:
		res.err = fmt.Errorf("%w: observing %s: %w", ErrTransportUnavailable, canonical, err)
	case st == transport.StateOurs:
		res.mounted = true
	case st == transport.StateEmpty:
		if err := tr.Mount(p.ctx); err != nil {
			if errors.Is(err, mount.ErrConflict) {
				res.err = fmt.Errorf("%w: %w", ErrTransportConflict, err)
			} else {
				res.err = fmt.Errorf("%w: mounting %s: %w", ErrTransportUnavailable, canonical, err)
			}
		} else {
			res.mounted = true
			r.log.Info("transport mounted", "transport", canonical, "dir", dir)
		}
	default:
		res.err = fmt.Errorf("%w: %s is %s, not %s", ErrTransportConflict, dir, st, canonical)
	}
	r.metrics.SetTransport(spec.Kind, spec.ID(), res.mounted)
	r.reported[dir] = spec.Kind
	if res.err != nil {
		p.noteHung(dir, res.err)
		p.count("transport", res.err)
	}
	return res.err
}

// transportFor returns the Transport for spec, making it the first time. The
// object is kept because it carries the guard that stops a hung name lookup
// or path resolution from being repeated every pass; it holds no mount state.
// The cache is keyed by Spec and never evicted: the number of distinct
// transports a node sees is the number of StorageClasses in use, which is
// small and bounded by configuration, so M1 does not bound it further.
func (r *Reconciler) transportFor(spec transport.Spec) (transport.Transport, error) {
	if tr, ok := r.transports[spec]; ok {
		return tr, nil
	}
	tr, err := transport.New(spec, r.opts.TransportsRoot, r.m, r.opts.Transport)
	if err != nil {
		return nil, err
	}
	r.transports[spec] = tr
	return tr, nil
}

// publish makes one publish target a bind of the item's publish source (for a
// directory, the volume's directory in its transport) with the requested
// read-only flag, from the table obs. A visible mount that is a bind of the
// source (observe.Table.IsBindOf, which compares the NFS way, so that it holds
// on nfs3) is adopted without a mount call when its flag
// already matches, and has the flag set with a remount (Mounter.Bind) when it
// does not; anything else visible at the target, or hidden under a parent's
// mount, is ErrPublishConflict and is left alone. When nothing is mounted
// there the leaf directory is created if it is missing, but only if its
// parent exists, and the stage path is bound on it.
func (p *pass) publish(in intent.Intent, pub intent.Publish, obs observe.Table) error {
	r := p.r
	target := pub.TargetPath
	it, ok := r.items[in.Kind]
	if !ok {
		return fmt.Errorf("volume %s: no item handles kind %q", in.VolumeID, in.Kind)
	}
	source, err := it.PublishSource(in)
	if err != nil {
		return fmt.Errorf("volume %s, publish %q: %w", in.VolumeID, target, err)
	}
	if top, ok := obs.At(target); ok {
		same, err := obs.IsBindOf(target, source)
		if err != nil {
			return fmt.Errorf("volume %s, publish %q: %w", in.VolumeID, target, err)
		}
		if !same {
			return fmt.Errorf("volume %s: %w: %s is mounted as %s from %q, which is not a bind of the volume's source %s",
				in.VolumeID, ErrPublishConflict, target, top.FSType, top.Source, source)
		}
		if top.ReadOnly() == pub.ReadOnly {
			return nil
		}
	} else {
		for _, m := range obs {
			if m.MountPoint == target {
				return fmt.Errorf("volume %s: %w: %s has a mount that is hidden under another mount; clear it before publishing", in.VolumeID, ErrPublishConflict, target)
			}
		}
		if err := r.makeTarget(p.ctx, target); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("volume %s, publish %q: %w: %w", in.VolumeID, target, errTargetGone, err)
			}
			return fmt.Errorf("volume %s, publish %q: %w", in.VolumeID, target, err)
		}
	}
	err = r.m.Bind(p.ctx, source, target, pub.ReadOnly)
	if errors.Is(err, mount.ErrConflict) && !errors.Is(err, ErrPublishConflict) {
		err = fmt.Errorf("%w: %w", ErrPublishConflict, err)
	}
	if err != nil {
		return fmt.Errorf("volume %s, publish %q: %w", in.VolumeID, target, err)
	}
	return nil
}

// makeTarget creates the leaf directory of a publish target if it is missing.
// It does not create the parents: see Publish.
func (r *Reconciler) makeTarget(ctx context.Context, target string) error {
	what := fmt.Sprintf("creating the publish target %q", target)
	_, err := deadline.Run(ctx, r.opts.StatTimeout, what, &r.mkdir, "mkdir\x00"+target, func(context.Context) (struct{}, error) {
		err := os.Mkdir(target, publishDirMode)
		if errors.Is(err, fs.ErrExist) {
			if fi, serr := os.Stat(target); serr == nil && fi.IsDir() {
				return struct{}{}, nil
			}
		}
		return struct{}{}, err
	})
	return err
}
