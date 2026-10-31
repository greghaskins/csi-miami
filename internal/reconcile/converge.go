package reconcile

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"

	"github.com/greghaskins/csi-miami/internal/deadline"
	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/item"
	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/observe"
	"github.com/greghaskins/csi-miami/internal/transport"
)

// teardown is the unstage half of a pass: for each volume with an unstage
// request, unmount its publishes, then its stage path (item.Teardown), then
// delete its record, stopping at the first failure so the record stays and
// the next Unstage retries. The transport is not touched. A volume with no
// record in this pass has nothing to undo.
//
// Every volume named by an unstage request is excluded from convergence in
// this pass, whether or not its teardown succeeded: converging it would bind
// the publishes that were just unmounted.
func (p *pass) teardown(batch []*request) {
	r := p.r
	for _, req := range batch {
		if req.kind != reqUnstage {
			continue
		}
		if _, done := p.unstaging[req.volume]; done {
			continue
		}
		p.unstaging[req.volume] = struct{}{}
		in, ok := p.intents[req.volume]
		if !ok {
			p.teardownErr[req.volume] = nil
			continue
		}
		err := p.teardownVolume(in)
		if err == nil {
			if derr := r.store.Delete(in.VolumeID); derr != nil {
				err = fmt.Errorf("removing the intent of %s after its mounts were removed: %w", in.VolumeID, derr)
			} else {
				delete(p.intents, in.VolumeID)
				delete(r.lastErr, in.VolumeID)
			}
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

// teardownVolume unmounts the publishes and then the stage path. Each unmount
// is without MNT_DETACH: a busy mount stays and is an error, because a lazy
// detach would leave a pod writing into a mount nobody tracks. Publishes are
// all tried even if one fails, so one stuck pod does not keep the others
// mounted; the stage path is only unmounted when every publish is gone, since
// the binds are of it.
func (p *pass) teardownVolume(in intent.Intent) error {
	r := p.r
	var errs []error
	for _, pub := range in.Publishes {
		if err := item.UnmountVisible(p.ctx, r.m, pub.TargetPath); err != nil {
			errs = append(errs, fmt.Errorf("unmounting the publish target %q: %w", pub.TargetPath, err))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	it, ok := r.items[in.Kind]
	if !ok {
		return fmt.Errorf("no item handles kind %q", in.Kind)
	}
	obs, err := r.observe(p.ctx)
	if err != nil {
		return err
	}
	return it.Teardown(p.ctx, in, item.Observed{Mounts: obs})
}

// convergeAll drives every intent not being unstaged toward its desired
// state, one at a time in volume ID order.
func (p *pass) convergeAll() {
	for _, id := range sortedIDs(p.intents) {
		if _, skip := p.unstaging[id]; skip {
			continue
		}
		in := p.intents[id]
		reason, err := p.convergeVolume(in)
		p.converged[id] = err
		p.noteVolume(id, reason, err)
	}
}

// convergeVolume brings one volume up: the transport, then the item (its
// directory and the stage bind), then each publish, each step a no-op when it
// is already true. A failing step ends the volume's convergence and nothing
// else, except that the publishes are independent of each other: a stale
// publish whose target directory is gone must not keep a healthy one from
// being bound, so all are tried and the errors joined.
//
// It returns the event reason for an error.
func (p *pass) convergeVolume(in intent.Intent) (reason string, err error) {
	r := p.r
	it, ok := r.items[in.Kind]
	if !ok {
		return ReasonMountFailed, p.fail("item", fmt.Errorf("volume %s: no item handles kind %q", in.VolumeID, in.Kind))
	}
	dir, err := in.Transport.MountPoint(r.opts.TransportsRoot)
	if err != nil {
		return ReasonMountFailed, p.fail("transport", fmt.Errorf("volume %s: %w", in.VolumeID, err))
	}

	if err := p.ensureTransport(in, dir); err != nil {
		return ReasonTransportUnavailable, fmt.Errorf("volume %s: %w", in.VolumeID, err)
	}
	if herr := p.hung[dir]; herr != nil {
		// Another volume on this transport timed out earlier in the pass.
		// Each attempt on a hung mount costs a full deadline and leaves a
		// goroutine stuck behind it, so the rest wait for the next pass.
		return ReasonTransportUnavailable, p.fail("transport", fmt.Errorf("volume %s: %w: the transport stopped answering earlier in this pass and is not tried again until the next: %w", in.VolumeID, ErrTransportUnavailable, herr))
	}

	obs, err := r.observe(p.ctx)
	if err != nil {
		return ReasonMountFailed, fmt.Errorf("volume %s: %w", in.VolumeID, err)
	}
	if err := it.Converge(p.ctx, in, item.Observed{Mounts: obs}); err != nil {
		p.noteHung(dir, err)
		return ReasonMountFailed, p.fail("item", err)
	}

	if len(in.Publishes) == 0 {
		return "", nil
	}
	obs, err = r.observe(p.ctx)
	if err != nil {
		return ReasonMountFailed, fmt.Errorf("volume %s: %w", in.VolumeID, err)
	}
	var errs []error
	for _, pub := range in.Publishes {
		if err := p.publish(in, pub, obs); err != nil {
			p.noteHung(dir, err)
			errs = append(errs, p.fail("publish", err))
		}
	}
	return ReasonMountFailed, errors.Join(errs...)
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
func (p *pass) noteVolume(id, reason string, err error) {
	r := p.r
	prev, had := r.lastErr[id]
	if err == nil {
		if had {
			delete(r.lastErr, id)
			r.log.Info("volume converged again", "volume", id)
		}
		return
	}
	text := err.Error()
	if had && prev == text {
		r.log.Debug("volume still not converged", "volume", id, "error", err)
		return
	}
	r.lastErr[id] = text
	r.log.Warn("volume did not converge", "volume", id, "error", err)
	r.events.Event(id, reason, text)
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

// publish makes one publish target a bind of the stage path with the
// requested read-only flag, from the table obs. A visible mount that is a
// bind of the stage path is adopted without a mount call when its flag
// already matches, and has the flag set with a remount (Mounter.Bind) when it
// does not; anything else visible at the target, or hidden under a parent's
// mount, is ErrPublishConflict and is left alone. When nothing is mounted
// there the leaf directory is created if it is missing, but only if its
// parent exists, and the stage path is bound on it.
func (p *pass) publish(in intent.Intent, pub intent.Publish, obs observe.Table) error {
	r := p.r
	target := pub.TargetPath
	if top, ok := obs.At(target); ok {
		same, err := obs.IsBindOf(target, in.StagePath)
		if err != nil {
			return fmt.Errorf("volume %s, publish %q: %w", in.VolumeID, target, err)
		}
		if !same {
			return fmt.Errorf("volume %s: %w: %s is mounted as %s from %q, which is not a bind of the stage path",
				in.VolumeID, ErrPublishConflict, target, top.FSType, top.Source)
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
			return fmt.Errorf("volume %s, publish %q: %w", in.VolumeID, target, err)
		}
	}
	err := r.m.Bind(p.ctx, in.StagePath, target, pub.ReadOnly)
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
