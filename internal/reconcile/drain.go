package reconcile

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/item"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// ReasonDrainStuck is a drain of a held volume that has been retrying for a
// long time without finishing; the log line beside it carries the cause.
const ReasonDrainStuck = "DrainStuck"

// drainState is the retry state of one orphan whose drain has not finished.
type drainState struct {
	since, next time.Time
	delay       time.Duration
	warned      bool
}

// drainOrphans is the last step of a start or timer pass before the garbage
// is collected: for each item kind that can drain (item.Drainer), the volumes
// it has node-local state for and this pass found no intent record of are
// drained (decision 52). Such a volume is held by this node, so no other node
// may take it over, and what only its cache holds would otherwise be lost to a
// volume that stays held for ever.
//
// Which volumes are orphans is decided here, from the intents: not staged and
// not marked as unstaging, with no record in the store now either (a Stage
// that landed since the pass loaded is the stage's to handle). Nothing is
// drained in a pass where a record could not be read, because it might be the
// volume's. A request-woken pass does not drain: a drain can take minutes
// (it publishes a generation) and kubelet is waiting behind the loop.
//
// A failure leaves everything as the item left it and is retried with the
// backoff of a marked unstage (MarkRetry, doubling to MarkRetryMax), and
// raises ReasonDrainStuck once after MarkWarnAfter.
func (p *pass) drainOrphans() {
	r := p.r
	due := r.drainDue
	r.drainDue = false
	if !due || p.ctx.Err() != nil {
		return
	}
	if len(p.failures) > 0 {
		r.log.Debug("no orphan is drained in a pass where a record could not be read")
		return
	}
	kinds := make([]string, 0, len(r.items))
	for k, it := range r.items {
		if _, ok := it.(item.Drainer); ok {
			kinds = append(kinds, string(k))
		}
	}
	sort.Strings(kinds)
	seen := map[string]struct{}{}
	for _, k := range kinds {
		d := r.items[volume.Kind(k)].(item.Drainer)
		orphans, err := d.Orphans(p.ctx)
		if err != nil {
			r.log.Warn("could not list the volumes to drain", "kind", k, "error", err)
			continue
		}
		sort.Slice(orphans, func(i, j int) bool { return orphans[i].VolumeID < orphans[j].VolumeID })
		for _, in := range orphans {
			seen[in.VolumeID] = struct{}{}
			p.drainOne(d, in)
		}
	}
	for id := range r.orphans {
		if _, ok := seen[id]; !ok {
			delete(r.orphans, id) // gone: drained, or discarded by someone
		}
	}
}

func (p *pass) drainOne(d item.Drainer, in intent.Intent) {
	r := p.r
	if _, staged := p.intents[in.VolumeID]; staged {
		return
	}
	// Fresh, and not the pass's snapshot: a record that cannot be read is not
	// "no record".
	if _, ok, err := r.store.Get(in.VolumeID); err != nil || ok {
		return
	}
	now := r.opts.Now()
	ds := r.orphans[in.VolumeID]
	if ds == nil {
		ds = &drainState{since: now, next: now, delay: r.opts.MarkRetry}
		r.orphans[in.VolumeID] = ds
	}
	if now.Before(ds.next) {
		return
	}
	err := p.drainVolume(d, in)
	if err == nil {
		delete(r.orphans, in.VolumeID)
		delete(r.lastErr, in.VolumeID)
		return
	}
	r.metrics.IncReconcileError("drain")
	r.log.Warn("draining a volume this node holds did not finish; it is retried", "volume", in.VolumeID, "error", err)
	p.results[in.VolumeID] = &volumeResult{stage: err, publish: map[string]error{}}
	ds.next = now.Add(ds.delay)
	ds.delay = min(ds.delay*2, r.opts.MarkRetryMax)
	if !ds.warned && max(0, now.Sub(ds.since)) >= r.opts.MarkWarnAfter {
		ds.warned = true
		r.log.Warn("a drain has not finished; the volume stays with this node until its last writes are in the store", "volume", in.VolumeID, "since", ds.since, "lastError", err)
		r.events.Event(in.VolumeID, ReasonDrainStuck)
	}
}

// drainVolume mounts the volume's transport (the holder file and the store are
// on it) and drains.
func (p *pass) drainVolume(d item.Drainer, in intent.Intent) error {
	r := p.r
	dir, err := in.Transport.MountPoint(r.opts.TransportsRoot)
	if err != nil {
		return fmt.Errorf("volume %s: %w", in.VolumeID, err)
	}
	if err := p.ensureTransport(in, dir); err != nil {
		return fmt.Errorf("volume %s: %w", in.VolumeID, err)
	}
	if herr := p.hung[dir]; herr != nil {
		return fmt.Errorf("volume %s: %w: the transport stopped answering earlier in this pass: %w", in.VolumeID, ErrTransportUnavailable, herr)
	}
	err = d.Drain(p.ctx, in)
	if err != nil {
		p.noteHung(dir, err)
		if errors.Is(err, item.ErrUnstageIncomplete) {
			return err
		}
	}
	return err
}
