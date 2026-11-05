package reconcile

import (
	"context"
	"time"

	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// Probed is what the active probe needs of an item: the device a pod's file
// system sits on. The volume item implements it; an item that does not is
// never probed.
type Probed interface {
	// ProbeDevice returns the block device to read the first sector of (the
	// mapper, or the nbd device of a plain volume), and false when the volume
	// has none yet. It decides from memory and touches nothing.
	ProbeDevice(in intent.Intent) (string, bool)
}

// ProbeOptions turn the active probe on and tune it. The zero value probes
// with probe.Read, a timeout of probe.DefaultTimeout and two failures.
type ProbeOptions struct {
	// Read replaces probe.Read, for tests.
	Read func(ctx context.Context, path string) error
	// Timeout is how long a read may run before it counts as a failure.
	Timeout time.Duration
	// Failures is how many consecutive failures make a volume abnormal.
	Failures int
}

// probeTick is the probe half of a pass, after the volumes were converged: a
// volume that converged and whose item names a device gets a read started
// unless one is in flight, and the verdict the earlier reads reached is put
// in the volume's result, where the condition metric and the CSI node read
// it. The reads run in goroutines of the prober's; nothing here waits for one,
// so a hung device costs the pass nothing. A volume that is no longer
// converged, or no longer staged, loses its state, so a restage starts clean.
func (p *pass) probeTick() {
	r := p.r
	if r.probes == nil || p.ctx.Err() != nil {
		return
	}
	pm, _ := r.metrics.(ProbeMetrics)
	live := map[string]struct{}{}
	for _, id := range sortedIDs(p.intents) {
		in := p.intents[id]
		res, ok := p.results[id]
		if !ok || res.stage != nil || in.Kind != volume.KindVolume || in.Unstaging {
			continue
		}
		if _, leaving := p.unstaging[id]; leaving {
			continue
		}
		pr, ok := r.items[in.Kind].(Probed)
		if !ok {
			continue
		}
		dev, ok := pr.ProbeDevice(in)
		if !ok {
			continue
		}
		live[id] = struct{}{}
		st := r.probes.State(id)
		res.probeFailed = st.Abnormal
		if st.Known && pm != nil {
			pm.SetVolumeProbe(id, !st.Abnormal)
		}
		if st.Abnormal != r.probeBad[id] {
			r.probeBad[id] = st.Abnormal
			if st.Abnormal {
				note := r.storeNote(in)
				r.probeNote.Store(id, note)
				r.log.Warn("a volume's device failed its probe; restart its pod once the store is reachable and the cache has drained", "volume", id, "reason", st.Message, "store", note)
				if d, ok := r.events.(DetailEvents); ok && note != "" {
					d.EventDetail(id, ReasonVolumeUnhealthy, note)
				} else {
					r.events.Event(id, ReasonVolumeUnhealthy)
				}
			} else {
				r.log.Info("a volume's device answers its probe again", "volume", id)
			}
		}
		r.probes.Observe(id, dev)
	}
	for _, id := range r.probes.IDs() {
		if _, ok := live[id]; ok {
			continue
		}
		r.probes.Forget(id)
		delete(r.probeBad, id)
		r.probeNote.Delete(id)
		if pm != nil {
			pm.DeleteVolumeProbe(id)
		}
	}
}

// VolumeCondition reports whether the volume's device is abnormal by its
// active probe, and why (fixed text, no path). It is what the node's
// NodeGetVolumeStats answers with. A volume that is not probed is normal. It is
// safe to call from any goroutine.
func (r *Reconciler) VolumeCondition(volumeID string) (abnormal bool, message string) {
	if r.probes == nil {
		return false, ""
	}
	st := r.probes.State(volumeID)
	if st.Abnormal {
		if n, ok := r.probeNote.Load(volumeID); ok && n.(string) != "" {
			return true, st.Message + " (" + n.(string) + ")"
		}
	}
	return st.Abnormal, st.Message
}

// StoreUnreachable is the note for a volume whose transport did not answer its
// last probe.
const StoreUnreachable = "the store is currently unreachable"

// storeNote says what the reconciler knows about why: the volume's transport
// not answering. (A cache full of dirty chunks is not known here.)
func (r *Reconciler) storeNote(in intent.Intent) string {
	dir, err := in.Transport.MountPoint(r.opts.TransportsRoot)
	if err != nil {
		return ""
	}
	if up, known := r.up[dir]; known && !up {
		return StoreUnreachable
	}
	return ""
}
