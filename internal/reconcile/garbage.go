package reconcile

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/observe"
)

// Garbage collection removes mounts that no intent claims. It is the part of
// the reconciler that can unmount something a pod is using, so what counts as
// garbage is deliberately narrow, and each rule says why.
//
// A mount is garbage in exactly these cases, and no others:
//
//  (a) It is strictly below Options.StageRoot, it is visible, and it is
//      neither a volume's stage path nor inside one. StageRoot is the
//      driver's own staging directory, so nothing else has business mounting
//      there; the mount at StageRoot itself is never touched. A mount inside
//      a live stage path is something a pod or an operator put there and is
//      left alone.
//
//  (b) It is at exactly a publish target that no intent claims now and that
//      the reconciler has reason to believe it made: the target was named by
//      an Unpublish request ("explicit"), or it was claimed by an intent in
//      the previous pass and no longer is ("derived": the record was deleted
//      or its volume unstaged). Nothing else under kubelet's pod directories
//      is ever unmounted, whatever it looks like: kubelet and other drivers
//      mount there too, and a mount at a path the reconciler never heard of
//      is not its to remove. The previous pass's targets live in memory only,
//      so a restart forgets them: that leaves a leak where the alternative is
//      a wrong unmount, and kubelet's own retry of the Unpublish repairs it.
//
//  (c) It is a transport (a mount at exactly <TransportsRoot>/transports/<x>)
//      that no intent references, that nothing else is mounted from, and that
//      has been seen unreferenced for at least Options.TransportIdle. Zero,
//      the default, never collects transports: remounting an NFS export on the
//      next stage is slow and can fail, and holding it open costs nothing.
//      While any intent has a transport whose mount point cannot be computed
//      this is skipped, because the references are then unknown.
//
// Garbage is never collected in a pass whose Load of the intent store
// returned an error: the intents are then unknown, not empty (see runPass).
//
// A record that fails to load (corrupt, or one read that failed for a reason
// that may pass, such as running out of file descriptors) makes that volume's
// intent unknown, not absent, and garbage is defined as "no intent". So in a
// pass where any record failed to load nothing is collected at all: not (a),
// not the derived half of (b), not (c). Only an explicit Unpublish drop still
// goes, because kubelet asked for that target by name. The failure is logged
// once per change of the set of failed records, counted in the load-failure
// gauge, and the file is kept; collection resumes on its own the first pass
// after every record reads, whether a person fixed it or the error passed.
// This is the cost of the rule: one record that stays broken suspends
// collection node-wide, which leaks mounts and does not unmount a pod's data.
//
// Unmounts are without MNT_DETACH, because a lazy detach would leave a pod
// writing into a mount nobody tracks and would hide a busy mount instead of
// reporting it. They go deepest path first, and the round repeats with a
// freshly read table until it removes nothing more, since a mount stacked
// under another is reachable only after the one above it is gone. A mount
// that stays (busy) is reported and retried by the next pass.

// candidate is a mount point that rule (a) or (b) selects.
type candidate struct {
	path string
	drop *dropped // nil for rule (a)
}

// collectGarbage applies rules (a) and (b), then (c). failuresClean says that
// no record failed to load in this pass.
func (p *pass) collectGarbage(batch []*request, failuresClean bool) {
	r := p.r

	current := map[string]string{} // publish target → volume, over the intents
	var stages []string
	refs := map[string]struct{}{} // transport mount points the intents reference
	refsKnown := true
	for _, in := range p.intents {
		stages = append(stages, in.StagePath)
		for _, pub := range in.Publishes {
			current[pub.TargetPath] = in.VolumeID
		}
		dir, err := in.Transport.MountPoint(r.opts.TransportsRoot)
		if err != nil {
			refsKnown = false
			continue
		}
		refs[dir] = struct{}{}
	}

	p.collect = failuresClean
	p.noteSuspended(!failuresClean)
	if failuresClean {
		for t, v := range r.lastTargets {
			if _, claimed := current[t]; claimed {
				continue
			}
			if _, have := r.drops[t]; !have {
				r.drops[t] = dropped{volume: v}
			}
		}
	}
	for _, req := range batch {
		if req.kind != reqUnpublish {
			continue
		}
		if v, claimed := current[req.target]; claimed {
			r.log.Warn("an unpublish names a target that another intent claims; leaving it mounted", "volume", req.volume, "claimed_by", v)
			continue
		}
		r.drops[req.target] = dropped{volume: req.volume, explicit: true}
	}
	for t := range r.drops {
		if _, claimed := current[t]; claimed {
			delete(r.drops, t)
		}
	}
	// The next pass compares against this one. While a record is failing to
	// load, keep what the previous passes knew as well, so the targets of the
	// unreadable volume are still remembered when it reads again or is deleted.
	if failuresClean {
		r.lastTargets = current
	} else {
		for t, v := range current {
			r.lastTargets[t] = v
		}
	}

	claimedStage := func(path string) bool {
		for _, s := range stages {
			if within(s, path) {
				return true
			}
		}
		return false
	}

	table, errs := p.unmountRounds(claimedStage)
	p.finishDrops(batch, table, errs)
	if table != nil && failuresClean {
		p.collectIdleTransports(table, refs, refsKnown)
	}
}

// unmountRounds repeats: read the table, select the candidates, unmount them
// deepest first, until a round leaves as many mounts to remove as it found.
// It returns the last table read (nil if the table could not be read) and,
// for the candidates still mounted at the end, the error of their last
// unmount attempt (or a note that the unmount reported success and the mount
// is still there).
func (p *pass) unmountRounds(claimedStage func(string) bool) (observe.Table, map[string]error) {
	r := p.r
	prev := -1
	var errs map[string]error
	for {
		table, err := r.observe(p.ctx)
		if err != nil {
			r.log.Warn("garbage collection could not read the mount table", "error", err)
			return nil, map[string]error{"": err}
		}
		cands, weight := p.candidates(table, claimedStage)
		if len(cands) == 0 {
			return table, nil
		}
		if prev >= 0 && weight >= prev {
			for _, c := range cands {
				if errs[c.path] == nil {
					errs[c.path] = fmt.Errorf("%s is still mounted after it was unmounted", c.path)
				}
			}
			for path, err := range errs {
				if !stillMounted(cands, path) {
					delete(errs, path)
					continue
				}
				r.log.Warn("garbage mount could not be removed; the next pass retries", "path", path, "error", err)
				r.metrics.IncReconcileError("garbage")
				if errors.Is(err, mount.ErrTimeout) {
					r.metrics.IncObservationTimeout("garbage")
				}
			}
			return table, errs
		}
		prev = weight
		errs = map[string]error{}
		for _, c := range cands {
			if err := r.m.Unmount(p.ctx, c.path, 0); err != nil {
				errs[c.path] = err
				continue
			}
			if c.drop == nil || !c.drop.explicit {
				v := ""
				if c.drop != nil {
					v = c.drop.volume
				}
				r.metrics.IncGarbageUnmount()
				r.log.Info("unmounted a mount no intent claims", "path", c.path, "volume", v)
				r.events.Event(v, ReasonGarbageUnmounted, "unmounted "+c.path+", which no intent claims")
			}
		}
	}
}

func stillMounted(cands []candidate, path string) bool {
	for _, c := range cands {
		if c.path == path {
			return true
		}
	}
	return false
}

// candidates selects the mounts of rules (a) and (b) in t, deepest path
// first, one entry per path, and returns how many mounts they stand for (a
// stack of mounts at one path counts each layer, so a round that pops one
// layer is progress).
func (p *pass) candidates(t observe.Table, claimedStage func(string) bool) ([]candidate, int) {
	r := p.r
	seen := map[string]bool{}
	var out []candidate
	strays := t.Below(r.opts.StageRoot)
	if !p.collect {
		strays = nil // rule (a) is off while any record is unreadable
	}
	for _, m := range strays {
		path := m.MountPoint
		if seen[path] || claimedStage(path) {
			continue
		}
		if _, visible := t.At(path); !visible {
			continue // hidden: nothing leads to it until the cover is gone
		}
		seen[path] = true
		out = append(out, candidate{path: path})
	}
	for path, d := range r.drops {
		if seen[path] || !p.collect && !d.explicit {
			continue
		}
		if _, visible := t.At(path); !visible {
			continue
		}
		seen[path] = true
		out = append(out, candidate{path: path, drop: &d})
	}
	sort.Slice(out, func(i, j int) bool {
		di, dj := strings.Count(out[i].path, "/"), strings.Count(out[j].path, "/")
		if di != dj {
			return di > dj
		}
		return out[i].path < out[j].path
	})
	weight := 0
	for _, m := range t {
		if seen[m.MountPoint] {
			weight++
		}
	}
	return out, weight
}

// finishDrops settles the drop set and the answers to the Unpublish requests
// after the rounds. A target that is no longer visible is done (success for
// the request, and it leaves the set); one that is still mounted keeps its
// entry, so the next pass retries it, and its request gets the error.
func (p *pass) finishDrops(batch []*request, t observe.Table, errs map[string]error) {
	r := p.r
	if t == nil {
		// The table could not be read: nothing is known to be unmounted.
		err := errs[""]
		for _, req := range batch {
			if req.kind == reqUnpublish {
				p.dropErr[req.target] = fmt.Errorf("unpublishing %q: %w", req.target, err)
			}
		}
		return
	}
	for path := range r.drops {
		if _, visible := t.At(path); !visible {
			delete(r.drops, path)
		}
	}
	for _, req := range batch {
		if req.kind != reqUnpublish {
			continue
		}
		if _, still := r.drops[req.target]; still {
			err := errs[req.target]
			if err == nil {
				err = errors.New("the target is still mounted")
			}
			p.dropErr[req.target] = fmt.Errorf("unpublishing %q: %w", req.target, err)
		}
	}
}

// collectIdleTransports is rule (c).
func (p *pass) collectIdleTransports(t observe.Table, refs map[string]struct{}, refsKnown bool) {
	r := p.r
	if r.opts.TransportIdle <= 0 || !refsKnown {
		clear(r.idleSince)
		return
	}
	base := filepath.Join(r.opts.TransportsRoot, "transports")
	now := r.opts.Now()
	seen := map[string]bool{}
	for _, m := range t {
		dir := m.MountPoint
		if filepath.Dir(dir) != base || seen[dir] {
			continue
		}
		top, visible := t.At(dir)
		if !visible {
			continue
		}
		seen[dir] = true
		if _, referenced := refs[dir]; referenced {
			delete(r.idleSince, dir)
			continue
		}
		since, known := r.idleSince[dir]
		if !known {
			r.idleSince[dir] = now
			continue
		}
		if now.Sub(since) < r.opts.TransportIdle {
			continue
		}
		if n := dependents(t, top); n > 0 {
			r.log.Debug("an idle transport is still in use and stays mounted", "dir", dir, "mounts", n)
			continue
		}
		if err := r.m.Unmount(p.ctx, dir, 0); err != nil {
			r.metrics.IncReconcileError("garbage")
			if errors.Is(err, mount.ErrTimeout) {
				r.metrics.IncObservationTimeout("garbage")
			}
			r.log.Warn("an idle transport could not be unmounted; the next pass retries", "dir", dir, "error", err)
			continue
		}
		delete(r.idleSince, dir)
		kind := r.reported[dir]
		if kind == "" {
			kind = "unknown" // unreferenced since before this process started
		}
		r.metrics.SetTransport(kind, filepath.Base(dir), false)
		r.metrics.IncGarbageUnmount()
		r.log.Info("unmounted an idle transport", "dir", dir)
		r.events.Event("", ReasonGarbageUnmounted, "unmounted the idle transport "+filepath.Base(dir))
	}
	for dir := range r.idleSince {
		if !seen[dir] {
			delete(r.idleSince, dir)
		}
	}
}

// dependents counts the other mounts that are made from the transport mount
// tm: same file system, rooted at or inside tm's root, not stacked at tm's
// own mount point. They are binds of the transport (a stage nobody claims, a
// publish) that unmounting the transport would orphan from their path, and
// the idle timeout waits for them to go. For a transport that is the whole
// export every mount of that file system counts; for one that is a host
// directory, mounts of the host file system outside that directory do not.
func dependents(t observe.Table, tm observe.Mount) int {
	n := 0
	for _, m := range t {
		if m.ID == tm.ID || m.MountPoint == tm.MountPoint || m.Deleted {
			continue
		}
		if m.Major != tm.Major || m.Minor != tm.Minor {
			continue
		}
		if rootWithin(tm.Root, m.Root) {
			n++
		}
	}
	return n
}

// rootWithin reports whether path is dir or lies below it, where dir may be
// "/".
func rootWithin(dir, path string) bool {
	return dir == "/" || path == dir || strings.HasPrefix(path, dir+"/")
}

// noteSuspended logs when collection is suspended or resumes, once per
// change, so a record that stays broken is one line and not one per pass.
func (p *pass) noteSuspended(suspended bool) {
	r := p.r
	if suspended == r.suspended {
		return
	}
	r.suspended = suspended
	if suspended {
		r.log.Warn("garbage collection is suspended while any intent record cannot be read; mounts are left alone")
	} else {
		r.log.Info("every intent record reads again; garbage collection resumes")
	}
}
