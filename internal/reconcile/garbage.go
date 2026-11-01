package reconcile

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/item"
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
//      there; the mount at StageRoot itself is never touched. The driver makes
//      no mount at a directory volume's stage path, so every mount below the
//      staging root is a stray; one at exactly a live stage path, or inside
//      one, is nevertheless left and reported (ErrStageConflict for the
//      volume), because the driver cannot show that it made it or that it
//      holds nothing a pod needs, and what it did not make is not its to
//      remove.
//      The one exception is a bind of the volume's own directory, which an
//      earlier build made: the item recognises it and unmounts it (see
//      internal/item/directory).
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
// What a person should do is repair the file or, if the volume is gone, remove
// it. Removing the record of a volume that is still in use unmounts it: with
// the record gone the volume has no intent, and its stage is garbage by rule
// (a) and its publishes by the derived half of rule (b) on the next pass.
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

	current := map[string]targetInfo{} // publish target → owner, over the intents
	var stages []string
	refs := map[string]struct{}{} // transport mount points the intents reference
	refsKnown := true
	for _, in := range p.intents {
		stages = append(stages, in.StagePath)
		for _, pub := range in.Publishes {
			current[pub.TargetPath] = targetInfo{volume: in.VolumeID, itemDir: r.itemDirOf(in), tdir: r.transportDirOf(in)}
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
		for t, info := range r.lastTargets {
			if _, claimed := current[t]; claimed {
				continue
			}
			if _, have := r.drops[t]; !have {
				r.drops[t] = dropped{volume: info.volume, itemDir: info.itemDir, tdir: info.tdir}
			}
		}
	}
	for _, req := range batch {
		if req.kind != reqUnpublish {
			continue
		}
		if owner, claimed := current[req.target]; claimed {
			// Publish and Unpublish refuse this at request time; a claim made
			// since is the same conflict.
			r.log.Warn("an unpublish names a target that another intent claims; leaving it mounted", "volume", req.volume, "claimed_by", owner.volume)
			p.dropErr[req.target] = fmt.Errorf("%w: %s is claimed by the publish of %s", ErrPublishConflict, req.target, owner.volume)
			continue
		}
		d := dropped{volume: req.volume, explicit: true}
		if in, known := p.intents[req.volume]; known {
			d.itemDir = r.itemDirOf(in)
			d.tdir = r.transportDirOf(in)
		} else {
			d.wide = true // no intent: nothing says which directory it should be a bind of
		}
		r.drops[req.target] = d
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
		for t, info := range current {
			r.lastTargets[t] = info
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
	for path := range r.warned {
		if _, failing := errs[path]; !failing && !within(r.opts.TransportsRoot, path) {
			delete(r.warned, path) // it came off, or is no longer wanted off
		}
	}
	p.finishDrops(batch, table, errs)
	if table != nil && failuresClean {
		p.collectIdleTransports(table, refs, refsKnown)
	}
}

// unmountRounds repeats: read the table, select the candidates, unmount them
// deepest first, until a round leaves the table as large as it found it.
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
		cands := p.candidates(table, claimedStage)
		if len(cands) == 0 {
			return table, nil
		}
		// Progress is the table getting smaller: a round only unmounts, so if
		// it did not shrink nothing came off. The count of candidates would
		// miss a mount that a removed cover has just revealed.
		if size := len(table); prev >= 0 && size >= prev {
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
				p.warnOnChange(path, err, "garbage mount could not be removed; the next pass retries")
				r.metrics.IncReconcileError("garbage")
				if errors.Is(err, mount.ErrTimeout) {
					r.metrics.IncObservationTimeout("garbage")
				}
			}
			return table, errs
		}
		prev = len(table)
		errs = map[string]error{}
		for _, c := range cands {
			if err := p.unmountCandidate(c); err != nil {
				if errors.Is(err, ErrPublishConflict) {
					p.refuse(c, err)
				} else {
					errs[c.path] = err
				}
				continue
			}
			if c.drop == nil || !c.drop.explicit {
				r.metrics.IncGarbageUnmount()
				if c.drop != nil {
					// A publish target of a volume the reconciler knew: an event
					// has something to attach to. A stray below the staging root
					// names no volume (the stage directory is a hash of the ID),
					// so it is logged and not made an event.
					r.log.Info("unmounted a publish target no intent claims", "path", c.path, "volume", c.drop.volume)
					r.events.Event(c.drop.volume, ReasonGarbageUnmounted)
				} else {
					r.log.Info("unmounted a mount below the staging root that no intent claims", "path", c.path)
				}
			}
		}
	}
}

// unmountCandidate unmounts one candidate. A stray below the staging root is
// unmounted outright (the area is the driver's own). A publish target goes
// through the same ownership check as an unstage (publishOwnership): it is
// unmounted, once, only if it is a bind of the volume's directory in its
// transport, whether it was named by an Unpublish or found by comparing
// passes. Anything else, a foreign mount at the target or one stacked on our
// bind, is ErrPublishConflict and is left.
func (p *pass) unmountCandidate(c candidate) error {
	r := p.r
	if c.drop == nil {
		return r.m.Unmount(p.ctx, c.path, 0)
	}
	if c.drop.tdir != "" {
		// Judged only with the transport mounted: without it the directory
		// resolves to whatever holds the transports root, and a bind of the
		// volume would be taken for a foreign mount. That is a transient the
		// next pass repairs, not a conflict, and the drop is kept.
		t, err := r.observe(p.ctx)
		if err != nil {
			return fmt.Errorf("%w: %w", item.ErrObserve, err)
		}
		if err := transportMounted(t, c.drop.tdir); err != nil {
			return err
		}
	}
	owned, what := r.publishOwnership(c.drop.itemDir, c.drop.wide)
	return item.UnmountIf(p.ctx, r.m, c.path, owned, what, ErrPublishConflict)
}

// refuse records that a publish target is held by something the driver did not
// make. It is logged once per path (further passes see no candidate: the drop
// is discarded here so it is not retried forever, a foreign mount being a
// person's to clear), counted, and the Unpublish that named it is answered
// with the conflict.
func (p *pass) refuse(c candidate, err error) {
	r := p.r
	delete(r.drops, c.path)
	p.conflicts[c.path] = err
	p.warnOnChange(c.path, err, "a publish target is held by a mount the driver did not make; it is left in place and not retried")
	r.metrics.IncReconcileError("garbage")
}

// itemDirOf is the directory every publish of the volume of in is a bind of
// (its item's PublishSource), or "" if the record cannot name one.
func (r *Reconciler) itemDirOf(in intent.Intent) string {
	it, ok := r.items[in.Kind]
	if !ok {
		return ""
	}
	dir, err := it.PublishSource(in)
	if err != nil {
		return ""
	}
	return dir
}

// transportDirOf is the mount point of the transport of in, or "".
func (r *Reconciler) transportDirOf(in intent.Intent) string {
	dir, err := in.Transport.MountPoint(r.opts.TransportsRoot)
	if err != nil {
		return ""
	}
	return dir
}

// transportMounted is ErrTransportUnavailable if nothing is mounted at the
// transport's mount point tdir, which makes a judgement about the volume's
// mounts (is this a bind of the volume's directory?) impossible until the next
// pass has mounted the transport again.
func transportMounted(t observe.Table, tdir string) error {
	if _, ok := t.At(tdir); ok {
		return nil
	}
	return fmt.Errorf("%w: the transport is not mounted, so the volume's mounts cannot be recognised as the driver's own and are left in place; the next pass mounts it again", ErrTransportUnavailable)
}

// publishOwnership is the ownership test for a publish target, and the words
// for what it should have been. There are two forms, and the second is wider
// on purpose.
//
// The usual form, for teardown, derived drops and an Unpublish of a volume
// with an intent: the target must be a bind of the volume's directory in its
// transport (itemDir) as the kernel reports binds (observe.Table.IsBindOf: the
// same device and root path, or on NFS the directory carried in the mount
// source), including a directory that has since been removed
// (observe.Table.IsBindOfIncludingDeleted).
// It is tested against the item directory and not the stage path so that it
// holds with the stage gone, and so that a bind of a different volume's
// directory, stacked on ours or put there by someone else, is not taken for
// ours. With itemDir unknown ("") nothing is owned.
//
// The wide form (wide is set), only for an explicit Unpublish of a volume that
// has no intent: the target may be a bind of any directory in one of the
// transports the driver mounts (the same device, with a root inside the
// transport's). It is wider because nothing identifies the volume any more,
// and because kubelet asked for exactly that path by name; a mount of any
// other file system is still not the driver's. It is never used for a drop the
// reconciler derived, which always knows the volume's directory.
func (r *Reconciler) publishOwnership(itemDir string, wide bool) (item.Owned, string) {
	if wide {
		return r.inTransport, "a bind of a directory in one of the driver's transports"
	}
	if itemDir == "" {
		return func(observe.Table, observe.Mount) (bool, error) { return false, nil },
			"a bind of the volume's directory (which is not known)"
	}
	return item.BindOf(itemDir), "a bind of the volume's directory " + itemDir
}

// inTransport is the wide form: top is a bind of a directory inside a transport
// the driver has mounted.
func (r *Reconciler) inTransport(t observe.Table, top observe.Mount) (bool, error) {
	base := filepath.Join(r.opts.TransportsRoot, "transports")
	for _, tm := range t {
		if filepath.Dir(tm.MountPoint) != base || tm.ID == top.ID {
			continue
		}
		if _, visible := t.At(tm.MountPoint); !visible {
			continue
		}
		if tm.Major == top.Major && tm.Minor == top.Minor && rootWithin(tm.Root, top.Root) {
			return true, nil
		}
	}
	return false, nil
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
// first, one entry per path.
func (p *pass) candidates(t observe.Table, claimedStage func(string) bool) []candidate {
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
	return out
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
		if cerr, refused := p.conflicts[req.target]; refused {
			p.dropErr[req.target] = fmt.Errorf("unpublishing %q: %w", req.target, cerr)
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
			p.warnOnChange(dir, err, "an idle transport could not be unmounted; the next pass retries")
			continue
		}
		delete(r.idleSince, dir)
		kind := r.reported[dir]
		if kind == "" {
			kind = "unknown" // unreferenced since before this process started
		}
		r.metrics.SetTransport(kind, filepath.Base(dir), false)
		r.metrics.IncGarbageUnmount()
		delete(r.warned, dir)
		delete(r.up, dir)
		r.log.Info("unmounted an idle transport", "dir", dir)
	}
	for dir := range r.idleSince {
		if !seen[dir] {
			delete(r.idleSince, dir)
		}
	}
	for dir := range r.warned {
		if within(r.opts.TransportsRoot, dir) && !seen[dir] {
			delete(r.warned, dir)
		}
	}
}

// dependents counts the other mounts that are made from the transport mount
// tm: same file system, rooted at or inside tm's root, not stacked at tm's
// own mount point. They are binds of the transport (a publish, or a stray
// below the staging root) that unmounting the transport would orphan from their path, and
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

// warnOnChange logs a failure to remove a mount at warn level the first time,
// and again only when its kind changes (busy, unresponsive, anything else),
// so a mount that stays busy for an hour is one line and not one per pass. The
// error counter still counts every pass.
func (p *pass) warnOnChange(path string, err error, msg string) {
	r := p.r
	kind := failureKind(err)
	if r.warned[path] == kind {
		r.log.Debug(msg, "path", path, "error", err)
		return
	}
	r.warned[path] = kind
	r.log.Warn(msg, "path", path, "error", err)
}

// failureKind names the kind of an unmount failure, without its wording.
func failureKind(err error) string {
	switch {
	case errors.Is(err, unix.EBUSY):
		return "busy"
	case errors.Is(err, mount.ErrTimeout), errors.Is(err, mount.ErrBusy):
		return "unresponsive"
	default:
		return "failed"
	}
}
