package reconcile

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/greghaskins/csi-miami/internal/deadline"
	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/item"
	"github.com/greghaskins/csi-miami/internal/item/directory"
	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/transport"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// publishDirMode is the mode of a publish target directory the reconciler
// creates when a Publish request arrives and kubelet has not made it.
const publishDirMode = 0o700

// reqKind says what a queued request waits for.
type reqKind int

const (
	// reqStage waits for the volume's transport and stage bind (Stage).
	reqStage reqKind = iota
	// reqPublish waits for one publish target to be bound (Publish).
	reqPublish
	// reqUnpublish waits for one publish target to be unmounted.
	reqUnpublish
	// reqUnstage waits for the volume's teardown.
	reqUnstage
)

// request is one caller's wait for a pass. done is buffered, so the loop never
// blocks on a caller that has gone away.
type request struct {
	kind   reqKind
	volume string
	target string // reqPublish and reqUnpublish: the publish target path
	done   chan error
}

// Reconciler is the node agent's converge loop. Build one with New, start it
// with Run, and let the CSI handlers drive it through Stage, Unstage, Publish
// and Unpublish.
//
// # What it believes
//
// The kernel is the only truth about what is mounted: every pass reads the
// mount table and nothing about mounts is remembered between passes. What
// should be true comes from the intent store, which is read afresh on every
// pass. The few things kept in memory are not beliefs about the kernel: the
// publish targets of the previous pass and the targets an Unpublish asked to
// drop (garbage.go says how they are used), when an unreferenced transport was
// first seen (for the idle timeout), the text of the last error per volume (so
// events fire on a change, not on every pass), and the Transport objects
// (which carry the guard against piling up lookups on a hung resolver). Losing
// any of them, as a restart does, makes the reconciler more conservative,
// never less.
//
// # Requests and passes
//
// One goroutine, Run, converges. Every request writes its intent first, so
// the desired state is on disk before anyone waits, then asks the loop for a
// pass and waits for the pass that includes it or for its context. A caller
// whose context ends first leaves the request in the queue: the pass still
// runs, and the retry the caller will make finds it done, which is why every
// request is idempotent. Requests for different volumes do not block each
// other at the request layer; the loop is sequential, so a pass that is busy
// delays the next one, never the one after.
type Reconciler struct {
	store *intent.Store
	m     mount.Mounter
	opts  Options

	items   map[volume.Kind]item.Item
	log     *slog.Logger
	metrics Metrics
	events  Events

	locks keyedLock    // serialises requests for one volume
	mkdir deadline.Set // Publish's creation of a target directory, by path

	mu      sync.Mutex
	queue   []*request
	started bool
	stopped bool
	wake    chan struct{} // capacity 1: a pass is wanted

	// Loop-only state: touched by runPass and Run, never by a request.
	transports  map[transport.Spec]transport.Transport
	lastTargets map[string]targetInfo // publish targets of the previous pass
	drops       map[string]dropped    // targets to unmount once they are no longer claimed
	idleSince   map[string]time.Time  // unreferenced transport dir → first pass that saw it
	lastErr     map[string]string     // volume → text of its last error, for dedup
	reported    map[string]string     // transport dir → kind, for the metrics seam
	kinds       map[string]struct{}   // intent kinds reported, so a kind that empties reads 0
	failedLoad  map[string]struct{}   // load-failure paths already logged
	warned      map[string]string     // path → kind of the failure last logged at warn level
	// starts and ends count the passes begun and finished. Nothing in the loop
	// reads them; tests use them to wait for a pass that began after a point,
	// which the metrics seam cannot say.
	starts, ends atomic.Int64
	suspended    bool // collection is suspended by an unreadable record

	// newTicker is the timer seam; tests replace it.
	newTicker func(d time.Duration) (<-chan time.Time, func())
}

// dropped is a publish target that no intent claims and that should be
// unmounted.
type dropped struct {
	volume   string
	stage    string // the volume's stage path, "" if the reconciler no longer knows it
	explicit bool   // an Unpublish asked; false means it was derived (garbage)
}

// targetInfo is what the reconciler remembers of a publish target between
// passes: whose it was, and the stage path it was a bind of.
type targetInfo struct {
	volume, stage string
}

// New returns a Reconciler over an intent store and a Mounter. It fails if
// the roots in opts are not absolute, cleaned paths, if StageRoot and
// TransportsRoot overlap, or if the Mounter or the store is nil. The roots
// should already be resolved (no symlink): a mount point reached through a
// symlink is refused by the Mounter.
func New(store *intent.Store, m mount.Mounter, opts Options) (*Reconciler, error) {
	if store == nil {
		return nil, errors.New("reconcile: an intent store is required")
	}
	if m == nil {
		return nil, errors.New("reconcile: a Mounter is required")
	}
	for _, c := range []struct{ what, p string }{{"transports root", opts.TransportsRoot}, {"stage root", opts.StageRoot}} {
		if err := mount.CheckPath("reconcile", c.what, c.p); err != nil {
			return nil, err
		}
		if c.p == "/" {
			return nil, fmt.Errorf("reconcile: the %s must not be /", c.what)
		}
	}
	if within(opts.StageRoot, opts.TransportsRoot) || within(opts.TransportsRoot, opts.StageRoot) {
		return nil, errors.New("reconcile: the stage root and the transports root must not contain each other")
	}
	if opts.Interval <= 0 {
		opts.Interval = DefaultInterval
	}
	if opts.StatTimeout <= 0 {
		opts.StatTimeout = DefaultStatTimeout
	}
	r := &Reconciler{
		store:       store,
		m:           m,
		opts:        opts,
		items:       map[volume.Kind]item.Item{},
		log:         opts.Logger,
		metrics:     opts.Metrics,
		events:      opts.Events,
		wake:        make(chan struct{}, 1),
		transports:  map[transport.Spec]transport.Transport{},
		lastTargets: map[string]targetInfo{},
		drops:       map[string]dropped{},
		idleSince:   map[string]time.Time{},
		lastErr:     map[string]string{},
		reported:    map[string]string{},
		kinds:       map[string]struct{}{},
		failedLoad:  map[string]struct{}{},
		warned:      map[string]string{},
		newTicker:   realTicker,
	}
	if r.log == nil {
		r.log = slog.New(slog.DiscardHandler)
	}
	if r.metrics == nil {
		r.metrics = noopMetrics{}
	}
	if r.events == nil {
		r.events = noopEvents{}
	}
	if r.opts.Now == nil {
		r.opts.Now = time.Now
	}
	items := opts.Items
	if items == nil {
		d, err := directory.New(directory.Config{Root: opts.TransportsRoot, Mounter: m, Timeout: opts.StatTimeout})
		if err != nil {
			return nil, err
		}
		items = []item.Item{d}
	}
	for _, it := range items {
		if _, dup := r.items[it.Kind()]; dup {
			return nil, fmt.Errorf("reconcile: two items for kind %q", it.Kind())
		}
		r.items[it.Kind()] = it
	}
	return r, nil
}

func realTicker(d time.Duration) (<-chan time.Time, func()) {
	t := time.NewTicker(d)
	return t.C, t.Stop
}

// within reports whether path is dir or lies below it. Both are clean, and dir
// may be "/".
func within(dir, path string) bool {
	return dir == "/" || path == dir || strings.HasPrefix(path, dir+"/")
}

// strictlyBelow reports whether path lies below dir and is not dir.
func strictlyBelow(dir, path string) bool { return path != dir && within(dir, path) }

// Stage records that a volume should be staged and waits until the stage path
// is a mount of it (the transport mounted, the item directory present, the
// bind made), or until ctx ends. The result is the outcome of those steps
// only, nil when they hold, otherwise one of the package's errors wrapping
// the cause. It is not the outcome of the volume's publishes: one whose pod
// is gone must not fail the Stage of a volume other pods are using.
//
// The intent is validated against the boundary policy (volume.ParseID, the
// context rules of internal/volume, the structural rules of the intent
// package) and the stage path must be strictly below Options.StageRoot, so
// that the volume is inside the area garbage collection answers for; the
// errors are InvalidArgument statuses from those packages, or
// ErrStagePathOutsideRoot.
//
// Stage is idempotent. in.Publishes is ignored: publishes belong to Publish
// and Unpublish, and a record that already has some keeps them, so a second
// NodeStage for a volume that pods are using (kubelet repeats it after its
// own restart) can never unpublish them.
//
// A record is overwritten only after it was read successfully. If it cannot
// be read (corrupt, or a transient I/O error that looks the same), Stage
// writes nothing and returns an error wrapping ErrIntentsUnknown: rewriting it
// would drop the publishes of pods that are running, and the pass after would
// unmount them as targets no intent claims. A record that stays unreadable is
// for a person to repair or remove; every pass says so (see garbage.go).
func (r *Reconciler) Stage(ctx context.Context, in intent.Intent) error {
	if in.Version == 0 {
		in.Version = intent.Version
	}
	id, err := volume.ParseID(in.VolumeID)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidRequest, err)
	}
	if err := in.Context().MatchesID(id); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidRequest, err)
	}
	if err := in.Context().Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidRequest, err)
	}
	if err := in.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidRequest, err)
	}
	if _, ok := r.items[in.Kind]; !ok {
		return fmt.Errorf("%w: no item handles kind %q", ErrInvalidRequest, in.Kind)
	}
	if !strictlyBelow(r.opts.StageRoot, in.StagePath) {
		return fmt.Errorf("%w: %s", ErrStagePathOutsideRoot, in.StagePath)
	}

	unlock, err := r.locks.lock(ctx, in.VolumeID)
	if err != nil {
		return fmt.Errorf("waiting for volume %s: %w", in.VolumeID, err)
	}
	defer unlock()

	in.Publishes = nil
	prev, ok, err := r.store.Get(in.VolumeID)
	if err != nil {
		return fmt.Errorf("%w: reading the record of %s: %w", ErrIntentsUnknown, in.VolumeID, err)
	}
	if ok {
		in.Publishes = prev.Publishes
	}
	if err := r.store.Save(in); err != nil {
		return fmt.Errorf("recording the intent of %s: %w", in.VolumeID, err)
	}
	return r.await(ctx, &request{kind: reqStage, volume: in.VolumeID})
}

// Publish adds a publish (a target path and its read-only flag) to the
// volume's intent and waits until the target is a bind of the stage path with
// that flag, and the result is the outcome for that target only: a failing
// publish of another pod is not reported here (it is logged and counted).
// Publishing a target that is already published changes its flag and nothing
// else. A volume with no intent is ErrUnknownVolume: it must be
// staged first.
//
// The target directory is created here, with MkdirAll, in the caller's
// request, when kubelet has not made it. The converge loop does not do that
// when it recreates binds after a restart or a reboot: it creates only the
// leaf of a target whose parent exists. A target whose parent directory is
// gone belongs to a pod kubelet has already torn down, and creating the whole
// tree again would leave kubelet's cleanup of the orphaned pod directory
// failing on it for as long as the bind exists.
func (r *Reconciler) Publish(ctx context.Context, volumeID string, p intent.Publish) error {
	if err := r.checkTargetPath("publish", p.TargetPath); err != nil {
		return err
	}
	unlock, err := r.locks.lock(ctx, volumeID)
	if err != nil {
		return fmt.Errorf("waiting for volume %s: %w", volumeID, err)
	}
	defer unlock()
	if err := r.checkTargetScope(volumeID, p.TargetPath); err != nil {
		return err
	}

	_, ok, err := r.store.Get(volumeID)
	if err != nil {
		return fmt.Errorf("%w: reading the record of %s: %w", ErrIntentsUnknown, volumeID, err)
	}
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownVolume, volumeID)
	}
	what := fmt.Sprintf("creating the publish target %q", p.TargetPath)
	if _, err := deadline.Run(ctx, r.opts.StatTimeout, what, &r.mkdir, "mkdir\x00"+p.TargetPath, func(context.Context) (struct{}, error) {
		return struct{}{}, os.MkdirAll(p.TargetPath, publishDirMode)
	}); err != nil {
		return fmt.Errorf("publishing %s: %w", volumeID, err)
	}
	existed := false
	err = r.store.Update(volumeID, func(in *intent.Intent) error {
		if in.VolumeID == "" {
			return fmt.Errorf("%w: %s", ErrUnknownVolume, volumeID)
		}
		for i := range in.Publishes {
			if in.Publishes[i].TargetPath == p.TargetPath {
				existed = true
				in.Publishes[i] = p
				return nil
			}
		}
		in.Publishes = append(in.Publishes, p)
		return nil
	})
	if err != nil {
		return fmt.Errorf("recording the publish of %s: %w", volumeID, err)
	}
	err = r.await(ctx, &request{kind: reqPublish, volume: volumeID, target: p.TargetPath})
	if errors.Is(err, ErrPublishConflict) && !existed {
		// The record must only name targets the driver bound. The intent is
		// written before the wait so that a caller that gives up still
		// converges, but a target held by a mount that is not ours was never
		// bound, and a record of it would have a later pass unmount, as "no
		// longer claimed", whatever sits there.
		r.unrecordPublish(volumeID, p.TargetPath)
	}
	return err
}

// unrecordPublish removes a publish from the volume's record, best effort. The
// caller holds the volume's lock.
func (r *Reconciler) unrecordPublish(volumeID, target string) {
	err := r.store.Update(volumeID, func(in *intent.Intent) error {
		if !hasPublish(*in, target) {
			return errNoChange
		}
		kept := in.Publishes[:0]
		for _, q := range in.Publishes {
			if q.TargetPath != target {
				kept = append(kept, q)
			}
		}
		in.Publishes = kept
		return nil
	})
	if err != nil && !errors.Is(err, errNoChange) {
		r.log.Warn("could not remove a publish that was never bound from the record", "volume", volumeID, "target", target, "error", err)
	}
}

// Unpublish removes a target from the volume's intent and waits until nothing
// is mounted at it. It is idempotent, and it does the work even when the
// volume has no intent (already unstaged): the target is the one kubelet is
// asking to be rid of, and a mount left there would pin the pod's directory.
//
// If the volume's record cannot be read, Unpublish unmounts nothing and
// returns an error wrapping ErrIntentsUnknown. The unpublish cannot be
// recorded, so a target unmounted now would come back as soon as the record
// reads again, a bind left in a dead pod's directory. Kubelet retries.
//
// The result is about the target alone: it does not wait for, or fail
// because of, the rest of the volume's convergence, so a transport that is
// down never keeps a pod from being deleted. A target that is busy (a process
// still has it open) stays mounted, the error says so, and the caller
// retries; nothing is detached lazily.
func (r *Reconciler) Unpublish(ctx context.Context, volumeID, targetPath string) error {
	if err := r.checkTargetPath("unpublish", targetPath); err != nil {
		return err
	}
	unlock, err := r.locks.lock(ctx, volumeID)
	if err != nil {
		return fmt.Errorf("waiting for volume %s: %w", volumeID, err)
	}
	defer unlock()
	if err := r.checkTargetScope(volumeID, targetPath); err != nil {
		return err
	}

	in, ok, err := r.store.Get(volumeID)
	if err != nil {
		return fmt.Errorf("%w: reading the record of %s: %w", ErrIntentsUnknown, volumeID, err)
	}
	if ok && hasPublish(in, targetPath) {
		err := r.store.Update(volumeID, func(in *intent.Intent) error {
			kept := in.Publishes[:0]
			for _, p := range in.Publishes {
				if p.TargetPath != targetPath {
					kept = append(kept, p)
				}
			}
			in.Publishes = kept
			return nil
		})
		if err != nil {
			return fmt.Errorf("recording the unpublish of %s: %w", volumeID, err)
		}
	}
	return r.await(ctx, &request{kind: reqUnpublish, volume: volumeID, target: targetPath})
}

// checkTargetPath refuses a target that is not an absolute, cleaned path, and
// one that is, is inside, or is an ancestor of a directory the driver keeps for
// itself: the transports root (a publish there would sit among the transports,
// and an unpublish would unmount one) or the staging root (a publish there is
// garbage by rule (a) and would be mounted and unmounted on every pass, and an
// unpublish would unmount a volume's stage). An ancestor is as bad as the
// directory: a bind mounted on the directory above the staging root hides
// every stage under it. All are ErrTargetOutsideScope.
func (r *Reconciler) checkTargetPath(what, target string) error {
	if err := mount.CheckPath(what, "target path", target); err != nil {
		return fmt.Errorf("%w: %w", ErrTargetOutsideScope, err)
	}
	for _, root := range []struct{ name, dir string }{{"transports root", r.opts.TransportsRoot}, {"stage root", r.opts.StageRoot}} {
		switch {
		case within(root.dir, target):
			return fmt.Errorf("%w: %s is the %s or inside it", ErrTargetOutsideScope, target, root.name)
		case within(target, root.dir):
			return fmt.Errorf("%w: %s is the %s or contains it", ErrTargetOutsideScope, target, root.name)
		}
	}
	return nil
}

// checkTargetScope reads the intents and refuses a target that is, is inside,
// or contains some intent's stage path (ErrTargetOutsideScope: another
// volume's stage would otherwise be an unpublish away from unmounted, or
// hidden by a publish above it), and one that
// another volume's record claims as its publish target (ErrPublishConflict,
// naming the claimant): two volumes cannot be bound at one path, and an
// unpublish for one must not unmount the other's. A store that cannot be read
// is ErrIntentsUnknown. Records that fail to load cannot be consulted; their
// stage paths are below the staging root, which checkTargetPath refuses.
func (r *Reconciler) checkTargetScope(volumeID, target string) error {
	loaded, err := r.store.Load()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrIntentsUnknown, err)
	}
	for id, in := range loaded.Intents {
		if within(in.StagePath, target) || within(target, in.StagePath) {
			return fmt.Errorf("%w: %s is the stage path of %s, or inside it, or contains it", ErrTargetOutsideScope, target, id)
		}
		if id == volumeID {
			continue
		}
		if hasPublish(in, target) {
			return fmt.Errorf("%w: %s is claimed by the publish of %s", ErrPublishConflict, target, id)
		}
	}
	return nil
}

func hasPublish(in intent.Intent, target string) bool {
	for _, p := range in.Publishes {
		if p.TargetPath == target {
			return true
		}
	}
	return false
}

// Unstage waits until the volume's publishes and its stage path are unmounted
// and then removes its intent. The transport is left mounted: the idle timeout
// decides about it.
//
// The order is the reverse of the usual one, on purpose. The record is
// deleted after the mounts are gone, not before: Unstage carries only the
// volume ID, and once the record is deleted nothing says where the stage path
// was, so a stage unmount that failed (the volume is busy) could not be
// retried or even reported to the caller. With the record kept until the
// teardown succeeds, a failed Unstage is simply retried by kubelet, and a
// crash between the unmount and the delete leaves a record whose next pass
// binds the stage again and whose next Unstage removes it. During the
// teardown the volume is not converged, so the publishes it just unmounted
// are not made again in the same pass.
//
// The volume's remaining publishes are unmounted too. CSI says kubelet
// unpublishes first, so there are none in practice; if there are, the volume
// is being removed, and binds left behind with no intent would be leaked.
//
// A volume with no intent is already unstaged: Unstage returns nil at once and
// the loop is nudged so any stray mount it left is collected. A record that
// cannot be read is neither torn down nor deleted: Unstage returns an error
// wrapping ErrIntentsUnknown. Telling a corrupt file from a transient read
// error is not possible from here, and deleting a healthy record on a
// transient error would forget a volume that is mounted.
func (r *Reconciler) Unstage(ctx context.Context, volumeID string) error {
	unlock, err := r.locks.lock(ctx, volumeID)
	if err != nil {
		return fmt.Errorf("waiting for volume %s: %w", volumeID, err)
	}
	defer unlock()

	_, ok, err := r.store.Get(volumeID)
	if err != nil {
		return fmt.Errorf("%w: reading the record of %s: %w", ErrIntentsUnknown, volumeID, err)
	}
	if !ok {
		r.nudge()
		return nil
	}
	return r.await(ctx, &request{kind: reqUnstage, volume: volumeID})
}

// await queues req and waits for its result or for ctx.
func (r *Reconciler) await(ctx context.Context, req *request) error {
	req.done = make(chan error, 1)
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return ErrStopped
	}
	r.queue = append(r.queue, req)
	r.mu.Unlock()
	r.nudge()

	select {
	case err := <-req.done:
		return err
	case <-ctx.Done():
		return fmt.Errorf("waiting for the reconcile pass: %w", ctx.Err())
	}
}

// nudge asks the loop for a pass, without blocking.
func (r *Reconciler) nudge() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// take removes and returns the queued requests. It is the first thing a pass
// does, before it reads the store: a request is queued only after its intent
// is written, so every intent a taken request wrote is visible to the Load
// that follows.
func (r *Reconciler) take() []*request {
	r.mu.Lock()
	defer r.mu.Unlock()
	b := r.queue
	r.queue = nil
	return b
}

// Run converges until ctx is done: once at the start, then on every request
// and on every tick of Options.Interval. Passes are sequential. It returns nil
// when ctx ends, after failing every queued request with ErrStopped, and an
// error if Run was already called.
func (r *Reconciler) Run(ctx context.Context) error {
	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		return errors.New("reconcile: Run was already called")
	}
	r.started = true
	r.mu.Unlock()
	defer r.stop()

	ticks, stopTicks := r.newTicker(r.opts.Interval)
	defer stopTicks()
	for {
		r.runPass(ctx, r.take())
		select {
		case <-ctx.Done():
			return nil
		case <-r.wake:
		case <-ticks:
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

// stop refuses further requests and fails the queued ones.
func (r *Reconciler) stop() {
	r.mu.Lock()
	r.stopped = true
	q := r.queue
	r.queue = nil
	r.mu.Unlock()
	for _, req := range q {
		req.done <- ErrStopped
	}
}
