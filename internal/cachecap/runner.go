package cachecap

import (
	"context"
	"errors"
	"log/slog"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// Runner keeps the node's advertised cache capacity equal to its budget, or to
// its committed figure when that is higher. It runs in a goroutine of its own, so a slow API server cannot stall volume
// work.
type Runner struct {
	src       *Source
	nc        NodeCapacity
	node      string
	committed func(context.Context) (int64, int, error)
	metrics   CapacityMetrics
	log       *slog.Logger
	interval  time.Duration
	deadline  time.Duration

	// The fence probe: when it last ran, what it found, how often it runs.
	probeEvery time.Duration
	probedAt   time.Time
	verdict    FenceState
	now        func() time.Time
	// probeKeyLeft: an allowed probe's key could not be removed yet.
	probeKeyLeft  bool
	cleanupWarned bool
	// probeRemoveFailing: the Error for an allowed probe's key that could not
	// be removed has been logged; repeats go to Debug until a removal works.
	probeRemoveFailing bool
	// warnedUnknown: the failed passes of the current unknown verdict have
	// been logged at Warn once already.
	warnedUnknown bool
	// lastHeld is the figure advertised on the last pass that could read the
	// commitment; an unreadable commitment never drops the figure below it.
	lastHeld int64
	// advertised: a figure has been advertised since the runner started.
	advertised bool
	// holding: the advertised figure is being held at the committed figure
	// (it exceeds the budget); the Warn for the episode has been logged.
	holding bool
}

// DefaultProbeInterval is how often the fence probe runs while the node is
// fenced. Each probe is a request the API server denies on purpose, so the
// expected denial rate of the policy is one per node per interval. A node that
// is unfenced or unknown probes on every pass instead.
const DefaultProbeInterval = 10 * time.Minute

// defaultDeadline bounds one pass when NewRunner is given no positive
// deadline; it is the node's -deadline default.
const defaultDeadline = 10 * time.Second

// defaultInterval is the reconciler's default period (reconcile.DefaultInterval;
// a test pins the two together). The package does not import reconcile: the
// stage check lives below it and imports this package.
const defaultInterval = 30 * time.Second

// NewRunner returns a Runner for node. committed reports the bytes and the
// count of volumes the node has committed; interval is the period of Run, and
// deadline bounds each Pass, API requests included; probeEvery is the period
// of the fence probe. A zero or negative
// interval means the reconciler's default, and a zero or negative deadline
// means the node's default, so neither can stop a ticker or leave a request
// unbounded.
func NewRunner(src *Source, nc NodeCapacity, node string, committed func(context.Context) (int64, int, error), m CapacityMetrics, log *slog.Logger, interval, deadline, probeEvery time.Duration) *Runner {
	if interval <= 0 {
		interval = defaultInterval
	}
	if deadline <= 0 {
		deadline = defaultDeadline
	}
	if probeEvery <= 0 {
		probeEvery = DefaultProbeInterval
	}
	return &Runner{src: src, nc: nc, node: node, committed: committed, metrics: m, log: log,
		interval: interval, deadline: deadline, probeEvery: probeEvery, now: time.Now}
}

// FencePolicy is the name of the admission policy that fences the node
// plugin's status patch.
const FencePolicy = "miami-node-capacity"

// ErrFenceUnknown is returned by a Pass whose probe could not tell whether
// FencePolicy is in force and that therefore left the last advertised figure
// as it was.
var ErrFenceUnknown = errors.New("cachecap: the admission policy " + FencePolicy + " has not been confirmed in force; keeping the last advertised figure")

// probeIfDue runs the fence probe when none has run, or the last verdict is
// older than its wait, and records the verdict. Only a Fenced verdict waits the
// probe interval; Unfenced and Unknown are retried at the normal pass interval,
// so a node that probed before the policy was enforced (as at install) recovers
// within a pass. It returns the probe's own error, or that of the removal of a
// leftover probe key that stopped it from running.
func (r *Runner) probeIfDue(ctx context.Context) error {
	wait := r.interval
	if r.verdict == Fenced {
		wait = r.probeEvery
	}
	if !r.probedAt.IsZero() && r.now().Sub(r.probedAt) < wait {
		return nil
	}
	// A probe key already on the node (left by an earlier run that was killed
	// between the probe's add and its remove, or placed by an admin) would make
	// the probe's add a no-op that the policy allows, and read as unfenced. So
	// the node itself is read and the key removed first; the in-memory flag only
	// knows about this process. If that cannot be done the probe is not run and
	// the verdict stays as it was. (A key this process knows it left is the
	// business of retryProbeCleanup, which has just tried and reported.)
	if !r.probeKeyLeft {
		if err := r.nc.RemoveFenceProbe(ctx, r.node); err != nil {
			return r.counted(err)
		}
	}
	prev := r.verdict
	state, perr := r.nc.ProbeFence(ctx, r.node)
	r.probedAt, r.verdict = r.now(), state
	switch state {
	case Fenced:
		r.metrics.SetCacheFenceState("fenced")
	case Unfenced:
		r.metrics.SetCacheFenceState("unfenced")
		subject := "unknown"
		if s, ok := r.nc.(interface{ Subject() string }); ok {
			subject = s.Subject()
		}
		// The probe repeats every pass while unfenced: say so loudly on the
		// way in, and quietly after.
		lvl := slog.LevelDebug
		if prev != Unfenced {
			lvl = slog.LevelError
		}
		r.log.Log(ctx, lvl, "the admission policy "+FencePolicy+" does not fence this node plugin; the node keeps advertising its cache capacity, but a compromised plugin is not held to its own node and key. "+
			"Patch the policy's matchCondition and binding for the renamed or moved ServiceAccount",
			"policy", FencePolicy, "subject", subject, "node", r.node)
		if perr != nil {
			// The probe was allowed but its key could not be removed. Say so,
			// count it, and retry on every pass until it is gone.
			lvl := slog.LevelDebug
			if !r.probeRemoveFailing {
				lvl = slog.LevelError
			}
			r.probeRemoveFailing = true
			r.log.Log(ctx, lvl, "the fence probe key could not be removed from the node; will retry on the next pass", "node", r.node, "error", perr)
			r.probeKeyLeft = true
			return r.counted(perr)
		}
		r.probeRemoveFailing = false
		return nil
	default:
		// Unknown shows as its own state and is counted under its own reason, not as a denial.
		r.metrics.SetCacheFenceState("unknown")
		if perr != nil {
			r.metrics.IncCachePatchFailure(PatchUnknown)
		}
		return perr
	}
	return nil
}

// retryProbeCleanup removes the fence probe's key if an allowed probe left it,
// on every pass and whatever the verdict has become since. The policy allows
// the removal of that key, so a removal that keeps failing is RBAC or the API
// server; the key is then stranded until an operator removes it, which is said
// once.
func (r *Runner) retryProbeCleanup(ctx context.Context) error {
	if !r.probeKeyLeft {
		return nil
	}
	err := r.nc.RemoveFenceProbe(ctx, r.node)
	if err == nil {
		r.probeKeyLeft, r.cleanupWarned, r.probeRemoveFailing = false, false, false
		return nil
	}
	if !r.cleanupWarned {
		r.cleanupWarned = true
		r.log.Warn("the fence probe key is still on the node and could not be removed; remove status.capacity[miami.csi.greghaskins.com/fence-probe] by hand if this keeps failing (the admission policy allows its removal, so look at the plugin's RBAC and the API server)",
			"node", r.node, "error", err)
	}
	return r.counted(err)
}

// Pass computes the budget, sets the metrics, and patches the node when its
// advertised figure differs. With accounting off it removes the node's key if
// present and sets no metric.
func (r *Runner) Pass(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, r.deadline)
	defer cancel()
	budget, enabled, err := r.src.Current(ctx)
	if err != nil {
		return err
	}
	if !enabled {
		_, present, err := r.nc.CacheCapacity(ctx, r.node)
		if err != nil || !present {
			return err
		}
		return r.counted(r.nc.RemoveCacheCapacity(ctx, r.node))
	}
	// A committed figure that cannot be read leaves its gauge as it was (a
	// zero would say the node has room) and the pass goes on: the
	// advertisement does not depend on it.
	committed, _, cerr := r.committed(ctx)
	size, free := r.src.Stats()
	r.metrics.SetCacheCapacity(budget, size, free)
	committedKnown := cerr == nil
	if committedKnown {
		r.metrics.SetCacheCommitted(committed)
	}

	// The admission policy that narrows the plugin's grant is probed before
	// the first advertisement, at start and then every probe interval while
	// fenced (every pass while unfenced or unknown), because an overlay that
	// renames the ServiceAccount can leave the policy matching nobody, which no
	// static check sees. The verdict is reported, not enforced: a lost fence
	// is logged at Error, shown by miami_cache_fence_state and paged on, and
	// the node keeps advertising. Removing our own key would not constrain a
	// compromised plugin, and would turn a configuration mistake into a
	// scheduling outage for every new pod that mounts a volume of ours.
	// An allowed probe's key is removed on every pass, whatever the verdict.
	cerr = errors.Join(cerr, r.retryProbeCleanup(ctx))
	cerr = errors.Join(cerr, r.probeIfDue(ctx))
	if r.verdict == FenceUnknown && r.advertised {
		// The probe could not tell. Keep the last figure, neither updating nor
		// removing it: removing it on an API server blip would stop the
		// scheduling of new pods cluster-wide, and the figure only goes stale
		// until the next probe answers. Before any advertisement (a fresh
		// start) the node advertises anyway.
		return errors.Join(cerr, ErrFenceUnknown)
	}

	// The node never advertises less than it has committed. The kubelet
	// re-admits every running pod against the advertised figure when it
	// restarts, and fails the ones that no longer fit; a lowered budget must
	// not do that, so it takes effect as volumes leave. The stage check still
	// uses the budget, so no new volume is let in over it. An unreadable
	// commitment is no reason to advertise less than the last figure held: the
	// pods it covered are still there.
	advertised := max(budget, r.lastHeld)
	if committedKnown && committed > budget {
		advertised = roundUpMiB(committed)
		if !r.holding {
			r.holding = true
			r.log.Warn("holding the advertised cache capacity at the committed figure; lower budgets take effect as volumes leave",
				"node", r.node, "budget", budget, "committed", committed)
		}
	} else if committedKnown {
		r.holding = false
		advertised = budget
	}
	if committedKnown {
		r.lastHeld = advertised
	}
	r.metrics.SetCacheAdvertised(advertised)

	have, present, err := r.nc.CacheCapacity(ctx, r.node)
	if err != nil {
		return errors.Join(cerr, err)
	}
	if present && have == advertised {
		r.advertised = true
		return cerr
	}
	serr := r.counted(r.nc.SetCacheCapacity(ctx, r.node, advertised))
	if serr == nil {
		r.advertised = true
	}
	return errors.Join(cerr, serr)
}

// roundUpMiB rounds n up to a whole MiB, so a figure held up for a commitment
// is never below it. Footprints are whole chunks, so this is n itself in practice.
func roundUpMiB(n int64) int64 {
	return (n + mib - 1) &^ (mib - 1)
}

// counted counts a failed patch or clear by reason and returns it.
func (r *Runner) counted(err error) error {
	switch {
	case err == nil:
	case apierrors.IsForbidden(err) || apierrors.IsInvalid(err):
		r.metrics.IncCachePatchFailure(PatchDenied)
	default:
		r.metrics.IncCachePatchFailure(PatchError)
	}
	return err
}

// Run passes at once and then every interval until ctx ends. A failed pass is
// logged and tried again at the next tick.
func (r *Runner) Run(ctx context.Context) {
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		if err := r.Pass(ctx); err != nil && ctx.Err() == nil {
			// While the fence verdict is unknown (a slow start, a flapping API
			// server) every pass fails the same way: say it once, then quietly.
			if r.verdict == FenceUnknown && r.warnedUnknown {
				r.log.Debug("cache capacity advertisement failed; will retry", "error", err)
			} else {
				r.log.Warn("cache capacity advertisement failed; will retry", "error", err)
				r.warnedUnknown = r.verdict == FenceUnknown
			}
		}
		if r.verdict != FenceUnknown {
			r.warnedUnknown = false
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
