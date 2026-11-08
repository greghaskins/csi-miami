package cachecap

import (
	"context"
	"errors"
	"log/slog"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// Runner keeps the node's advertised cache capacity equal to its budget. It
// runs in a goroutine of its own, so a slow API server cannot stall volume
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
	// warnedUnknown: the failed passes of the current unknown verdict have
	// been logged at Warn once already.
	warnedUnknown bool
}

// DefaultProbeInterval is how often the fence probe runs. Each probe is a
// request the API server denies on purpose, so the expected denial rate of the
// policy is one per node per interval.
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

// ErrUnfenced is returned by a Pass that found the plugin's grant not
// narrowed by FencePolicy.
var ErrUnfenced = errors.New("cachecap: the admission policy " + FencePolicy + " does not fence the node plugin")

// ErrFenceUnknown is returned by a Pass that has no verdict on the fence, so
// it did not advertise.
var ErrFenceUnknown = errors.New("cachecap: not advertising: the admission policy " + FencePolicy + " has not been confirmed in force")

// probeIfDue runs the fence probe when none has run, or the last verdict is
// older than the probe interval (an unknown verdict is retried at the normal
// pass interval), and records the verdict. It returns the probe's own error.
func (r *Runner) probeIfDue(ctx context.Context) error {
	wait := r.probeEvery
	if r.verdict == FenceUnknown {
		wait = r.interval
	}
	if !r.probedAt.IsZero() && r.now().Sub(r.probedAt) < wait {
		return nil
	}
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
		r.log.Error("the admission policy "+FencePolicy+" does not fence this node plugin; it will advertise no cache capacity until a probe is denied. "+
			"Patch the policy's matchCondition and binding for the renamed or moved ServiceAccount",
			"policy", FencePolicy, "subject", subject, "node", r.node)
		if perr != nil {
			// The probe was allowed but its key could not be removed. Say so,
			// count it, and retry on every pass until it is gone.
			r.log.Error("the fence probe key could not be removed from the node; will retry on the next pass", "node", r.node, "error", perr)
			r.probeKeyLeft = true
			return r.counted(perr)
		}
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
// on every pass and whatever the verdict has become since. If the policy
// itself now denies the removal (it denies touching any key but ours), the
// key is stranded and an operator has to remove it: that is said once.
func (r *Runner) retryProbeCleanup(ctx context.Context) error {
	if !r.probeKeyLeft {
		return nil
	}
	err := r.nc.RemoveFenceProbe(ctx, r.node)
	if err == nil {
		r.probeKeyLeft, r.cleanupWarned = false, false
		return nil
	}
	if !r.cleanupWarned {
		r.cleanupWarned = true
		r.log.Warn("the fence probe key is still on the node and could not be removed; an operator must remove status.capacity[miami.csi.greghaskins.com/fence-probe] if the admission policy now denies it",
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
	if cerr == nil {
		r.metrics.SetCacheCommitted(committed)
	}

	// The plugin advertises only under the admission policy that narrows its
	// grant; an overlay that renames the ServiceAccount can leave the policy
	// matching nobody, which no static check sees. So the runner asks the API
	// server, at start and then every probe interval, and fails closed. Each
	// probe is a denied request by design, so it is not made on every pass.
	// An allowed probe's key is removed on every pass, whatever the verdict.
	cerr = errors.Join(cerr, r.retryProbeCleanup(ctx))
	if perr := r.probeIfDue(ctx); perr != nil {
		return errors.Join(cerr, perr)
	}
	switch r.verdict {
	case Fenced:
	case Unfenced:
		// Whatever the cause, nothing of ours stays on the node.
		_, present, err := r.nc.CacheCapacity(ctx, r.node)
		if err == nil && present {
			err = r.counted(r.nc.RemoveCacheCapacity(ctx, r.node))
		}
		return errors.Join(cerr, ErrUnfenced, err)
	default:
		// No verdict yet, or the last probe could not tell: no advertising.
		return errors.Join(cerr, ErrFenceUnknown)
	}

	have, present, err := r.nc.CacheCapacity(ctx, r.node)
	if err != nil {
		return errors.Join(cerr, err)
	}
	if present && have == budget {
		return cerr
	}
	return errors.Join(cerr, r.counted(r.nc.SetCacheCapacity(ctx, r.node, budget)))
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
