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
}

// defaultDeadline bounds one pass when NewRunner is given no positive
// deadline; it is the node's -deadline default.
const defaultDeadline = 10 * time.Second

// defaultInterval is the reconciler's default period (reconcile.DefaultInterval;
// a test pins the two together). The package does not import reconcile: the
// stage check lives below it and imports this package.
const defaultInterval = 30 * time.Second

// NewRunner returns a Runner for node. committed reports the bytes and the
// count of volumes the node has committed; interval is the period of Run, and
// deadline bounds each Pass, API requests included. A zero or negative
// interval means the reconciler's default, and a zero or negative deadline
// means the node's default, so neither can stop a ticker or leave a request
// unbounded.
func NewRunner(src *Source, nc NodeCapacity, node string, committed func(context.Context) (int64, int, error), m CapacityMetrics, log *slog.Logger, interval, deadline time.Duration) *Runner {
	if interval <= 0 {
		interval = defaultInterval
	}
	if deadline <= 0 {
		deadline = defaultDeadline
	}
	return &Runner{src: src, nc: nc, node: node, committed: committed, metrics: m, log: log, interval: interval, deadline: deadline}
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
			r.log.Warn("cache capacity advertisement failed; will retry", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
