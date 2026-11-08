package cachecap

import (
	"context"
	"errors"
	"log/slog"
	"time"
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
}

// NewRunner returns a Runner for node. committed reports the bytes and the
// count of volumes the node has committed; interval is the period of Run.
func NewRunner(src *Source, nc NodeCapacity, node string, committed func(context.Context) (int64, int, error), m CapacityMetrics, log *slog.Logger, interval time.Duration) *Runner {
	return &Runner{src: src, nc: nc, node: node, committed: committed, metrics: m, log: log, interval: interval}
}

// Pass computes the budget, sets the metrics, and patches the node when its
// advertised figure differs. With accounting off it removes the node's key if
// present and sets no metric.
func (r *Runner) Pass(ctx context.Context) error {
	budget, enabled, err := r.src.Current(ctx)
	if err != nil {
		return err
	}
	if !enabled {
		_, present, err := r.nc.CacheCapacity(ctx, r.node)
		if err != nil || !present {
			return err
		}
		return r.nc.RemoveCacheCapacity(ctx, r.node)
	}
	// A committed figure that cannot be read is reported as zero and the
	// pass goes on: the advertisement does not depend on it.
	committed, _, cerr := r.committed(ctx)
	size, free := r.src.Stats()
	r.metrics.SetCacheCapacity(budget, committed, size, free)

	have, present, err := r.nc.CacheCapacity(ctx, r.node)
	if err != nil {
		return errors.Join(cerr, err)
	}
	if present && have == budget {
		return cerr
	}
	return errors.Join(cerr, r.nc.SetCacheCapacity(ctx, r.node, budget))
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
