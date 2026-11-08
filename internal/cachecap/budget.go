// Package cachecap computes how much node-local cache space this node offers
// the scheduler and keeps the node's advertised figure in step with it
// (design decision 66).
package cachecap

import "context"

// mib is the unit of every figure the package produces.
const mib int64 = 1 << 20

// Config is what the node's flags say about the cache budget.
type Config struct {
	// Accounting is false when the node neither advertises nor checks.
	Accounting bool
	// Share is the fraction of the cache filesystem offered, in (0, 1].
	Share float64
	// Reserve is the space kept back from the share, in bytes.
	Reserve int64
	// Absolute is the budget in bytes when HaveAbsolute is set; zero is an
	// answer, not an absence.
	Absolute     int64
	HaveAbsolute bool
}

// Budget is the cache budget for a cache filesystem of fsSize bytes:
// max(0, Share x fsSize - Reserve), or Absolute when HaveAbsolute is set,
// rounded down to a whole MiB and never negative.
func Budget(fsSize int64, c Config) int64 {
	b := c.Absolute
	if !c.HaveAbsolute {
		b = int64(c.Share*float64(fsSize)) - c.Reserve
	}
	if b < 0 {
		return 0
	}
	return b &^ (mib - 1)
}

// NodeCapacity reads and writes the node's cache resource in its status.
type NodeCapacity interface {
	// CacheCapacity reports the figure the node advertises, and whether the
	// node carries the key at all.
	CacheCapacity(ctx context.Context, node string) (bytes int64, present bool, err error)
	SetCacheCapacity(ctx context.Context, node string, bytes int64) error
	RemoveCacheCapacity(ctx context.Context, node string) error
}

// CapacityMetrics receives the figures of every pass. The committed figure
// has a setter of its own so that a pass that cannot read it leaves the last
// value standing, which a zero would misreport as a node with room.
type CapacityMetrics interface {
	SetCacheCapacity(budget, fsSize, fsFree int64)
	SetCacheCommitted(committed int64)
}
