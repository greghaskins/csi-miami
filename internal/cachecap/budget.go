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
	// ProbeFence asks whether the admission policy that fences the node's
	// status patch is in force, by trying a patch it must deny (decision 66,
	// 4.9). Fenced: the policy denied it. Unfenced: the patch was allowed
	// (and is removed again). Unknown comes with the error that left it so.
	ProbeFence(ctx context.Context, node string) (FenceState, error)
	// RemoveFenceProbe removes the probe's key if the node carries it.
	RemoveFenceProbe(ctx context.Context, node string) error
}

// FenceState is the answer of NodeCapacity.ProbeFence.
type FenceState int

const (
	// FenceUnknown means the probe could not tell (a timeout, a server error, a
	// denial that does not name the policy).
	FenceUnknown FenceState = iota
	// Fenced means the policy denied the probe, naming itself.
	Fenced
	// Unfenced means the probe patch was allowed, so nothing narrows the node
	// plugin's grant.
	Unfenced
)

// CapacityMetrics receives the figures of every pass. The committed figure
// has a setter of its own so that a pass that cannot read it leaves the last
// value standing, which a zero would misreport as a node with room.
type CapacityMetrics interface {
	SetCacheCapacity(budget, fsSize, fsFree int64)
	SetCacheCommitted(committed int64)
	// IncCachePatchFailure counts a failed patch or clear of the node's
	// capacity; reason is PatchDenied or PatchError.
	IncCachePatchFailure(reason string)
	// SetCacheFenceState records the verdict of the last probe: "fenced",
	// "unfenced" or "unknown" (see the Fence* names); exactly one is 1.
	SetCacheFenceState(state string)
}

// The reasons of CapacityMetrics.IncCachePatchFailure.
const (
	PatchDenied  = "denied"  // the API server answered 403 or 422
	PatchError   = "error"   // any other failure
	PatchUnknown = "unknown" // a fence probe that could not tell
)
