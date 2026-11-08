package volume

import "math/bits"

// CacheResource is the extended resource a pod requests and a node
// advertises for node-local cache space, in bytes and always a whole number
// of MiB (design 2026-11-07-cache-capacity-scheduling, section 3).
const CacheResource = "miami.csi.greghaskins.com/cache"

const (
	indexSlack     = 16 << 20 // the cache index and bookkeeping a volume holds beyond its data
	footprintUnit  = 1 << 20  // footprints are whole MiB
	footprintAlign = footprintUnit - 1

	// MaxFootprint is the largest footprint, and what a sum saturates to: the
	// greatest positive int64 that is a whole number of MiB.
	MaxFootprint int64 = (1<<63 - 1) &^ footprintAlign
)

// CacheFootprint is the node-local cache space a volume claims:
//
//	min(Size, CacheSize) + DirtyWatermark + indexSlack (16 MiB)
//
// rounded up to a whole MiB (design 2026-11-07-cache-capacity-scheduling,
// section 3, decision 66). It is the one place the formula is spelled: the
// webhook, the node advertisement and the stage check all call it, on a
// VolumeParams as Context.WithSize resolves it. A missing CacheSize means
// Size and a missing DirtyWatermark means max(CacheSize/2, 1), the defaults
// WithSize applies. A VolumeParams whose Size is not yet known gives 0. The
// sum saturates at MaxFootprint rather than overflow.
func CacheFootprint(v VolumeParams) int64 {
	if v.Size <= 0 {
		return 0
	}
	cache := v.CacheSize
	if cache <= 0 {
		cache = v.Size
	}
	dirty := v.Generations.DirtyWatermark
	if dirty <= 0 {
		dirty = max(cache/2, 1)
	}
	sum, carry := bits.Add64(uint64(min(v.Size, cache)), uint64(dirty), 0)
	if carry != 0 {
		return MaxFootprint
	}
	sum, carry = bits.Add64(sum, indexSlack+footprintAlign, 0)
	if carry != 0 {
		return MaxFootprint
	}
	sum &^= footprintAlign
	if sum > uint64(MaxFootprint) {
		return MaxFootprint
	}
	return int64(sum)
}

// AddFootprint adds two footprints, saturating at MaxFootprint (design
// 2026-11-07-cache-capacity-scheduling, section 3). Both are whole MiB, so
// the sum is too.
func AddFootprint(a, b int64) int64 {
	if a < 0 || b < 0 {
		return MaxFootprint
	}
	if a > MaxFootprint-b {
		return MaxFootprint
	}
	return a + b
}
