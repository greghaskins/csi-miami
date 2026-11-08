package volume

import (
	"math"
	"math/bits"

	"github.com/greghaskins/csi-miami/internal/chunk"
)

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

// ServedFootprint is the footprint of a volume at the size it is served: the
// larger of v.Size and served rounded up to whole chunks. A PV's volume
// context is immutable and says the size the volume was created with, so a
// grown volume is served at more; and a class that caches the whole volume
// (CacheSize >= Size) keeps doing so when it grows, so its cache bound rises
// with the size. A class with a partial cache keeps its bound. The
// rule's condition is CachesWholeVolume, which the node's size raising uses
// too, so the node and the webhook cannot disagree about an expanded volume.
// served <= 0 means no larger size is known.
func ServedFootprint(v VolumeParams, served int64) int64 {
	return CacheFootprint(ServedParams(v, served))
}

// ServedParams is v at the size it is served (see ServedFootprint): Size
// raised to served rounded up to whole chunks, never lowered, and CacheSize
// raised with it when the class caches the whole volume. The node's size
// raising and ServedFootprint both call it.
func ServedParams(v VolumeParams, served int64) VolumeParams {
	if v.ChunkSize > 0 && served > math.MaxInt64-v.ChunkSize {
		served = math.MaxInt64 / v.ChunkSize * v.ChunkSize // RoundUp would overflow
	}
	size := max(v.Size, chunk.RoundUp(served, v.ChunkSize))
	if size > v.Size && CachesWholeVolume(v.Size, v.CacheSize) {
		v.CacheSize = size
	}
	v.Size = size
	return v
}

// CachesWholeVolume reports whether a class's cache bound covers its whole
// volume, in which case the bound rises with the size when the volume grows.
// The node's size raising, its footprint accounting and the webhook all ask it.
func CachesWholeVolume(size, cacheSize int64) bool { return cacheSize >= size }
