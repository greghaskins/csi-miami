package volume

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/greghaskins/csi-miami/internal/cache"
	"github.com/greghaskins/csi-miami/internal/chunk"
	"github.com/greghaskins/csi-miami/internal/item"
	kinds "github.com/greghaskins/csi-miami/internal/volume"
)

// # The cache-budget stage check (decision 66)
//
// The scheduler places a pod by the node's advertised cache capacity; this
// check is the defence in depth behind it, for a pod that got here anyway (a
// webhook that was down, a namespace that opted out, a race between two
// schedulers). It refuses a volume that is new to the node when its cache
// footprint, added to what the node has committed, would exceed the node's
// cache budget.
//
// It runs at the top of Converge, before the holder layer, so a refusal
// acquires nothing and writes nothing: no holder file, no drain record, no
// cache directory. It applies only to a volume new to the node: one with no
// nbd device of its name and no cache index. Adopt, a block server's restart,
// a reboot with a surviving cache, a drain and a teardown are never refused,
// because refusing them would strand data the node already holds.
//
// It fails open. A budget that cannot be computed (a statfs error) or a cache
// directory that cannot be listed logs why and admits the volume: the check is
// a second line, and a broken one must never refuse every new volume. A store
// read, in contrast, returns its own error: starting at a smaller size is the
// one wrong answer (raiseSize), and the stage is retried.

// Committed returns the cache footprint the node has committed, and the number
// of volumes it covers: the plain sum of CacheFootprint over the drain records
// in the cache directory. A drain record is written before every block server
// starts and removed with a clean release, so the records cover every staged
// volume and every cache awaiting a drain, one per volume. No volume is
// excluded by name: a discarded cache keeps its record, and a fresh volume has
// none yet. A record Orphans would skip is skipped here, and warned about once.
func (it *Item) Committed(ctx context.Context) (int64, int, error) {
	by, err := it.committedByName(ctx)
	if err != nil {
		return 0, 0, err
	}
	var sum int64
	for _, f := range by {
		sum = kinds.AddFootprint(sum, f)
	}
	return sum, len(by), nil
}

// committedByName is the footprint of each volume with a usable drain record,
// by volume name; Committed is its sum.
func (it *Item) committedByName(ctx context.Context) (map[string]int64, error) {
	ents, err := os.ReadDir(it.cfg.CacheDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("volume item: listing the cache directory: %w", err)
	}
	out := map[string]int64{}
	for _, e := range ents {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		name, ok := strings.CutSuffix(e.Name(), drainSuffix)
		if !ok || e.IsDir() {
			continue
		}
		in, err := readDrainRecord(filepath.Join(it.cfg.CacheDir, e.Name()), name)
		var rv vol
		if err == nil {
			rv, err = it.volOf(in)
		}
		if err != nil {
			it.warnRecordOnce("record\x00"+e.Name(), "a drain record is not usable, so its volume is not counted in the node's committed cache", "file", e.Name(), "error", err)
			continue
		}
		out[name] = servedFootprint(rv)
	}
	return out, nil
}

// servedFootprint is the cache footprint of v at the size the item serves it:
// the largest of the class size and the intent's grown size, rounded to whole
// chunks, with the whole-volume cache bound raised with it (as raiseSize does).
// It reads nothing, so a drain record and a stage are counted by one rule; the
// store's own records (raiseSize) can only raise a stage's figure further.
func servedFootprint(v vol) int64 {
	size := max(v.size, chunk.RoundUp(v.in.GrownSize, v.chunk))
	if size > v.size && v.params.CacheSize >= v.params.Size {
		v.bound = size
	}
	p := v.params
	p.Size, p.CacheSize = size, v.bound
	return kinds.CacheFootprint(p)
}

// warnRecordOnce logs a warning the first time key is seen.
func (it *Item) warnRecordOnce(key, msg string, args ...any) {
	it.mu.Lock()
	_, seen := it.warned[key]
	it.warned[key] = struct{}{}
	it.mu.Unlock()
	if !seen {
		it.log.Warn(msg, args...)
	}
}

// checkBudget refuses v when it is new to the node and would not fit.
func (it *Item) checkBudget(ctx context.Context, v vol) error {
	if it.cfg.CacheBudget == nil {
		return nil
	}
	if _, found, err := it.findDevice(v); err != nil || found {
		return err
	}
	if _, err := cache.ReadIndex(v.cacheDir); !errors.Is(err, cache.ErrNoIndex) {
		// A cache is there (or its index is unreadable, which a person
		// judges): the volume is not new.
		return nil
	}
	budget, enabled, err := it.cfg.CacheBudget(ctx)
	if err != nil {
		it.log.Warn("the node's cache budget could not be computed, so the cache-budget check is skipped", "volume", v.name, "error", err)
		return nil
	}
	if !enabled {
		return nil
	}

	// The volume is checked at the size it will be staged at.
	cur, err := it.readCurrent(ctx, v)
	if err != nil {
		return err
	}
	if v, err = it.raiseSize(ctx, v, it.state(v.in.VolumeID), cur); err != nil {
		return err
	}
	need := servedFootprint(v)

	// The volume's own record, left by a stage that failed after writing it or
	// by a release that failed, is not a second volume: it is replaced by need.
	by, err := it.committedByName(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		it.log.Warn("the node's committed cache could not be read, so the cache-budget check is skipped", "volume", v.name, "error", err)
		return nil
	}
	delete(by, v.name)
	var committed int64
	for _, f := range by {
		committed = kinds.AddFootprint(committed, f)
	}
	volumes := len(by)
	if kinds.AddFootprint(committed, need) <= budget {
		return nil
	}
	it.event(v.in.VolumeID, ReasonCacheBudgetExceeded)
	return fmt.Errorf("volume item %s: %w: the node's cache budget is %s, %s is committed to %d volumes, and this volume needs %s",
		v.in.VolumeID, item.ErrCacheBudgetExceeded, bytesString(budget), bytesString(committed), volumes, bytesString(need))
}

func bytesString(n int64) string { return resource.NewQuantity(n, resource.BinarySI).String() }
