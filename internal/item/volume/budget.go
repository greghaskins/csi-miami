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
	ents, err := os.ReadDir(it.cfg.CacheDir)
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, fmt.Errorf("volume item: listing the cache directory: %w", err)
	}
	var sum int64
	var n int
	for _, e := range ents {
		if err := ctx.Err(); err != nil {
			return 0, 0, err
		}
		name, ok := strings.CutSuffix(e.Name(), drainSuffix)
		if !ok || e.IsDir() {
			continue
		}
		in, err := readDrainRecord(filepath.Join(it.cfg.CacheDir, e.Name()), name)
		if err == nil {
			_, err = it.volOf(in)
		}
		if err != nil {
			it.warnRecordOnce("record\x00"+e.Name(), "a drain record is not usable, so its volume is not counted in the node's committed cache", "file", e.Name(), "error", err)
			continue
		}
		sum = kinds.AddFootprint(sum, kinds.CacheFootprint(*in.Volume))
		n++
	}
	return sum, n, nil
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
	p := v.params
	p.Size, p.CacheSize = v.size, v.bound
	need := kinds.CacheFootprint(p)

	committed, volumes, err := it.Committed(ctx)
	if err != nil {
		it.log.Warn("the node's committed cache could not be read, so the cache-budget check is skipped", "volume", v.name, "error", err)
		return nil
	}
	if kinds.AddFootprint(committed, need) <= budget {
		return nil
	}
	it.event(v.in.VolumeID, ReasonCacheBudgetExceeded)
	return fmt.Errorf("volume item %s: %w: the node's cache budget is %s, %s is committed to %d volumes, and this volume needs %s",
		v.in.VolumeID, item.ErrCacheBudgetExceeded, bytesString(budget), bytesString(committed), volumes, bytesString(need))
}

func bytesString(n int64) string { return resource.NewQuantity(n, resource.BinarySI).String() }
