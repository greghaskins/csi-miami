package volume

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/greghaskins/csi-miami/internal/cache"
	"github.com/greghaskins/csi-miami/internal/holder"
	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/item"
)

// # The orphan drain (decision 52)
//
// A node that comes back from a crash with a held volume it no longer stages
// (the pod moved, kubelet has no memory of the mount, the intent record is
// gone) drains it on its own: it serves the surviving cache without a key or a
// mount, publishes, verifies against the store and releases. Nothing else
// would ever do it: the holder file names this node, so no other node may take
// the volume over, and the data that is only in the cache would be lost to a
// volume that is held for ever.
//
// Without an intent record the node does not know where the volume is. The
// cache directory is <CacheDir>/<name> and says nothing about the transport,
// so the item keeps a small record beside it while it serves the volume:
// <CacheDir>/<name>.drain.json, the volume's intent (without its publishes).
// It is written before the block server is started, so a cache never exists
// without it, rewritten when it changes, and removed with the cache.

const drainSuffix = ".drain.json"

// drainRecordPath is the record beside v's cache directory.
func (it *Item) drainRecordPath(v vol) string { return v.cacheDir + drainSuffix }

// drainBytes is the record's content: the intent of the volume as the item
// serves it, which is all volOf needs to find the volume again. Publishes,
// the unstaging mark and the generation counter are the reconciler's and mean
// nothing here.
func drainBytes(in intent.Intent) ([]byte, error) {
	in.Publishes, in.Unstaging, in.UnstagingSince, in.Generation = nil, false, time.Time{}, 0
	b, err := json.MarshalIndent(in, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// saveDrainRecord writes the record if it is not already what it should be:
// atomically (a temporary file renamed over it) and durably.
func (it *Item) saveDrainRecord(v vol) error {
	want, err := drainBytes(v.in)
	if err != nil {
		return err
	}
	path := it.drainRecordPath(v)
	if have, err := os.ReadFile(path); err == nil && string(have) == string(want) { //nolint:gosec // the item's own file
		return nil
	}
	if err := os.MkdirAll(it.cfg.CacheDir, 0o750); err != nil {
		return err
	}
	f, err := os.CreateTemp(it.cfg.CacheDir, ".drain-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer func() { _ = os.Remove(tmp) }()
	if _, err := f.Write(want); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// removeDrainRecord removes the record; a missing one is fine.
func (it *Item) removeDrainRecord(v vol) error {
	if err := os.Remove(it.drainRecordPath(v)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("volume item %s: removing the drain record: %w", v.in.VolumeID, err)
	}
	return nil
}

// Orphans returns the intent of every volume that has a drain record and a
// surviving cache directory. Whether the volume is also staged is the
// reconciler's to say (it has the intents); a record whose cache is gone is
// stale and is removed. A record that does not decode, does not validate or
// sits under another volume's name is skipped and left for a person.
func (it *Item) Orphans(ctx context.Context) ([]intent.Intent, error) {
	ents, err := os.ReadDir(it.cfg.CacheDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("volume item: listing the cache directory: %w", err)
	}
	var out []intent.Intent
	for _, e := range ents {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		name, ok := strings.CutSuffix(e.Name(), drainSuffix)
		if !ok || e.IsDir() {
			continue
		}
		path := filepath.Join(it.cfg.CacheDir, e.Name())
		in, err := readDrainRecord(path, name)
		if err != nil {
			it.log.Warn("a drain record is not usable and is left alone", "file", e.Name(), "error", err)
			continue
		}
		v, err := it.volOf(in)
		if err != nil {
			it.log.Warn("a drain record is not usable and is left alone", "file", e.Name(), "error", err)
			continue
		}
		if _, err := os.Stat(v.cacheDir); errors.Is(err, os.ErrNotExist) {
			// Nothing survived: the record is all that is left of a volume that
			// was unstaged or discarded.
			if rerr := os.Remove(path); rerr != nil && !errors.Is(rerr, os.ErrNotExist) {
				it.log.Warn("could not remove a stale drain record", "file", e.Name(), "error", rerr)
			}
			continue
		} else if err != nil {
			it.log.Warn("could not look at a cache directory", "volume", in.VolumeID, "error", err)
			continue
		}
		out = append(out, in)
	}
	return out, nil
}

func readDrainRecord(path, name string) (intent.Intent, error) {
	b, err := os.ReadFile(path) //nolint:gosec // the item's own directory
	if err != nil {
		return intent.Intent{}, err
	}
	var in intent.Intent
	if err := json.Unmarshal(b, &in); err != nil {
		return intent.Intent{}, fmt.Errorf("decode: %w", err)
	}
	if err := in.Validate(); err != nil {
		return intent.Intent{}, err
	}
	id, err := in.ID()
	if err != nil || id.Name != name {
		return intent.Intent{}, errors.New("the record is not under its volume's name")
	}
	return in, nil
}

// Drain drains one orphan volume (see Orphans), or leaves it alone. The caller
// guarantees the volume has no intent record and its transport is mounted.
//
// Nothing is touched unless the volume is plainly unused and plainly ours:
//
//   - something mounted at the stage path, a mount made from the volume's
//     device, an open mapping or a device with other devices stacked on it
//     means someone uses it, and is an error (retried by the caller);
//   - a holder file that names another node, or none, is not ours to drain,
//     and the cache is left as it is (nil: there is nothing to retry);
//   - a cache index that cannot be read may hold writes only a person can
//     judge and is an error.
//
// The cache is then held to the stage's own test (reuse): if the store's
// `current` shows it was left behind, its unpublished writes are discarded,
// reported, and the holder file is released if it is still this node's: it
// holds nothing worth the volume's being stuck. Otherwise the cache is served
// by a block server with no key and no mount (the device above the cache is
// never opened), a final generation is published and verified against the
// store as in an unstage, and only then are the server stopped, the cache
// removed and the holder released. A generation published this way is a crash
// image: the writes that had reached the cache, not a frozen file system.
// ext4 replays its journal when the volume is next mounted.
//
// A publish the store refuses is the epoch fence: an operator's detach moved
// the volume while the drain ran. The cache is discarded, the loss reported,
// and nothing is released (the holder file is the new holder's).
//
// Any other failure leaves the server, the cache and the holder file where
// they are, for the next attempt.
func (it *Item) Drain(ctx context.Context, in intent.Intent) error {
	v, err := it.volOf(in)
	if err != nil {
		return err
	}
	if _, err := os.Stat(v.cacheDir); errors.Is(err, os.ErrNotExist) {
		return it.removeDrainRecord(v)
	}
	idx, err := cache.ReadIndex(v.cacheDir)
	if err != nil {
		if errors.Is(err, cache.ErrNoIndex) {
			return fmt.Errorf("volume item %s: draining: %w: the cache has no index; it is left for an operator", v.in.VolumeID, item.ErrUnstageIncomplete)
		}
		return fmt.Errorf("volume item %s: draining: %w: the cache index is unreadable (%v); the cache is left for an operator", v.in.VolumeID, item.ErrUnstageIncomplete, err)
	}
	// The server is started with the geometry of the cache it serves.
	v.size, v.chunk = idx.Size, idx.Chunk
	if v.bound < v.size {
		v.bound = v.size
	}

	if err := it.drainable(ctx, v); err != nil {
		return err
	}

	h, err := it.cfg.Holder.Adopt(ctx, v.holdDir, v.name, it.cfg.Node)
	switch {
	case errors.Is(err, holder.ErrHeld), errors.Is(err, holder.ErrLost):
		// Another node holds the volume, or the file is absent (which a
		// transport that is not mounted looks like): not ours to drain.
		it.log.Debug("a surviving cache is not drained: this node does not hold the volume", "volume", v.in.VolumeID, "reason", err)
		return nil
	case err != nil:
		return fmt.Errorf("volume item %s: draining: %w: reading the holder file: %w", v.in.VolumeID, item.ErrUnstageIncomplete, err)
	}
	st := it.state(v.in.VolumeID)
	st.fz.Lock()
	st.tearingDown = true // no generation starts while it drains
	st.h = h
	st.fz.Unlock()

	cur, err := it.readCurrent(ctx, v)
	if err != nil {
		return fmt.Errorf("volume item %s: draining: %w: %w", v.in.VolumeID, item.ErrUnstageIncomplete, err)
	}
	dirty := int64(len(idx.Dirty)) * v.chunk
	if !reuse(idx, cur) {
		it.lost(v, dirty, "the store has moved past the surviving cache")
		return it.finishDrain(ctx, v, st, false)
	}
	if cur.ok && cur.epoch > h.Epoch() {
		it.lost(v, dirty, "our ownership was fenced by a newer generation; its data was given up")
		return it.finishDrain(ctx, v, st, false)
	}

	it.log.Info("draining a volume this node holds and no longer stages; the generation published is a crash image (the writes that reached the cache, not a frozen file system) and ext4 replays its journal at the next mount", "volume", v.in.VolumeID, "dirtyBytes", dirty)
	if _, err := it.ensureServer(ctx, v, st, h, cur); err != nil {
		return it.incomplete(ctx, v, st, "the block server cannot be started to drain: %v", err)
	}
	if err := it.syncOut(ctx, v, st); err != nil {
		return err
	}
	return it.finishDrain(ctx, v, st, true)
}

// finishDrain takes the server down (if any), removes the cache and its
// record, and releases the holder file if it is still ours. A release after the
// fence is a no-op: the file names the node that took the volume.
func (it *Item) finishDrain(ctx context.Context, v vol, st *vstate, served bool) error {
	if served || it.serverPresent(ctx, v) {
		if err := it.stopServerAndUnit(ctx, v); err != nil {
			return err
		}
	}
	if err := it.discardCache(v, "the volume was drained"); err != nil {
		return err
	}
	if err := it.removeDrainRecord(v); err != nil {
		return err
	}
	it.release(ctx, v, st)
	st.fz.Lock()
	st.gone = true
	st.fz.Unlock()
	return nil
}

// serverPresent reports whether a unit or a device of the volume exists.
func (it *Item) serverPresent(ctx context.Context, v vol) bool {
	if us, err := it.cfg.Systemd.State(ctx, v.unit); err != nil || us.Present {
		return true
	}
	_, ok, err := it.findDevice(v)
	return err != nil || ok
}

// drainable checks that nothing uses the volume on this node.
func (it *Item) drainable(ctx context.Context, v vol) error {
	busy := func(format string, args ...any) error {
		return fmt.Errorf("volume item %s: draining: %w: %s", v.in.VolumeID, item.ErrUnstageIncomplete, fmt.Sprintf(format, args...))
	}
	mounted, err := it.mounted(ctx, v)
	if err != nil {
		return err
	}
	if mounted {
		return busy("something is mounted at %s, so the volume is in use", v.stage)
	}
	dev, ok, err := it.findDevice(v)
	if err != nil {
		return err
	}
	if ok {
		if err := it.deviceFree(ctx, v, dev); err != nil {
			return busy("%v", err)
		}
	}
	return nil
}
