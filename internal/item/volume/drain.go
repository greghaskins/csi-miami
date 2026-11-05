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
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	// The rename is what makes the record exist after a crash.
	d, err := os.Open(it.cfg.CacheDir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}

// removeDrainRecord removes the record; a missing one is fine.
func (it *Item) removeDrainRecord(v vol) error {
	if err := os.Remove(it.drainRecordPath(v)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("volume item %s: removing the drain record: %w", v.in.VolumeID, err)
	}
	return nil
}

// Orphans returns the intent of every volume that has a drain record. Whether
// the volume is also staged is the reconciler's to say (it has the intents).
// A record whose cache is gone is returned too: the record outlives the cache
// until the holder file is released, so that a release that failed is found
// and retried (Drain). A record that does not decode, does not validate or sits
// under another volume's name is skipped and left for a person.
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
		if _, err := it.volOf(in); err != nil {
			it.log.Warn("a drain record is not usable and is left alone", "file", e.Name(), "error", err)
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
		// The cache is gone: it was removed after a verified publish or on
		// proof. What may remain is the holder file, if that release failed;
		// the record is kept until it is done.
		return it.finishDrain(ctx, v, it.state(v.in.VolumeID), false)
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
	dirty := int64(len(idx.Dirty)) * v.chunk

	if err := it.drainable(ctx, v); err != nil {
		return err
	}
	st := it.state(v.in.VolumeID)
	st.fz.Lock()
	st.tearingDown = true // no generation starts while it drains
	st.fz.Unlock()

	h, err := it.cfg.Holder.Adopt(ctx, v.holdDir, v.name, it.cfg.Node)
	switch {
	case errors.Is(err, holder.ErrHeld):
		// Proof, as in an unstage: the holder file names another node, so the
		// volume was given up and this cache's writes can never be published.
		it.lost(v, dirty, "the holder file names another node; the volume was given up")
		return it.finishDrain(ctx, v, st, false)
	case errors.Is(err, holder.ErrLost):
		// No holder file. Absence alone proves nothing (the transport may not be
		// mounted); a store that has moved on past the cache does.
		cur, cerr := it.readCurrent(ctx, v)
		switch {
		case errors.Is(cerr, item.ErrItemMissing):
			if !it.transportMounted(ctx, v) {
				return it.incomplete(ctx, v, st, "the volume's directory is missing and its transport does not look mounted")
			}
			it.lost(v, dirty, "the volume's directory is gone; it was deleted")
			return it.finishDrain(ctx, v, st, false)
		case cerr != nil:
			return fmt.Errorf("volume item %s: draining: %w: %w", v.in.VolumeID, item.ErrUnstageIncomplete, cerr)
		case cur.ok && cur.epoch > idx.Epoch:
			it.lost(v, dirty, "there is no holder file and the store has moved past the surviving cache")
			return it.finishDrain(ctx, v, st, false)
		}
		it.warnOnce(v, "a surviving cache has no holder file and no proof the volume was given up, so it is left alone; its unpublished writes stay until an operator removes the cache directory by hand, or stages the volume here again. The emergency detach (PV annotation force-detach) acts on a holder file and has none to act on")
		return nil
	case err != nil:
		return fmt.Errorf("volume item %s: draining: %w: reading the holder file: %w", v.in.VolumeID, item.ErrUnstageIncomplete, err)
	}
	// The earlier attempt's holder, if any, is replaced: close its descriptor.
	st.fz.Lock()
	if st.h != nil {
		_ = st.h.Close()
	}
	st.h = h
	st.fz.Unlock()

	cur, err := it.readCurrent(ctx, v)
	if errors.Is(err, item.ErrItemMissing) && it.transportMounted(ctx, v) {
		it.lost(v, dirty, "the volume's directory is gone; it was deleted")
		return it.finishDrain(ctx, v, st, false)
	}
	if err != nil {
		return fmt.Errorf("volume item %s: draining: %w: %w", v.in.VolumeID, item.ErrUnstageIncomplete, err)
	}
	if !reuse(idx, cur) {
		it.lost(v, dirty, "the store has moved past the surviving cache")
		return it.finishDrain(ctx, v, st, false)
	}
	if cur.ok && cur.epoch > max(idx.Epoch, h.Epoch()) {
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

// warnOnce logs msg at Warn the first time for the volume in this process.
func (it *Item) warnOnce(v vol, msg string) {
	it.mu.Lock()
	_, seen := it.warned[v.in.VolumeID]
	it.warned[v.in.VolumeID] = struct{}{}
	it.mu.Unlock()
	if seen {
		it.log.Debug(msg, "volume", v.in.VolumeID)
		return
	}
	it.log.Warn(msg, "volume", v.in.VolumeID, "cache", v.cacheDir)
}

// transportMounted reports whether the volume's transport is in the mount
// table: a store directory that is "gone" under a transport that is not
// mounted is an empty mount point, not a deleted volume.
func (it *Item) transportMounted(ctx context.Context, v vol) bool {
	mp, err := v.in.Transport.MountPoint(it.cfg.Root)
	if err != nil {
		return false
	}
	t, err := it.cfg.Mounter.Mounts(ctx)
	if err != nil {
		return false
	}
	_, ok := t.At(mp)
	return ok
}

// finishDrain takes the server down (if any), removes the cache, releases the
// holder file if it is still ours, and only then removes the record: a release
// that fails leaves the record, so the next pass finds the volume and releases
// again. A release after the fence is a no-op: the file names the node that
// took the volume.
func (it *Item) finishDrain(ctx context.Context, v vol, st *vstate, served bool) error {
	if served || it.serverPresent(ctx, v) {
		if err := it.stopServerAndUnit(ctx, v); err != nil {
			return err
		}
	}
	if err := it.discardCache(v, "the volume was drained"); err != nil {
		return err
	}
	if err := it.release(ctx, v, st); err != nil {
		return it.incomplete(ctx, v, st, "releasing the holder file: %v", err)
	}
	if err := it.removeDrainRecord(v); err != nil {
		return err
	}
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
