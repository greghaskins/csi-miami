package volume

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/greghaskins/csi-miami/internal/blockd"
	"github.com/greghaskins/csi-miami/internal/cache"
	"github.com/greghaskins/csi-miami/internal/crypt"
	"github.com/greghaskins/csi-miami/internal/holder"
	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/item"
	"github.com/greghaskins/csi-miami/internal/observe"
)

// ownsStage is the item.Owned test for the stage mount: an ext4 file system on
// this volume's mapper or on this volume's own nbd device. A bind, another
// volume's device or anything else at the stage path is not the volume's, and
// is left alone.
func (it *Item) ownsStage(v vol) item.Owned {
	mapper := crypt.MapperPath(crypt.Name(v.name))
	own := ""
	if dev, ok, err := it.findDevice(v); err == nil && ok {
		own = it.devPath(dev.Index)
	}
	return func(_ observe.Table, top observe.Mount) (bool, error) {
		return top.FSType == "ext4" && (sameDevice(top.Source, mapper) || (own != "" && sameDevice(top.Source, own))), nil
	}
}

// Teardown is a verified clean release of the volume (decision 52). In order:
// thaw (a plugin that died inside a freeze must not wedge the unmount),
// unmount, close the mapping, wait out any publish already in flight, then the
// final generation, then a check against the store itself: `current` is read
// afresh and must name what the server just published (or, when nothing was
// to publish, the manifest the cache descends from), and the server must
// report no publish running. Only then are the server stopped (the one call
// that disconnects the device), its unit stopped, the cache removed and the
// holder released.
//
// Anything short of that, before the server is stopped, is an
// item.ErrUnstageIncomplete: retryable, and the server, the cache and the
// holder file are left exactly as they were, so the volume stays with the node
// that holds the unsynced data. A retry skips every step already done; an
// earlier attempt's publish that kept running inside the server is accepted by
// the next attempt, whose own publish then finds nothing to add and whose
// verification finds `current` at the cache's base.
//
// A publish the store refuses means the holder was abandoned by an operator
// and the epoch fence has already given its data up: the cache is discarded,
// the loss is reported, and the unstage succeeds. So does an unstage whose
// holder file now names another node.
//
// A busy mount is an error, retried by the caller, before anything past it is
// touched.
func (it *Item) Teardown(ctx context.Context, in intent.Intent, _ item.Observed) error {
	v, err := it.volOf(in)
	if err != nil {
		return err
	}
	st := it.state(in.VolumeID)
	// Wait out a freeze in progress, then keep generations from starting.
	st.fz.Lock()
	st.tearingDown = true
	st.fz.Unlock()

	if err := it.unmountAndClose(ctx, v, st); err != nil {
		return err
	}
	if err := it.syncOut(ctx, v, st); err != nil {
		return err
	}
	if err := it.stopServerAndUnit(ctx, v); err != nil {
		return err
	}
	if err := it.discardCache(v, "the volume is unstaged"); err != nil {
		return err
	}
	it.release(ctx, v, st)
	st.fz.Lock()
	st.gone = true
	st.fz.Unlock()
	return nil
}

// unmountAndClose takes the mount and the mapping down: it thaws the file
// system first (EINVAL, "not frozen", is the normal answer), then unmounts
// once, never lazily, and closes the mapping. Each step is skipped when its
// layer is already gone, so it can be repeated after a partial failure.
func (it *Item) unmountAndClose(ctx context.Context, v vol, st *vstate) error {
	mounted, err := it.mounted(ctx, v)
	if err != nil {
		return err
	}
	if mounted {
		st.fz.Lock()
		terr := it.cfg.Thaw(v.stage)
		st.fz.Unlock()
		if terr != nil {
			return fmt.Errorf("volume item %s: thawing before the unmount: %w", v.in.VolumeID, terr)
		}
	}
	if err := item.UnmountIf(ctx, it.cfg.Mounter, v.stage, it.ownsStage(v), "an ext4 mount of the volume's device", item.ErrStageConflict); err != nil {
		return fmt.Errorf("volume item %s: %w", v.in.VolumeID, err)
	}
	if v.params.Encryption {
		name := crypt.Name(v.name)
		present, err := it.dmPresent(name)
		if err != nil {
			return fmt.Errorf("volume item %s: looking for the mapping: %w", v.in.VolumeID, err)
		}
		if present {
			if err := crypt.Close(ctx, it.cfg.Tools, name); err != nil {
				return fmt.Errorf("volume item %s: %w", v.in.VolumeID, err)
			}
		}
	}
	return nil
}

// stopServerAndUnit stops the block server, by its Stop request first so that
// it syncs its cache and disconnects its device, and then its unit, which also
// stops a server that was restarting. The request is best effort and its
// failure is logged with the device; the unit's stop is not.
func (it *Item) stopServerAndUnit(ctx context.Context, v vol) error {
	it.stopServer(ctx, v)
	if err := it.cfg.Systemd.Stop(ctx, v.unit); err != nil {
		return fmt.Errorf("volume item %s: %w: stopping the unit: %w", v.in.VolumeID, item.ErrServerDown, err)
	}
	_ = os.Remove(v.socket)
	return nil
}

// stopServer sends the server its Stop request. A dial or a request that
// fails is a Warn naming the device, because the device is what stays behind
// if the unit stop does not take it down.
func (it *Item) stopServer(ctx context.Context, v vol) {
	device := -1
	if dev, ok, err := it.findDevice(v); err == nil && ok {
		device = dev.Index
	}
	cctx, cancel := context.WithTimeout(ctx, it.cfg.Timeout)
	defer cancel()
	c, err := it.cfg.Dial(cctx, v.socket)
	if err != nil {
		it.log.Warn("could not reach the block server to stop it", "volume", v.in.VolumeID, "device", device, "error", err)
		return
	}
	defer func() { _ = c.Close() }()
	if err := c.Stop(cctx); err != nil {
		it.log.Warn("the block server did not take the stop request", "volume", v.in.VolumeID, "device", device, "error", err)
	}
}

// incomplete wraps a cause as the retryable unstage error.
func (it *Item) incomplete(v vol, format string, args ...any) error {
	return fmt.Errorf("volume item %s: %w: %s", v.in.VolumeID, item.ErrUnstageIncomplete, fmt.Sprintf(format, args...))
}

// syncOut gets the volume's last writes into the store and proves it, or
// returns the retryable error. It returns nil when they are there, and when
// the holder was abandoned (the data is given up and reported).
func (it *Item) syncOut(ctx context.Context, v vol, st *vstate) error {
	up, err := it.serverUp(ctx, v, st)
	if err != nil {
		return err
	}
	if !up {
		return nil // nothing of ours to sync, or abandoned: serverUp said which
	}
	pctx, cancel := context.WithTimeout(ctx, it.cfg.PublishTimeout)
	defer cancel()

	cctx, ccancel := context.WithTimeout(ctx, it.cfg.Timeout)
	c, err := it.cfg.Dial(cctx, v.socket)
	ccancel()
	if err != nil {
		return it.incomplete(v, "the block server does not answer: %v", err)
	}
	defer func() { _ = c.Close() }()

	// Wait for any publish already running, then mark the boundary: always,
	// even with nothing dirty, since the dirty byte count leaves out trimmed
	// chunks. The file system is unmounted, so there is nothing to freeze.
	for {
		if err := it.waitNoPublish(pctx, v, c); err != nil {
			return err
		}
		st.fz.Lock()
		err := it.freezeAndMark(pctx, v, c)
		st.fz.Unlock()
		if errors.Is(err, ErrGenerationSkipped) {
			continue // a publish began between the status and the boundary
		}
		if err != nil {
			return it.incomplete(v, "marking the final boundary: %v", err)
		}
		break
	}

	reply, err := c.Publish(pctx, "unstage")
	switch {
	case errors.Is(err, blockd.ErrRefused) || (err == nil && reply.Refused):
		// Fenced: an operator gave this holder's data up.
		ss, _ := c.Status(pctx)
		it.lost(v, ss.Dirty, "the final generation was refused; the holder was abandoned")
		return nil
	case err != nil:
		return it.incomplete(v, "the final generation failed: %v", err)
	}

	// Verify against the store itself, fresh, and then against the server.
	ss, err := c.Status(pctx)
	if err != nil {
		return it.incomplete(v, "the block server does not answer after the final generation: %v", err)
	}
	want := reply.Name
	if want == "" {
		want = ss.Base // nothing to add: the store must already name what the cache descends from
	}
	cur, err := it.readCurrent(pctx, v)
	if err != nil {
		return it.incomplete(v, "reading the store after the final generation: %v", err)
	}
	if got := cur.name; got != want {
		return it.incomplete(v, "the store names %q, the final generation is %q", got, want)
	}
	if ss.PublishInFlight {
		return it.incomplete(v, "a publish is still running")
	}
	it.log.Info("the final generation is in the store", "volume", v.in.VolumeID, "manifest", want, "dirtyBytes", ss.Dirty)
	return nil
}

// waitNoPublish polls the server until no publish is running, within the
// context's deadline.
func (it *Item) waitNoPublish(ctx context.Context, v vol, c Control) error {
	for {
		ss, err := c.Status(ctx)
		if err != nil {
			return it.incomplete(v, "the block server does not answer: %v", err)
		}
		if !ss.PublishInFlight {
			return nil
		}
		select {
		case <-ctx.Done():
			return it.incomplete(v, "a publish is still in flight after %s", it.cfg.PublishTimeout)
		case <-time.After(it.cfg.PollEvery):
		}
	}
}

// serverUp makes sure a server is running to publish from, when the cache
// holds anything to publish. It returns false, nil when there is nothing to
// sync: no cache, or the holder is no longer ours (abandoned, or fenced), in
// which case the loss is reported. A server that is gone while the holder is
// still ours is started again, as a drain, and a failure to start it is
// retryable.
func (it *Item) serverUp(ctx context.Context, v vol, st *vstate) (bool, error) {
	us, err := it.cfg.Systemd.State(ctx, v.unit)
	if err == nil && us.Present && us.Active == "active" {
		return true, nil
	}
	idx, ierr := cache.ReadIndex(v.cacheDir)
	if errors.Is(ierr, cache.ErrNoIndex) {
		return false, nil
	}
	dirty := int64(len(idx.Dirty)) * v.chunk
	h := st.h
	if h == nil {
		var aerr error
		if h, aerr = it.cfg.Holder.Adopt(ctx, v.holdDir, v.name, it.cfg.Node); aerr != nil {
			if errors.Is(aerr, holder.ErrHeld) || errors.Is(aerr, holder.ErrLost) {
				it.lost(v, dirty, "the holder is no longer ours; its data was given up")
				return false, nil
			}
			return false, it.incomplete(v, "reading the holder file: %v", aerr)
		}
		st.h = h
	}
	cur, cerr := it.readCurrent(ctx, v)
	if cerr != nil {
		return false, it.incomplete(v, "reading the store: %v", cerr)
	}
	if cur.ok && cur.epoch > h.Epoch() {
		it.lost(v, dirty, "our ownership was fenced by a newer generation; its data was given up")
		return false, nil
	}
	if _, serr := it.ensureServer(ctx, v, st, h, cur); serr != nil {
		return false, it.incomplete(v, "the block server is not running and cannot be started to sync: %v", serr)
	}
	return true, nil
}

// lost reports unpublished writes that are being discarded: an event of fixed
// text and a log line with the byte count.
func (it *Item) lost(v vol, bytes int64, why string) {
	it.log.Warn(why+"; unpublished writes are discarded", "volume", v.in.VolumeID, "bytes", bytes)
	it.event(v.in.VolumeID, ReasonGenerationLost)
}

// release gives the holder file up: it is removed only if it is still ours. A
// failure is logged; the file then ages out under the usual conditions.
func (it *Item) release(ctx context.Context, v vol, st *vstate) {
	h := st.h
	if h == nil {
		var err error
		if h, err = it.cfg.Holder.Adopt(ctx, v.holdDir, v.name, it.cfg.Node); err != nil {
			if !errors.Is(err, holder.ErrLost) && !errors.Is(err, holder.ErrHeld) {
				it.log.Warn("could not look at the holder file to release it", "volume", v.in.VolumeID, "error", err)
			}
			return
		}
	}
	if err := h.Release(ctx); err != nil {
		it.log.Warn("could not release the holder file", "volume", v.in.VolumeID, "error", err)
	}
	st.h = nil
}

// publishRefused wraps blockd's refusal with the item's condition.
func publishRefused() error {
	return fmt.Errorf("%w: %w", item.ErrPublishRefused, blockd.ErrRefused)
}
