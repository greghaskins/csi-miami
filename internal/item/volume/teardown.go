package volume

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"

	"github.com/greghaskins/csi-miami/internal/blockd"
	"github.com/greghaskins/csi-miami/internal/cache"
	"github.com/greghaskins/csi-miami/internal/crypt"
	"github.com/greghaskins/csi-miami/internal/holder"
	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/item"
	"github.com/greghaskins/csi-miami/internal/observe"
)

var nbdSource = regexp.MustCompile(`^/dev/nbd[0-9]+$`)

// ownsStage is the item.Owned test for the stage mount: an ext4 file system on
// this volume's mapper or on an nbd device. A bind or anything else at the
// stage path is not the volume's, and is left alone.
func (it *Item) ownsStage(v vol) item.Owned {
	mapper := crypt.MapperPath(crypt.Name(v.name))
	return func(_ observe.Table, top observe.Mount) (bool, error) {
		return top.FSType == "ext4" && (sameDevice(top.Source, mapper) || nbdSource.MatchString(top.Source)), nil
	}
}

// Teardown undoes Converge in the order of the spec: the final generation,
// unmount, close the mapping, stop the server and its unit, remove the cache,
// release the holder. A final generation that is refused or cannot be made
// does not fail the unstage: the writes it would have published are discarded
// with the cache, and the loss is logged and reported as an event. The
// holder's release is likewise best effort, because a transport that is down
// must not keep a volume from unstaging; the holder then ages out and is taken
// over by the usual conditions. A busy mount is an error, retried by the
// caller, before anything past it is touched.
func (it *Item) Teardown(ctx context.Context, in intent.Intent, _ item.Observed) error {
	v, err := it.volOf(in)
	if err != nil {
		return err
	}
	st := it.state(in.VolumeID)

	it.finalGeneration(ctx, v, st)
	if err := it.stopStack(ctx, v); err != nil {
		return err
	}
	if err := it.discardCache(v, "the volume is unstaged"); err != nil {
		return err
	}
	it.release(ctx, v, st)
	it.forget(in.VolumeID)
	return nil
}

// stopStack takes down what the item raised above the store and below the
// holder: the mount, the mapping and the block server. It is the shared body
// of an unstage and of a re-fence. Each step is skipped when its layer is
// already gone, so it can be repeated after a partial failure.
func (it *Item) stopStack(ctx context.Context, v vol) error {
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
	// Ask the server to stop first, so that it disconnects its device and
	// syncs its cache; whether it answers or not, the unit is then stopped,
	// which also stops a server that was restarting.
	it.stopServer(ctx, v)
	if err := it.cfg.Systemd.Stop(ctx, v.unit); err != nil {
		return fmt.Errorf("volume item %s: %w: stopping the unit: %w", v.in.VolumeID, item.ErrServerDown, err)
	}
	_ = os.Remove(v.socket)
	return nil
}

// stopServer sends the server its Stop request, best effort.
func (it *Item) stopServer(ctx context.Context, v vol) {
	cctx, cancel := context.WithTimeout(ctx, it.cfg.Timeout)
	defer cancel()
	c, err := it.cfg.Dial(cctx, v.socket)
	if err != nil {
		return
	}
	defer func() { _ = c.Close() }()
	if err := c.Stop(cctx); err != nil {
		it.log.Debug("the block server did not take the stop request", "volume", v.in.VolumeID, "error", err)
	}
}

// finalGeneration attempts the last generation of a volume once, under its
// own deadline, while the file system is still mounted. Whatever happens it
// returns: a refusal and a failure only decide which event reports the
// unpublished writes that the unstage is about to discard.
func (it *Item) finalGeneration(ctx context.Context, v vol, st *vstate) {
	us, err := it.cfg.Systemd.State(ctx, v.unit)
	if err != nil || !us.Present || us.Active != "active" {
		it.reportDiscard(v, "the block server is not running; its cache is discarded unpublished")
		return
	}
	gctx, cancel := context.WithTimeout(ctx, it.cfg.PublishTimeout)
	defer cancel()
	err = it.generate(gctx, v, st, "unstage")
	switch {
	case err == nil, errors.Is(err, ErrGenerationSkipped):
		return
	case errors.Is(err, item.ErrPublishRefused):
		it.log.Warn("the final generation was refused; the unpublished writes are discarded", "volume", v.in.VolumeID, "error", err)
		it.event(v.in.VolumeID, ReasonPublishRefused)
	default:
		it.log.Warn("the final generation failed; the unpublished writes are discarded", "volume", v.in.VolumeID, "error", err)
		it.event(v.in.VolumeID, ReasonGenerationLost)
	}
}

// reportDiscard reports unpublished writes in a cache about to be removed,
// when there are any.
func (it *Item) reportDiscard(v vol, msg string) {
	idx, err := cache.ReadIndex(v.cacheDir)
	if err != nil || len(idx.Dirty) == 0 {
		return
	}
	it.log.Warn(msg, "volume", v.in.VolumeID, "chunks", len(idx.Dirty))
	it.event(v.in.VolumeID, ReasonGenerationLost)
}

// release gives the holder file up, best effort: the file is removed only if
// it is still ours, and a transport that does not answer leaves it to age out.
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
