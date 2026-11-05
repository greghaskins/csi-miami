package volume

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/blockd"
	"github.com/greghaskins/csi-miami/internal/ext4"
	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/item"
)

// Generation makes one generation of a staged volume: it freezes the file
// system, has the block server mark its boundary, thaws, and has the server
// publish. It is what the reconciler's generation timers call, on their own
// goroutine; it takes only what the item holds in memory and the volume's
// intent.
//
// A server that already has a publish in flight is not asked to freeze
// anything: the generation is ErrGenerationSkipped, and the next trigger tries
// again. The thaw is deferred, so it also runs when the boundary fails, and a
// watchdog thaws once FreezeBound has passed in case the boundary call hangs:
// a file system stays frozen for milliseconds, never for a stuck RPC. A refused
// publish is item.ErrPublishRefused, with the dirty set kept by the server.
func (it *Item) Generation(ctx context.Context, in intent.Intent) error {
	return it.GenerationFor(ctx, in, "interval")
}

// GenerationFor is Generation for a given trigger: interval, watermark or
// request (blockd.PublishArgs).
func (it *Item) GenerationFor(ctx context.Context, in intent.Intent, trigger string) error {
	v, err := it.volOf(in)
	if err != nil {
		return err
	}
	return it.generate(ctx, v, it.state(in.VolumeID), trigger)
}

// FreezeBound is how long a file system may stay frozen by a generation; the
// reconciler waits that long for generations to thaw at exit.
func (it *Item) FreezeBound() time.Duration { return it.cfg.FreezeBound }

// Status is the block server's status for a staged volume, read without
// freezing or changing anything. The reconciler's generation timers decide
// from it (dirty bytes, the last successful publish); a server that does not
// answer is item.ErrServerDown.
func (it *Item) Status(ctx context.Context, in intent.Intent) (blockd.Status, error) {
	v, err := it.volOf(in)
	if err != nil {
		return blockd.Status{}, err
	}
	st, err := it.status(ctx, v)
	if err != nil {
		return blockd.Status{}, fmt.Errorf("volume item %s: %w: %w", in.VolumeID, item.ErrServerDown, err)
	}
	return st, nil
}

// ServerOutdated reports whether the block server that produced st runs a
// binary other than the one the node is running now (a plugin upgrade leaves
// a running server on its old binary until the volume is unstaged). It says
// false until a converge has hashed the running binary, and for a status that
// carries no hash.
func (it *Item) ServerOutdated(st blockd.Status) bool {
	want := it.srcHex.Load()
	return want != nil && st.BinaryHash != "" && st.BinaryHash != *want
}

// ProbeDevice is the block device the volume's file system is on, which the
// reconciler's active probe reads the first sector of: the mapper for an
// encrypted volume, the nbd device otherwise. It is what the last Converge
// that reached the mapping layer found, from memory; false before any.
//
// stack changes whenever the stack was stopped to be rebuilt in place (a
// fenced ownership, a server at another epoch), so the probe can drop a read
// that is still in flight on the old device.
func (it *Item) ProbeDevice(in intent.Intent) (dev string, stack uint64, ok bool) {
	it.mu.Lock()
	defer it.mu.Unlock()
	s := it.vols[in.VolumeID]
	if s == nil || s.blk == "" {
		return "", 0, false
	}
	return s.blk, s.stack, true
}

// generate is one generation. Only the freeze and the boundary run under the
// volume's freeze lock (one freeze at a time, and never a thaw of an adopted
// mount inside one); the upload does not, because it takes as long as the
// transport likes and the reconciler's loop, an unstage and every other
// pass must not wait behind it. The server serialises publishes itself. The
// upload runs under its own deadline, PublishTimeout.
func (it *Item) generate(ctx context.Context, v vol, st *vstate, trigger string) error {
	c, err := it.freezeSection(ctx, v, st)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()

	pctx, cancel := context.WithTimeout(ctx, it.cfg.PublishTimeout)
	defer cancel()
	return it.publish(pctx, v, c, trigger)
}

// publish asks the server to upload the boundary generation and maps a
// refusal to the item's condition.
func (it *Item) publish(ctx context.Context, v vol, c Control, trigger string) error {
	reply, err := c.Publish(ctx, trigger)
	switch {
	case errors.Is(err, blockd.ErrRefused) || (err == nil && reply.Refused):
		return fmt.Errorf("volume item %s: %w", v.in.VolumeID, publishRefused())
	case err != nil:
		return fmt.Errorf("volume item %s: publishing the generation: %w", v.in.VolumeID, err)
	}
	return nil
}

// freezeSection dials the server, refuses when a publish is in flight or the
// volume is leaving, and freezes, marks the boundary and thaws, all under the
// freeze lock. It returns the open control connection for the upload.
func (it *Item) freezeSection(ctx context.Context, v vol, st *vstate) (Control, error) {
	st.fz.Lock()
	defer st.fz.Unlock()
	if st.tearingDown || st.gone {
		return nil, fmt.Errorf("volume item %s: %w: the volume is being unstaged", v.in.VolumeID, ErrGenerationSkipped)
	}
	c, err := it.markBoundary(ctx, v)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// markBoundary is the freeze and boundary for a generation, the caller holding
// the freeze lock. It returns the control connection with the boundary set.
func (it *Item) markBoundary(ctx context.Context, v vol) (Control, error) {
	cctx, cancel := context.WithTimeout(ctx, it.cfg.Timeout)
	c, err := it.cfg.Dial(cctx, v.socket)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("volume item %s: %w: %w", v.in.VolumeID, item.ErrServerDown, err)
	}
	sctx, scancel := context.WithTimeout(ctx, it.cfg.Timeout)
	status, err := c.Status(sctx)
	scancel()
	if err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("volume item %s: %w: %w", v.in.VolumeID, item.ErrServerDown, err)
	}
	if err := it.identity(v, status); err != nil {
		_ = c.Close()
		return nil, err
	}
	if status.PublishInFlight {
		_ = c.Close()
		return nil, fmt.Errorf("volume item %s: %w", v.in.VolumeID, ErrGenerationSkipped)
	}
	if err := it.freezeAndMark(ctx, v, c); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

// freezeAndMark freezes the stage mount, has the server mark the boundary and
// thaws, in a function of its own so the thaw is a defer: it runs when the
// boundary fails, when it panics, and, through the watchdog, when it hangs.
// A volume with no mount (an unstage that finds it unmounted) marks the
// boundary with nothing to freeze. A boundary that returns after the watchdog
// thawed was taken while writes ran, and is not published.
func (it *Item) freezeAndMark(ctx context.Context, v vol, c Control) error {
	thaw := func() error { return nil }
	if mounted, err := it.mounted(ctx, v); err != nil {
		return err
	} else if mounted {
		t, err := it.cfg.Freeze(v.stage)
		if err != nil {
			return fmt.Errorf("volume item %s: freezing the file system: %w", v.in.VolumeID, err)
		}
		var once sync.Once
		thaw = func() error {
			var terr error
			once.Do(func() { terr = t() })
			return terr
		}
	}
	var fired atomic.Bool
	watchdog := time.AfterFunc(it.cfg.FreezeBound, func() {
		fired.Store(true)
		if err := thaw(); err != nil {
			it.log.Error("the watchdog could not thaw the file system", "volume", v.in.VolumeID, "error", err)
		}
	})
	defer func() {
		watchdog.Stop()
		if err := thaw(); err != nil {
			it.log.Error("could not thaw the file system", "volume", v.in.VolumeID, "error", err)
		}
	}()

	bctx, cancel := context.WithTimeout(ctx, it.cfg.FreezeBound)
	defer cancel()
	switch err := c.Boundary(bctx); {
	case errors.Is(err, blockd.ErrPublishInFlight):
		return fmt.Errorf("volume item %s: %w", v.in.VolumeID, ErrGenerationSkipped)
	case err != nil:
		return fmt.Errorf("volume item %s: marking the boundary: %w", v.in.VolumeID, err)
	case fired.Load():
		return fmt.Errorf("volume item %s: the file system was thawed by the watchdog before the boundary was marked; not publishing it", v.in.VolumeID)
	}
	return nil
}

// mounted reports whether anything is mounted at the stage path.
func (it *Item) mounted(ctx context.Context, v vol) (bool, error) {
	t, err := it.cfg.Mounter.Mounts(ctx)
	if err != nil {
		return false, fmt.Errorf("volume item %s: %w: %w", v.in.VolumeID, item.ErrObserve, err)
	}
	_, ok := t.At(v.stage)
	return ok, nil
}

// realFreeze is Config.Freeze: FIFREEZE through a descriptor on the mount's
// root, which stays open until the thaw closes it.
func realFreeze(path string) (func() error, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", path, err)
	}
	if err := ext4.Freeze(fd); err != nil {
		_ = unix.Close(fd)
		return nil, err
	}
	var once sync.Once
	var terr error
	return func() error {
		once.Do(func() {
			terr = ext4.Thaw(fd)
			_ = unix.Close(fd)
		})
		return terr
	}, nil
}

// realThaw is Config.Thaw: FITHAW, with EINVAL (not frozen) as success.
func realThaw(path string) error {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("opening %s: %w", path, err)
	}
	defer func() { _ = unix.Close(fd) }()
	if err := ext4.Thaw(fd); err != nil && !errors.Is(err, unix.EINVAL) {
		return err
	}
	return nil
}
