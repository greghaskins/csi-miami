package volume

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
	"unsafe"

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

// generate is the generation under the volume's freeze lock: one at a time,
// and never while an adopted mount is being thawed.
func (it *Item) generate(ctx context.Context, v vol, st *vstate, trigger string) error {
	st.fz.Lock()
	defer st.fz.Unlock()

	cctx, cancel := context.WithTimeout(ctx, it.cfg.Timeout)
	c, err := it.cfg.Dial(cctx, v.socket)
	cancel()
	if err != nil {
		return fmt.Errorf("volume item %s: %w: %w", v.in.VolumeID, item.ErrServerDown, err)
	}
	defer func() { _ = c.Close() }()

	sctx, scancel := context.WithTimeout(ctx, it.cfg.Timeout)
	status, err := c.Status(sctx)
	scancel()
	if err != nil {
		return fmt.Errorf("volume item %s: %w: %w", v.in.VolumeID, item.ErrServerDown, err)
	}
	if status.PublishInFlight {
		return fmt.Errorf("volume item %s: %w", v.in.VolumeID, ErrGenerationSkipped)
	}

	if err := it.freezeAndMark(ctx, v, c); err != nil {
		return err
	}

	reply, err := c.Publish(ctx, trigger)
	switch {
	case errors.Is(err, blockd.ErrRefused) || (err == nil && reply.Refused):
		return fmt.Errorf("volume item %s: %w", v.in.VolumeID, publishRefused())
	case err != nil:
		return fmt.Errorf("volume item %s: publishing the generation: %w", v.in.VolumeID, err)
	}
	return nil
}

// freezeAndMark freezes the stage mount, has the server mark the boundary and
// thaws, in a function of its own so the thaw is a defer: it runs when the
// boundary fails, when it panics, and, through the watchdog, when it hangs.
// A volume with no mount (an unstage that finds it gone) marks the boundary
// with nothing to freeze.
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
	watchdog := time.AfterFunc(it.cfg.FreezeBound, func() {
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

// blockSize is Config.DeviceSize: BLKGETSIZE64.
func blockSize(path string) (int64, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return 0, err
	}
	defer func() { _ = unix.Close(fd) }()
	var n uint64
	if _, _, e := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), unix.BLKGETSIZE64, uintptr(unsafe.Pointer(&n))); e != 0 { //nolint:gosec // the ioctl's out parameter
		return 0, e
	}
	return int64(n), nil //nolint:gosec // a device size fits int64
}
