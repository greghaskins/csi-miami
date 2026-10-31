// Package directory is the item kind for a plain directory of a transport,
// bound into pods.
//
// A directory volume lives at <transport mount>/<subdir>/<name>
// (volume.Context.Dir). Converge checks that it exists and is a directory,
// then makes the stage path a bind mount of it. The node never creates the
// directory: the controller does, and a missing one is item.ErrItemMissing,
// which the CSI layer reports as NotFound. Teardown unmounts the stage path.
//
// Nothing is cached. Whether the stage path is already right is decided from
// the mount table the reconciler hands in, so a restarted process adopts what
// its predecessor made without making a single mount call.
package directory

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"time"

	"github.com/greghaskins/csi-miami/internal/deadline"
	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/item"
	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/observe"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// DefaultTimeout bounds the stat of the item directory and the creation of
// the stage path when Config.Timeout is not positive.
const DefaultTimeout = 10 * time.Second

// stageMode is the mode of a stage directory this package creates. The
// staging tree is the driver's own, and only the driver, as root, uses it.
const stageMode = 0o700

// Config is what an Item needs from its environment. Stat and MkdirAll exist
// for tests; nil means the os package.
type Config struct {
	// Root is the transports root: the directory under which transports are
	// mounted at <Root>/transports/<id>. It must be absolute and cleaned.
	Root string

	// Mounter binds and unmounts.
	Mounter mount.Mounter

	// Timeout bounds the stat of the item directory and the creation of the
	// stage directory, each of which can hang on a dead mount. Zero or
	// negative means DefaultTimeout.
	Timeout time.Duration

	Stat     func(path string) (fs.FileInfo, error)
	MkdirAll func(path string, perm fs.FileMode) error
}

// Item is the directory item kind. It is safe for concurrent use.
type Item struct {
	cfg  Config
	busy deadline.Set // stat and mkdir calls that have not returned, by path
}

var _ item.Item = (*Item)(nil)

// New returns the directory Item. It fails if the Mounter is nil or Root is
// not an absolute, cleaned path.
func New(cfg Config) (*Item, error) {
	if cfg.Mounter == nil {
		return nil, errors.New("directory item: a Mounter is required")
	}
	if err := mount.CheckPath("directory item", "root", cfg.Root); err != nil {
		return nil, err
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultTimeout
	}
	if cfg.Stat == nil {
		cfg.Stat = os.Stat
	}
	if cfg.MkdirAll == nil {
		cfg.MkdirAll = os.MkdirAll
	}
	return &Item{cfg: cfg}, nil
}

// Kind is volume.KindDirectory.
func (d *Item) Kind() volume.Kind { return volume.KindDirectory }

// Converge checks the item directory and makes the stage path a bind of it,
// each step a no-op when it is already true:
//
//  1. The item directory exists and is a directory. The stat runs under the
//     Config.Timeout deadline in its own goroutine, because a stat on a dead
//     NFS mount never returns; a timeout wraps mount.ErrTimeout, and while
//     the abandoned stat has not returned the next call fails at once with
//     mount.ErrBusy instead of starting a second one. Missing is
//     item.ErrItemMissing, a file is item.ErrItemNotDirectory.
//
//  2. The stage path is a bind of the item directory. The decision is made
//     from obs: a visible mount that is a bind of the item directory is
//     adopted without a mount call (so a restart changes nothing, and a stage
//     whose transport is slow costs nothing), except that a read-only flag on
//     it is cleared, because stages are read-write and read-only is a
//     property of a publish. A visible mount that is anything else, or a
//     mount at the path hidden under a parent's mount, is item.ErrStageConflict
//     and is left exactly as it is: stacking over it would hide it and leak
//     it, and removing it is not ours to decide. When nothing is mounted
//     there, the stage directory is created (kubelet owns the staging tree
//     above it) and the item directory is bound on it.
func (d *Item) Converge(ctx context.Context, in intent.Intent, obs item.Observed) error {
	id, err := in.ID()
	if err != nil {
		return fmt.Errorf("directory item: %w", err)
	}
	dir, err := in.Context().Dir(d.cfg.Root, id)
	if err != nil {
		return fmt.Errorf("directory item %s: %w", in.VolumeID, err)
	}

	if err := d.checkDir(ctx, in.VolumeID, dir); err != nil {
		return err
	}

	stage := in.StagePath
	if top, ok := obs.Mounts.At(stage); ok {
		same, err := obs.Mounts.IsBindOf(stage, dir)
		if err != nil {
			return fmt.Errorf("directory item %s: %w", in.VolumeID, err)
		}
		if !same {
			return fmt.Errorf("directory item %s: %w: %s is mounted as %s from %q, which is not a bind of the volume's directory",
				in.VolumeID, item.ErrStageConflict, stage, top.FSType, top.Source)
		}
		if top.ReadOnly() {
			return d.bind(ctx, in.VolumeID, dir, stage)
		}
		return nil
	}
	if hidden(obs.Mounts, stage) {
		return fmt.Errorf("directory item %s: %w: %s has a mount that is hidden under another mount; clear it before staging", in.VolumeID, item.ErrStageConflict, stage)
	}

	what := fmt.Sprintf("creating the stage directory %q", stage)
	if _, err := deadline.Run(ctx, d.cfg.Timeout, what, &d.busy, "mkdir\x00"+stage, func(context.Context) (struct{}, error) {
		return struct{}{}, d.cfg.MkdirAll(stage, stageMode)
	}); err != nil {
		return fmt.Errorf("directory item %s: %w", in.VolumeID, err)
	}
	return d.bind(ctx, in.VolumeID, dir, stage)
}

// checkDir is step 1 of Converge.
func (d *Item) checkDir(ctx context.Context, volumeID, dir string) error {
	what := fmt.Sprintf("stat of the item directory %q", dir)
	fi, err := deadline.Run(ctx, d.cfg.Timeout, what, &d.busy, "stat\x00"+dir, func(context.Context) (fs.FileInfo, error) {
		return d.cfg.Stat(dir)
	})
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return fmt.Errorf("directory item %s: %w: %s", volumeID, item.ErrItemMissing, dir)
	case err != nil:
		return fmt.Errorf("directory item %s: %w", volumeID, err)
	case !fi.IsDir():
		return fmt.Errorf("directory item %s: %w: %s", volumeID, item.ErrItemNotDirectory, dir)
	}
	return nil
}

// bind binds dir on stage, read-write, turning the Mounter's conflict into
// item.ErrStageConflict.
func (d *Item) bind(ctx context.Context, volumeID, dir, stage string) error {
	err := d.cfg.Mounter.Bind(ctx, dir, stage, false)
	if errors.Is(err, mount.ErrConflict) && !errors.Is(err, item.ErrStageConflict) {
		err = fmt.Errorf("%w: %w", item.ErrStageConflict, err)
	}
	if err != nil {
		return fmt.Errorf("directory item %s: %w", volumeID, err)
	}
	return nil
}

// hidden reports whether the table has a mount at path that no visible mount
// leads to, because a mount of a parent directory covers it.
func hidden(t observe.Table, path string) bool {
	for _, m := range t {
		if m.MountPoint == path {
			return true
		}
	}
	return false
}

// Teardown unmounts the stage path, once and without MNT_DETACH, but only if
// the mount there is a bind of the volume's directory (item.UnmountBind).
// Anything else at the stage path is item.ErrStageConflict and is left where it
// is, as Converge leaves it: Converge never puts anything but that bind there,
// so a different mount is someone else's, or a stack on top of ours, and
// removing what the driver cannot show it made is not the driver's call. The
// cost is that an Unstage over such a mount fails until a person clears it,
// which is the loud outcome; the alternative is unmounting something that
// holds a pod's data.
//
// It does not stat the item directory, so it works when the item is gone, but
// it does need the transport in the mount table to recognise the bind, which
// the reconciler's converge restores before an Unstage can be retried.
func (d *Item) Teardown(ctx context.Context, in intent.Intent, _ item.Observed) error {
	id, err := in.ID()
	if err != nil {
		return fmt.Errorf("directory item: %w", err)
	}
	dir, err := in.Context().Dir(d.cfg.Root, id)
	if err != nil {
		return fmt.Errorf("directory item %s: %w", in.VolumeID, err)
	}
	if err := item.UnmountBind(ctx, d.cfg.Mounter, in.StagePath, dir, item.ErrStageConflict); err != nil {
		return fmt.Errorf("directory item %s: %w", in.VolumeID, err)
	}
	return nil
}
