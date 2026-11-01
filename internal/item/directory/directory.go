// Package directory is the item kind for a plain directory of a transport,
// bound into pods.
//
// A directory volume lives at <transport mount>/<subdir>/<name>
// (volume.Context.Dir). Converge checks that it exists and is a directory and
// that nothing is mounted at the stage path, and mounts nothing: the stage
// path is a directory kubelet owns and stays an empty one. The publish
// targets are binds of the item directory (PublishSource).
//
// The reason is the NFS client, which reports "/" as the root of every mount
// of an export, a bind of a subdirectory included. Before kubelet unstages a
// volume it lists every other mount with the same device and root as the stage
// path and refuses while one exists. A bind of the item directory at the stage
// path would share its device and root with the transport and every other
// volume's bind, so the stage could never be unstaged. With no mount at the
// stage path kubelet finds no reference and unstages.
//
// The node never creates the directory for an nfs3 transport: the controller
// does, and a missing one is item.ErrItemMissing, which the CSI layer reports
// as NotFound. For a local transport the controller runs on another node and
// cannot reach the disk, so the node creates the directory at first stage when
// it is missing (decision 23; the intent's EnsureItem says it is the first
// stage, and a later pass that finds the directory missing reports
// item.ErrItemMissing like nfs3), with mode 0755, relative to descriptors and
// without following a symlink or crossing a mount.
//
// A bind of the volume's own item directory at the stage path is the one thing
// the driver recognises there: an earlier build made it. Converge and Teardown
// unmount it once, without MNT_DETACH, so a node upgraded in place does not
// keep it. Anything else mounted there is not this volume's and is
// item.ErrStageConflict, from Converge and from Teardown alike: it is left
// exactly as it is, never stacked over and never removed.
//
// Nothing is cached. Whether the stage path is clear is decided from the mount
// table the reconciler hands in.
package directory

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"syscall"
	"time"

	"github.com/greghaskins/csi-miami/internal/deadline"
	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/item"
	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/observe"
	"github.com/greghaskins/csi-miami/internal/transport"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// DefaultTimeout bounds the stat of the item directory when Config.Timeout is
// not positive.
const DefaultTimeout = 10 * time.Second

// Config is what an Item needs from its environment. Stat and Ensure exist for
// tests; nil means the os package.
type Config struct {
	// Root is the transports root: the directory under which transports are
	// mounted at <Root>/transports/<id>. It must be absolute and cleaned.
	Root string

	// Mounter is required, as for every item kind; the directory kind makes
	// no mount call.
	Mounter mount.Mounter

	// Timeout bounds the stat and the creation of the item directory, each of
	// which can hang on a dead mount. Zero or negative means DefaultTimeout.
	Timeout time.Duration

	Stat func(path string) (fs.FileInfo, error)

	// Ensure makes the item directory dir below the transport mount mountDir,
	// with its parents, for a local transport. Nil means a safe creation with
	// openat2(2) that follows no symlink and crosses no mount.
	Ensure func(mountDir, dir string) error
}

// Item is the directory item kind. It is safe for concurrent use.
type Item struct {
	cfg  Config
	busy deadline.Set // stat and ensure calls that have not returned, by path
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
	if cfg.Ensure == nil {
		cfg.Ensure = ensureBeneath
	}
	return &Item{cfg: cfg}, nil
}

// Kind is volume.KindDirectory.
func (d *Item) Kind() volume.Kind { return volume.KindDirectory }

// PublishSource is the item directory in the transport (volume.Context.Dir):
// publish targets are binds of it.
func (d *Item) PublishSource(in intent.Intent) (string, error) {
	id, err := in.ID()
	if err != nil {
		return "", fmt.Errorf("directory item: %w", err)
	}
	dir, err := in.Context().Dir(d.cfg.Root, id)
	if err != nil {
		return "", fmt.Errorf("directory item %s: %w", in.VolumeID, err)
	}
	return dir, nil
}

// Converge clears the stage path, checks the item directory, and mounts
// nothing:
//
//  1. Nothing is mounted at the stage path. A visible mount there that is a
//     bind of the volume's item directory was made by an earlier build and is
//     unmounted (clearStage). Any other visible mount is item.ErrStageConflict
//     and is left exactly as it is: the driver makes no mount at a directory
//     volume's stage path now, so whatever else is there is not its own, and
//     kubelet's reference check would see it. A mount hidden under a
//     parent's mount is not looked at: kubelet resolves through the covering
//     mount and never sees it, and the driver mounts nothing there.
//
//  2. The item directory exists and is a directory; for a local transport a
//     missing one is created first if in.EnsureItem is set (see ensure). The
//     stat runs under the Config.Timeout deadline in its own goroutine, because
//     a stat on a dead NFS mount never returns; a timeout wraps
//     mount.ErrTimeout, and while the abandoned stat has not returned the next
//     call fails at once with mount.ErrBusy instead of starting a second one.
//     Missing is item.ErrItemMissing, a file is item.ErrItemNotDirectory.
func (d *Item) Converge(ctx context.Context, in intent.Intent, obs item.Observed) error {
	dir, err := d.PublishSource(in)
	if err != nil {
		return err
	}
	if err := d.clearStage(ctx, in, dir, obs.Mounts); err != nil {
		return err
	}

	err = d.checkDir(ctx, in.VolumeID, dir)
	// Only at first stage (in.EnsureItem, which the node sets when it first
	// stages a local volume and the reconciler clears once the item has
	// converged): a directory that is missing later is a disk that is not
	// mounted, a reboot or a removal, and a new empty directory would put a pod
	// on an empty volume.
	if errors.Is(err, item.ErrItemMissing) && in.EnsureItem && in.Transport.Kind == transport.KindLocal {
		if err = d.ensure(ctx, in, dir); err == nil {
			err = d.checkDir(ctx, in.VolumeID, dir)
		}
	}
	return err
}

// clearStage is step 1 of Converge and all of Teardown. A visible mount at the
// stage path that is a bind of the volume's item directory, deleted or not
// (observe.Table.IsBindOfIncludingDeleted), is the driver's own from before
// directory items stopped mounting there, and is unmounted by item.UnmountBind:
// once, without MNT_DETACH, and never a stack. Any other visible mount is
// item.ErrStageConflict.
func (d *Item) clearStage(ctx context.Context, in intent.Intent, dir string, t observe.Table) error {
	stage := in.StagePath
	top, ok := t.At(stage)
	if !ok {
		return nil
	}
	ours, err := t.IsBindOfIncludingDeleted(stage, dir)
	if err != nil {
		return fmt.Errorf("directory item %s: %w", in.VolumeID, err)
	}
	if !ours {
		return fmt.Errorf("directory item %s: %w: %s is mounted as %s from %q; the driver mounts nothing at a directory volume's stage path",
			in.VolumeID, item.ErrStageConflict, stage, top.FSType, top.Source)
	}
	if err := item.UnmountBind(ctx, d.cfg.Mounter, stage, dir, item.ErrStageConflict); err != nil {
		return fmt.Errorf("directory item %s: %w", in.VolumeID, err)
	}
	return nil
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
	case errors.Is(err, syscall.ENOTDIR):
		// An element of the path is a file: the item path is not a directory.
		return fmt.Errorf("directory item %s: %w: %s", volumeID, item.ErrItemNotDirectory, dir)
	case err != nil:
		return fmt.Errorf("directory item %s: %w", volumeID, err)
	case !fi.IsDir():
		return fmt.Errorf("directory item %s: %w: %s", volumeID, item.ErrItemNotDirectory, dir)
	}
	return nil
}

// ensure creates the item directory of a local volume, under the same
// deadline and busy rules as the stat: the controller runs on another node, so
// the directory is created here at first stage (decision 23).
func (d *Item) ensure(ctx context.Context, in intent.Intent, dir string) error {
	mountDir, err := in.Transport.MountPoint(d.cfg.Root)
	if err != nil {
		return fmt.Errorf("directory item %s: %w", in.VolumeID, err)
	}
	what := fmt.Sprintf("creating the item directory %q", dir)
	if _, err := deadline.Run(ctx, d.cfg.Timeout, what, &d.busy, "ensure\x00"+dir, func(context.Context) (struct{}, error) {
		return struct{}{}, d.cfg.Ensure(mountDir, dir)
	}); err != nil {
		return fmt.Errorf("directory item %s: %w", in.VolumeID, err)
	}
	return nil
}

// Teardown takes down what an earlier build made at the stage path and nothing
// else: a bind of the volume's item directory there is unmounted (clearStage),
// once and without MNT_DETACH, so a busy one is an error for the caller to
// retry, and it is recognised even if the directory has since been removed. In a
// current build there is nothing to undo. Any other mount visible at the stage
// path is item.ErrStageConflict and is left, as Converge leaves it: removing
// what the driver cannot show it made is not the driver's call. The cost is
// that an Unstage over such a mount fails until a person clears it, which is
// the loud outcome; the alternative is unmounting something that holds data. A
// mount hidden under a parent's is not looked at.
//
// It does not stat the item directory. Recognising an old bind needs the
// transport in the mount table; the reconciler checks for it first.
func (d *Item) Teardown(ctx context.Context, in intent.Intent, obs item.Observed) error {
	dir, err := d.PublishSource(in)
	if err != nil {
		return err
	}
	return d.clearStage(ctx, in, dir, obs.Mounts)
}
