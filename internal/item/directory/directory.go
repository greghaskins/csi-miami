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
// Anything mounted at the stage path is not this volume's and is
// item.ErrStageConflict, from Converge and from Teardown alike: it is left
// exactly as it is, never stacked over and never removed. Teardown has nothing
// to unmount and removes nothing.
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

// Converge checks the item directory and that the stage path is clear, and
// makes nothing:
//
//  1. Nothing is mounted at the stage path. A visible mount there, or a mount
//     hidden under a parent's mount, is item.ErrStageConflict and is left
//     exactly as it is: the driver makes no mount at a directory volume's
//     stage path, so whatever is there is not its own, and kubelet's reference
//     check would see it too.
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
	if err := stageClear(in, obs.Mounts, true); err != nil {
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

// stageClear is item.ErrStageConflict if anything is mounted at the stage
// path: a visible mount, or, when hiddenToo, one that a parent's mount covers.
func stageClear(in intent.Intent, t observe.Table, hiddenToo bool) error {
	stage := in.StagePath
	if top, ok := t.At(stage); ok {
		return fmt.Errorf("directory item %s: %w: %s is mounted as %s from %q; the driver mounts nothing at a directory volume's stage path",
			in.VolumeID, item.ErrStageConflict, stage, top.FSType, top.Source)
	}
	if hiddenToo && hidden(t, stage) {
		return fmt.Errorf("directory item %s: %w: %s has a mount that is hidden under another mount; clear it before staging", in.VolumeID, item.ErrStageConflict, stage)
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

// Teardown has nothing to unmount and removes nothing: Converge made nothing
// at the stage path. It returns item.ErrStageConflict if a mount is visible
// there, and leaves it, as Converge does: the driver never puts one there, so
// it is someone else's, and removing what the driver cannot show it made is not
// the driver's call. The cost is that an Unstage over such a mount fails until
// a person clears it, which is the loud outcome; the alternative is unmounting
// something that holds data. A mount hidden under a parent's is not reported
// here, so a stale one cannot make a volume impossible to unstage.
//
// It does not stat the item directory and does not depend on the transport
// being mounted or reachable.
func (d *Item) Teardown(_ context.Context, in intent.Intent, obs item.Observed) error {
	if _, err := d.PublishSource(in); err != nil {
		return err
	}
	return stageClear(in, obs.Mounts, false)
}
