package item

import (
	"context"
	"errors"
	"fmt"
	"io/fs"

	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/observe"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// Observed is what the kernel says, read by the reconciler just before it
// calls an Item. It is a snapshot: an Item that changes a mount must not
// assume the table still describes it afterwards, and reads the Mounter again
// (the Mounter decides from a fresh table itself) when it needs to know.
type Observed struct {
	// Mounts is /proc/self/mountinfo, in full.
	Mounts observe.Table
}

// Item makes one kind of volume real between its transport and its publish
// targets.
//
// The reconciler owns the layers on either side: it has the transport mounted
// before it calls Converge, and it makes the publish binds after Converge
// succeeds, each a bind of the directory PublishSource returns. An Item owns
// what lies between: that the volume's thing exists inside the transport and,
// if the kind needs one, a mount at the stage path. A directory needs none: the
// NFS client reports "/" as the root of every mount of an export, so a bind of
// a subdirectory at the stage path would look to kubelet's reference check
// like a mount of the transport itself, and the stage could never be
// unstaged. A directory item therefore makes nothing at the stage path, and
// its publishes are binds of the directory in the transport. An image item
// (M2) attaches a loop device and mounts a file system at the stage path, and
// its publishes are binds of that.
type Item interface {
	// Kind is the volume kind the Item handles.
	Kind() volume.Kind

	// Converge takes one idempotent step toward the volume's thing existing
	// in its transport and, if the kind needs one, being mounted at the stage
	// path, and returns nil when it is. It changes nothing that is already
	// right, and it never stacks a mount over another or removes a mount it
	// did not make: a mount at the stage path of a kind that makes none, or a
	// foreign one at the stage path of a kind that does, is ErrStageConflict.
	// Every call that can hang runs under a deadline, and a timeout wraps
	// mount.ErrTimeout.
	Converge(ctx context.Context, in intent.Intent, obs Observed) error

	// PublishSource is the directory each publish target is a bind of: for a
	// directory item the volume's directory in the transport, for an image
	// item its stage path. It decides from the intent alone and touches
	// nothing.
	PublishSource(in intent.Intent) (string, error)

	// Teardown undoes Converge: afterwards nothing the Item made is mounted at
	// the stage path, and what the Item made below it is gone. It is a no-op
	// when there is nothing to undo, and it never detaches lazily, so a busy
	// stage is an error for the caller to retry. It does not touch the
	// transport and does not depend on it being reachable.
	Teardown(ctx context.Context, in intent.Intent, obs Observed) error
}

var (
	// ErrItemMissing is returned when the volume's directory (or file) is not
	// in the transport and the node does not create it: the controller does
	// for nfs3. For a local transport the node creates the directory at first
	// stage (decision 23). It wraps fs.ErrNotExist.
	ErrItemMissing = fmt.Errorf("the volume's item does not exist in its transport: %w", fs.ErrNotExist)

	// ErrItemNotDirectory is returned when a directory item's path exists and
	// is something else.
	ErrItemNotDirectory = errors.New("the volume's item path in its transport is not a directory")

	// ErrStageConflict is returned when the stage path holds a mount that is
	// not this volume's: for a directory volume, any mount at all, since the
	// driver makes none there, and kubelet's reference check would see it.
	// It wraps mount.ErrConflict. Nothing is changed.
	ErrStageConflict = fmt.Errorf("the stage path is mounted from something else: %w", mount.ErrConflict)
)

// ErrNoOpenat2 is returned when the kernel has no openat2(2) (before Linux
// 5.6), which the creation of an item directory needs to be sure it follows no
// symlink and crosses no mount. There is no fallback to a path-based creation.
var ErrNoOpenat2 = errors.New("the node needs openat2(2), which this kernel does not have (Linux 5.6 or newer is required), so nothing is changed")

// ErrObserve is returned when the mount table cannot be read. It is the one
// error for that, wherever it is found, so that it has one class however the
// request that met it came by it. Retrying is the remedy.
var ErrObserve = errors.New("the mount table cannot be read")

// Owned says whether the visible mount top at a mount point is one the driver
// made. It decides from the table alone.
type Owned func(t observe.Table, top observe.Mount) (bool, error)

// BindOf is the Owned test for a mount that must be a bind of the directory
// source: the mount is a bind of that directory as the kernel reports it
// (the same device and the same root path, or, on NFS, which reports "/" as
// the root of every mount of an export, the same device and the directory in
// the mount source), including when the directory has since been removed
// (observe.Table.IsBindOfIncludingDeleted). A volume whose
// directory was deleted while it was staged must still be unstageable. For a
// removed directory this is "the same last-known root path", not a
// proof of identity: renaming another directory to the volume's name before
// the removal would pass for it. The mount table has nothing better to offer,
// and the cost of the mistake is unmounting a bind of a directory in the
// driver's own transport.
func BindOf(source string) Owned {
	return func(t observe.Table, top observe.Mount) (bool, error) {
		return t.IsBindOfIncludingDeleted(top.MountPoint, source)
	}
}

// UnmountBind unmounts the mount visible at mountPoint, once, and only if it
// is a bind of the directory source (BindOf). See UnmountIf.
func UnmountBind(ctx context.Context, m mount.Mounter, mountPoint, source string, conflict error) error {
	return UnmountIf(ctx, m, mountPoint, BindOf(source), "a bind of "+source, conflict)
}

// UnmountIf is how an Item or the reconciler takes down what it made without
// ever removing what it did not:
//
//   - Nothing visible at mountPoint, or a mount hidden under another, is
//     success: there is nothing of ours to remove.
//   - A visible mount for which owned says false is not ours. It is left in
//     place and the error wraps conflict (ErrStageConflict for a stage path,
//     the reconciler's ErrPublishConflict for a publish target). what says
//     what it should have been, for the message.
//   - The unmount is once and without MNT_DETACH, so a busy mount is an error
//     and stays.
//
// The caller chooses the test. The reconciler uses two, for publish targets.
// The usual one is BindOf the volume's directory in its transport: used for
// unstage, for an Unpublish of a volume that has an intent, and for every
// drop it derives itself, all of which know the volume. The second is wider,
// a bind of any directory in one of the driver's transports (same device, a
// root inside the transport's), and is used only for an explicit Unpublish of
// a volume with no intent, where kubelet named the path and nothing says which
// volume's directory it should be a bind of. A stage path uses BindOf the
// volume's directory.
//   - After it, the table is read again. If something is still visible at
//     mountPoint, another mount was stacked there; it is not removed and the
//     error wraps conflict. The driver never stacks, so popping layers until
//     the path is clear would remove mounts it cannot show it made.
func UnmountIf(ctx context.Context, m mount.Mounter, mountPoint string, owned Owned, what string, conflict error) error {
	t, err := m.Mounts(ctx)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrObserve, err)
	}
	top, ok := t.At(mountPoint)
	if !ok {
		return nil
	}
	ours, err := owned(t, top)
	if err != nil {
		return fmt.Errorf("%s: %w", mountPoint, err)
	}
	if !ours {
		return fmt.Errorf("%w: %s is mounted as %s from %q, which is not %s, and is left in place", conflict, mountPoint, top.FSType, top.Source, what)
	}
	if err := m.Unmount(ctx, mountPoint, 0); err != nil {
		return err
	}
	t, err = m.Mounts(ctx)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrObserve, err)
	}
	if _, ok := t.At(mountPoint); ok {
		return fmt.Errorf("%w: another mount is stacked at %s and is left in place", conflict, mountPoint)
	}
	return nil
}
