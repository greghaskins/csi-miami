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

// Item makes one kind of volume real between its transport and its stage
// path.
//
// The reconciler owns the layers on either side: it has the transport
// mounted before it calls Converge, and it makes the publish binds from the
// stage path after Converge succeeds. An Item owns what lies between: that
// the volume's thing exists inside the transport directory, and that the
// stage path is a mount of it. For a directory that is a stat and one bind;
// an image item would attach a loop device and mount a file system.
type Item interface {
	// Kind is the volume kind the Item handles.
	Kind() volume.Kind

	// Converge takes one idempotent step toward the stage path being a mount
	// of the volume, and returns nil when it is. It changes nothing that is
	// already right, and it never stacks a mount over another or removes a
	// mount it did not make: something else at the stage path is
	// ErrStageConflict. Every call that can hang runs under a deadline, and a
	// timeout wraps mount.ErrTimeout.
	Converge(ctx context.Context, in intent.Intent, obs Observed) error

	// Teardown undoes Converge: afterwards nothing is mounted at the stage
	// path, and what the Item made below it is gone. It is a no-op when there
	// is nothing to undo, and it never detaches lazily, so a busy stage is an
	// error for the caller to retry. It does not touch the transport and does
	// not depend on it being reachable.
	Teardown(ctx context.Context, in intent.Intent, obs Observed) error
}

var (
	// ErrItemMissing is returned when the volume's directory (or file) is not
	// in the transport. The node never creates it: the controller does. It
	// wraps fs.ErrNotExist.
	ErrItemMissing = fmt.Errorf("the volume's item does not exist in its transport: %w", fs.ErrNotExist)

	// ErrItemNotDirectory is returned when a directory item's path exists and
	// is something else.
	ErrItemNotDirectory = errors.New("the volume's item path in its transport is not a directory")

	// ErrStageConflict is returned when the stage path holds a mount that is
	// not this volume's. It wraps mount.ErrConflict. Nothing is changed.
	ErrStageConflict = fmt.Errorf("the stage path is mounted from something else: %w", mount.ErrConflict)
)

// UnmountBind unmounts the mount visible at mountPoint, once, and only if it
// is a bind of the directory source (observe.Table.IsBindOf). It is how an
// Item or the reconciler takes down what it made without ever removing what
// it did not:
//
//   - Nothing visible at mountPoint, or a mount hidden under another, is
//     success: there is nothing of ours to remove.
//   - A visible mount that is not a bind of source is not ours. It is left in
//     place and the error wraps conflict (ErrStageConflict for a stage path,
//     the reconciler's ErrPublishConflict for a publish target).
//   - The unmount is without MNT_DETACH, so a busy mount is an error and stays.
//   - After it, the table is read again. If something is still visible at
//     mountPoint, another mount was stacked there; it is not removed and the
//     error wraps conflict. The driver never stacks, so a loop that popped
//     layers until the path was clear would be removing mounts it cannot
//     show it made.
func UnmountBind(ctx context.Context, m mount.Mounter, mountPoint, source string, conflict error) error {
	t, err := m.Mounts(ctx)
	if err != nil {
		return err
	}
	top, ok := t.At(mountPoint)
	if !ok {
		return nil
	}
	same, err := t.IsBindOf(mountPoint, source)
	if err != nil {
		return fmt.Errorf("%s: %w", mountPoint, err)
	}
	if !same {
		return fmt.Errorf("%w: %s is mounted as %s from %q, which is not a bind of %s, and is left in place", conflict, mountPoint, top.FSType, top.Source, source)
	}
	if err := m.Unmount(ctx, mountPoint, 0); err != nil {
		return err
	}
	t, err = m.Mounts(ctx)
	if err != nil {
		return err
	}
	if _, ok := t.At(mountPoint); ok {
		return fmt.Errorf("%w: another mount is stacked at %s and is left in place", conflict, mountPoint)
	}
	return nil
}
