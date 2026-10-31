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

// maxLayers bounds how many stacked mounts UnmountVisible pops at one path.
// The driver never stacks, so more than a few means something else is
// stacking and the caller should see an error rather than loop on it.
const maxLayers = 8

// UnmountVisible unmounts whatever is visible at path, popping stacked mounts
// one layer at a time, and returns nil once nothing is visible there. It
// never detaches lazily (flags 0), so a busy mount is an error and the mount
// stays. A mount hidden under another mount of a parent directory is not
// visible and is left alone: no path leads to it.
//
// It reads the table afresh before each layer, because the mount table is the
// only truth.
func UnmountVisible(ctx context.Context, m mount.Mounter, path string) error {
	for range maxLayers {
		t, err := m.Mounts(ctx)
		if err != nil {
			return err
		}
		if _, ok := t.At(path); !ok {
			return nil
		}
		if err := m.Unmount(ctx, path, 0); err != nil {
			return err
		}
	}
	t, err := m.Mounts(ctx)
	if err != nil {
		return err
	}
	if _, ok := t.At(path); ok {
		return fmt.Errorf("%s is still mounted after %d layers were unmounted", path, maxLayers)
	}
	return nil
}
