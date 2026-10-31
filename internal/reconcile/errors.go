package reconcile

import (
	"errors"
	"fmt"

	"github.com/greghaskins/csi-miami/internal/item"
	"github.com/greghaskins/csi-miami/internal/mount"
)

// The errors below are what the CSI layer maps to status codes; none of them
// carries a gRPC code itself. Every one is wrapped with the volume and the
// path it concerns, so match them with errors.Is. An error can match more
// than one: a conflict also matches mount.ErrConflict, a hung transport
// matches both ErrTransportUnavailable and mount.ErrTimeout.
//
// Suggested mapping: ErrUnknownVolume to NotFound or FailedPrecondition (the
// CSI call decides); ErrItemMissing to NotFound; ErrTransportConflict,
// ErrStageConflict, ErrPublishConflict, ErrItemNotDirectory and
// ErrStagePathOutsideRoot to FailedPrecondition or InvalidArgument as the
// call warrants; mount.ErrTimeout to DeadlineExceeded; mount.ErrBusy,
// ErrTransportUnavailable and ErrIntentsUnknown to Unavailable.
var (
	// ErrTransportConflict is returned when the transport's mount point holds
	// something other than this transport (a different file system, the same
	// export under a different option profile, or a mount hidden under a
	// parent's mount). It wraps mount.ErrConflict. Nothing is changed, and
	// nothing is stacked over it.
	ErrTransportConflict = fmt.Errorf("the transport's mount point holds something else: %w", mount.ErrConflict)

	// ErrTransportUnavailable is returned when the transport could not be
	// observed or mounted: the server did not answer, the name did not
	// resolve, the host path is gone, the mount table could not be read. The
	// error it wraps (often mount.ErrTimeout or mount.ErrBusy) says which.
	// It is retryable, and the next pass retries it.
	ErrTransportUnavailable = errors.New("the transport is unavailable")

	// ErrStageConflict is returned when the stage path holds a mount that is
	// not a bind of the volume's directory. It wraps mount.ErrConflict.
	// Nothing is changed.
	ErrStageConflict = item.ErrStageConflict

	// ErrPublishConflict is returned when a publish target holds a mount that
	// is not a bind of the volume's stage path. It wraps mount.ErrConflict.
	// Nothing is changed.
	ErrPublishConflict = fmt.Errorf("the publish target is mounted from something else: %w", mount.ErrConflict)

	// ErrItemMissing is returned when the volume's directory does not exist in
	// its transport; the node never creates it. It wraps fs.ErrNotExist.
	ErrItemMissing = item.ErrItemMissing

	// ErrItemNotDirectory is returned when the volume's path in its transport
	// exists and is not a directory.
	ErrItemNotDirectory = item.ErrItemNotDirectory

	// ErrUnknownVolume is returned by Publish for a volume that has no intent
	// on this node, that is, one that was never staged or was unstaged.
	ErrUnknownVolume = errors.New("the volume is not staged on this node")

	// ErrStagePathOutsideRoot is returned by Stage for a stage path that is not
	// strictly below Options.StageRoot. Garbage collection only looks below
	// that root, so a volume staged elsewhere could never be cleaned up.
	ErrStagePathOutsideRoot = errors.New("the stage path is not below the staging root")

	// ErrIntentsUnknown is returned when the intent store could not be read
	// during a pass: what should be true on the node is unknown, so nothing
	// is converged and nothing is collected. It is retryable.
	ErrIntentsUnknown = errors.New("the intents on this node cannot be read")

	// ErrStopped is returned to requests that were waiting when Run returned,
	// and to requests made after it did. The intent they wrote is kept, and
	// the next process converges it.
	ErrStopped = errors.New("the reconciler is not running")
)
