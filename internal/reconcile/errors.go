package reconcile

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"syscall"

	"github.com/greghaskins/csi-miami/internal/item"
	"github.com/greghaskins/csi-miami/internal/mount"
)

// The errors below are what the CSI layer maps to status codes; none of them
// carries a gRPC code itself, and this package imports no gRPC. Every one is
// wrapped with the volume and the path it concerns, so match them with
// errors.Is, or better, call Classify, which applies the one precedence order
// below and so gives every error exactly one Class. An error can match more
// than one sentinel: a conflict also matches mount.ErrConflict, a hung
// transport matches both ErrTransportUnavailable and mount.ErrTimeout. The
// order resolves it; no error is built to carry two sentinels that the order
// would not resolve (an unstage that cannot unmount stops at its first
// failure, and does not join a conflict with a timeout).
//
// # The mapping
//
// Classify tests, in this order, and the first match wins. T7 maps a Class to
// a gRPC code one to one.
//
//	 1. context.Canceled                          ClassCanceled          Canceled
//	    context.DeadlineExceeded that is not a    ClassDeadlineExceeded  DeadlineExceeded
//	      mount.ErrTimeout (the caller's own)
//	 2. ErrTargetOutsideScope,                    ClassInvalidArgument   InvalidArgument
//	    ErrStagePathOutsideRoot,
//	    ErrInvalidRequest
//	 3. ErrItemMissing                            ClassNotFound          NotFound
//	 4. ErrUnknownVolume                          ClassNotFound          NotFound
//	 5. ErrStageConflict, ErrPublishConflict,     ClassFailedPrecondition FailedPrecondition
//	    ErrTransportConflict, ErrItemNotDirectory,
//	    mount.ErrConflict, mount.ErrNotResolved,
//	    errTargetGone (a publish whose target's
//	    parent directory is gone, seen only as the
//	    answer to a Publish of that target)
//	 6. ErrSuperseded                             ClassAborted           Aborted
//	 7. mount.ErrTimeout                          ClassDeadlineExceeded  DeadlineExceeded
//	 8. ErrIntentsUnknown, ErrTransportUnavailable, ClassUnavailable     Unavailable
//	    ErrObserve, mount.ErrBusy, ErrStopped, EBUSY
//	 9. anything else                             ClassInternal          Internal
//
// Consequences worth knowing. A mount.ErrTimeout wraps context.DeadlineExceeded
// too, but it is the mount's deadline and not the caller's, so it is tested at
// 7 and not 1: a conflict joined with a timeout is a FailedPrecondition, and
// an unavailable transport that timed out is a DeadlineExceeded, not an
// Unavailable. A transport that does not answer and then reports "busy" on the
// passes after is Unavailable, which is the retry-later answer it is. EBUSY,
// from an unmount of a mount something still has open, is retryable and so is
// Unavailable. Errors from the Mounter that are none of these, such as an
// unexpected errno, are Internal.

// Class is the kind of an error of this package, in the terms the CSI layer
// maps to status codes. See the mapping above.
type Class int

// The classes, in the order Classify tests them (after ClassOK and the
// catch-all ClassInternal, which is the zero value of an unrecognised error).
const (
	// ClassInternal is an error this package does not recognise.
	ClassInternal Class = iota
	// ClassOK is no error.
	ClassOK
	// ClassCanceled is a caller whose context was cancelled.
	ClassCanceled
	// ClassDeadlineExceeded is a deadline: the caller's, or a mount's.
	ClassDeadlineExceeded
	// ClassInvalidArgument is a request that can never succeed as made.
	ClassInvalidArgument
	// ClassNotFound is a volume or an item that does not exist on this node.
	ClassNotFound
	// ClassFailedPrecondition is a state of the node that a person has to
	// change: something else is mounted where the driver needs to mount.
	ClassFailedPrecondition
	// ClassAborted is a request overtaken by a later one for the same volume.
	ClassAborted
	// ClassUnavailable is a failure that a retry can be expected to cure.
	ClassUnavailable
)

func (c Class) String() string {
	names := [...]string{"Internal", "OK", "Canceled", "DeadlineExceeded", "InvalidArgument", "NotFound", "FailedPrecondition", "Aborted", "Unavailable"}
	if c < 0 || int(c) >= len(names) {
		return "Class(" + strconv.Itoa(int(c)) + ")"
	}
	return names[c]
}

// Classify returns the Class of err by the precedence order in the mapping
// above. nil is ClassOK.
func Classify(err error) Class {
	switch {
	case err == nil:
		return ClassOK
	case errors.Is(err, context.Canceled):
		return ClassCanceled
	case errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, mount.ErrTimeout):
		return ClassDeadlineExceeded
	case errors.Is(err, ErrTargetOutsideScope), errors.Is(err, ErrStagePathOutsideRoot), errors.Is(err, ErrInvalidRequest):
		return ClassInvalidArgument
	case errors.Is(err, ErrItemMissing), errors.Is(err, ErrUnknownVolume):
		return ClassNotFound
	case errors.Is(err, ErrStageConflict), errors.Is(err, ErrPublishConflict), errors.Is(err, ErrTransportConflict),
		errors.Is(err, ErrItemNotDirectory), errors.Is(err, mount.ErrConflict), errors.Is(err, mount.ErrNotResolved),
		errors.Is(err, errTargetGone):
		return ClassFailedPrecondition
	case errors.Is(err, ErrSuperseded):
		return ClassAborted
	case errors.Is(err, mount.ErrTimeout):
		return ClassDeadlineExceeded
	case errors.Is(err, ErrIntentsUnknown), errors.Is(err, ErrTransportUnavailable), errors.Is(err, ErrObserve), errors.Is(err, mount.ErrBusy),
		errors.Is(err, ErrStopped), errors.Is(err, syscall.EBUSY):
		return ClassUnavailable
	default:
		return ClassInternal
	}
}

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
	// its transport and the transport is nfs3: the controller creates those, the
	// node does not. (For a local transport the node creates the directory at
	// first stage.) It wraps fs.ErrNotExist.
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

	// ErrTargetOutsideScope is returned by Publish and Unpublish for a target
	// path the driver must not mount on or unmount: one that is not absolute
	// and cleaned, or is, is inside, or contains the transports root, the
	// staging root or a volume's stage path. It is the caller's mistake, and
	// InvalidArgument.
	ErrTargetOutsideScope = errors.New("the target path is not one the driver may publish to")

	// ErrSuperseded is returned to a request that a later request for the same
	// volume overtook: an Unstage that was still waiting when a Stage or a
	// Publish arrived, a Stage or Publish that a later Unstage took down, or an
	// Unstage whose record changed while it ran. The record is kept in the
	// last case. The caller of a superseded request is usually gone; one that
	// is not retries.
	ErrSuperseded = errors.New("the request was superseded by a later one for the same volume")

	// ErrInvalidRequest is returned by Stage for an intent that fails the
	// boundary rules of the volume and intent packages (a malformed volume ID,
	// a transport not in normal form, a record that is not structurally
	// sound). It wraps the error that says which.
	ErrInvalidRequest = errors.New("the request is not valid")

	// ErrObserve is returned when the mount table cannot be read (it is
	// item.ErrObserve). It wraps the read error. Retrying is the remedy.
	ErrObserve = item.ErrObserve

	// ErrStopped is returned to requests that were waiting when Run returned,
	// and to requests made after it did. The intent they wrote is kept, and
	// the next process converges it.
	ErrStopped = errors.New("the reconciler is not running")
)
