// Package csi serves the Container Storage Interface over a unix socket: the
// identity service, the controller service, and the plumbing that runs them.
//
// The controller is stateless. It holds no record of the volumes it made; the
// directory on the transport is the volume, and the PersistentVolume that
// Kubernetes keeps is the record of where it is. CreateVolume mounts the
// StorageClass's transport under the controller's own root and makes the
// volume's directory there. DeleteVolume receives only the volume ID, so it
// reads the PV to learn which transport and subdirectory the volume lives in,
// and removes the directory. Publish and unpublish accept everything and
// record nothing; they exist so that a later milestone needs no capability
// change. ListVolumes walks the transports the controller currently has
// mounted, and is best-effort.
//
// Deleting is the one destructive operation, so it is built to do nothing
// when it is in doubt: every condition under which it removes a directory is
// listed in the doc comment of removeItemDir, each with its reason, and every
// failure to establish one of them is Unavailable, which the caller retries,
// never success. Create is idempotent: an existing directory is adopted.
//
// Error codes follow the implementation plan: InvalidArgument for a request
// that is wrong whatever the state of the world, NotFound for a volume this
// driver could not have issued, AlreadyExists for a name used with other
// parameters or a path taken by something that is not a directory, Aborted
// for an operation on the same volume already in flight, Unavailable for a
// transport that cannot be mounted, a PV that cannot be read or a directory
// that cannot be proved safe to remove, and DeadlineExceeded or Canceled when
// the request's own context ends. A panic in a handler becomes Internal and
// is logged.
//
// Server wires the services to a unix socket with a logging interceptor that
// records the method, the code and the duration and, of a request, only the
// volume ID, and with a seam for further interceptors such as metrics.
package csi
