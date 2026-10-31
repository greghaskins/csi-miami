// Package kube reads the Kubernetes objects the controller needs, and nothing
// else.
//
// The controller is stateless, so DeleteVolume, which receives only a volume
// ID, finds the volume's transport and subdirectory by reading the
// PersistentVolume that carries them. PVReader is that one read. Client
// implements it with direct GET requests through client-go: no informer, no
// cache, no watch, so there is nothing to go stale and no memory that grows
// with the cluster. Every request runs under a deadline of its own.
//
// A PV that does not exist is reported as an error wrapping ErrNotFound, never
// as a nil PV, so a caller cannot mistake "absent" for "present but empty". Any
// other error means the answer is unknown, and a caller that deletes data must
// treat it that way.
//
// Package fake provides a PVReader for other packages' tests.
package kube
