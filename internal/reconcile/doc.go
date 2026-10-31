// Package reconcile is the node agent's converge loop.
//
// The CSI node handlers do not mount anything. They write what should be true
// on this node into the intent store (Stage, Publish, Unpublish, Unstage) and
// wait for the loop to make it so. One goroutine, Run, reads the intents and
// the kernel's mount table and drives the second toward the first, one
// idempotent step per layer: the transport mounted on the host, the item
// (for a directory, the volume's directory bound at the stage path), and a
// bind of the stage path at each publish target. It does this once at start,
// on every request and on a timer, so a plugin restart changes nothing the
// kernel reports, a reboot is repaired on the first pass, and a mount that
// someone removed is restored.
//
// # The kernel is the truth
//
// Nothing about mounts is cached. Every decision to mount, adopt, refuse or
// unmount is made from a mount table read at that moment. A mount that is
// already right is adopted without a mount call; one that is something else
// is a conflict (ErrTransportConflict, ErrStageConflict, ErrPublishConflict)
// and is left exactly as it is, never stacked over and never removed.
//
// # What can lose data or wedge a node, and what is done about it
//
// Unmounts are without MNT_DETACH, always: a busy mount is an error that the
// next pass retries, never a lazy detach that leaves a pod writing into a
// mount nobody tracks. What counts as garbage is narrow and is defined in
// garbage.go: mounts strictly below the staging root that no volume's stage
// path covers, publish targets the reconciler was asked or has reason to
// drop, and (only if a timeout is configured) transports nothing references.
// A pass whose intent store could not be read collects nothing. Every stat and
// observation of a path that may sit on a dead server runs under a deadline in
// its own goroutine that is abandoned, never joined (internal/deadline), and a
// transport that times out is not tried again by the other volumes on it in
// the same pass, so one dead server costs one deadline per pass, not one per
// volume. A step that times out wraps mount.ErrTimeout; a path whose last
// call has not returned fails at once wrapping mount.ErrBusy; neither stacks
// calls, and both are retried by the next pass.
//
// # Requests
//
// Stage, Unstage, Publish and Unpublish are idempotent and serialised per
// volume. Each writes the intent before it waits, so a caller whose context
// ends has still recorded what it wanted, and the retry finds it done.
// Different volumes do not block each other at the request layer; the loop is
// sequential, so passes are too. The errors are typed sentinels (errors.go)
// for the CSI layer to map to status codes; this package imports no gRPC.
//
// # Seams
//
// Metrics and Events are interfaces with no-op defaults; the CSI layer wires
// Prometheus and the Kubernetes event recorder. Log lines go through log/slog
// and never contain a mount data string or an option value.
package reconcile
