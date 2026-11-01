// Package reconcile is the node agent's converge loop.
//
// The CSI node handlers do not mount anything. They write what should be true
// on this node into the intent store (Stage, Publish, Unpublish, Unstage) and
// wait for the loop to make it so. One goroutine, Run, reads the intents and
// the kernel's mount table and drives the second toward the first, one
// idempotent step per layer: the transport mounted on the host, the item
// (for a directory, that the volume's directory exists and nothing is mounted
// at the stage path), and a bind of the item's publish source (for a
// directory, the volume's directory in the transport) at each publish
// target. It does this once at start,
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
// A record that cannot be read makes its volume's intent unknown, not absent:
// a pass in which the store or any record could not be read collects nothing
// (an explicit Unpublish excepted), and a request that would have to read or
// rewrite that record fails as ErrIntentsUnknown and changes nothing. A record
// that breaks the request rules (a stage path outside the staging root, a
// publish target in the driver's own directories) is treated the same way,
// with one way out: a fresh Stage overwrites its stage path and keeps its
// publishes, so a bad stage path is repaired by the next NodeStage, while a
// bad publish target survives it and, since Unpublish refuses such a path, is
// for a person to repair or remove. The
// driver unmounts only what the mount table shows to be a bind of what it
// should be a bind of, so a foreign mount at a stage path or a publish target
// is reported and left. Every stat and
// observation of a path that may sit on a dead server runs under a deadline in
// its own goroutine that is abandoned, never joined (internal/deadline), and a
// transport that times out is not tried again by the other volumes on it in
// the same pass, so one dead server costs one deadline per pass, not one per
// volume. A step that times out wraps mount.ErrTimeout; a path whose last
// call has not returned fails at once wrapping mount.ErrBusy; neither stacks
// calls, and both are retried by the next pass.
//
// # A transport's health is reported, not remediated
//
// Each pass asks every transport the intents reference whether it answers: a
// statfs of its mount directory under the stat deadline (statfs always asks
// the server, where a stat can come from the attribute cache), reported as the
// up metric and logged once per change. A stat of a volume's item directory
// that returns ESTALE (the server restarted and its file handles are gone),
// times out or is busy is the same condition, ErrTransportUnavailable: the
// volume's condition metric and its event say so, and nothing is unmounted,
// remounted or rebound on account of it. The next pass looks again, and the
// volume is ok when the server answers. What to do about a transport that
// stays down is a person's call (or a later milestone's remediation).
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
