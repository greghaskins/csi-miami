// Package mount wraps mount(2), umount2(2) and statfs(2) for the node agent.
//
// Every call runs in its own goroutine under a deadline taken from the
// caller's context (or the Mounter's default when the context has none),
// because a call on a dead NFS mount does not return and ignores
// cancellation. When the deadline passes the caller gets an error wrapping
// ErrTimeout and the goroutine is abandoned, never joined; it writes to a
// buffered channel so it can exit whenever the kernel lets it go.
//
// The operations are idempotent by construction and decide that from
// /proc/self/mountinfo, the only truth about the node: mounting what is
// already mounted exactly so, binding what is already bound, and unmounting
// what is not mounted are no-ops, and a different mount at the target is
// reported as ErrConflict instead of being stacked over. Nothing here execs a
// helper.
//
// Package fake provides a Mounter for other packages' tests.
package mount
