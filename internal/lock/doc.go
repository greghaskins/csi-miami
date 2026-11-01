// Package lock is the mutual exclusion that directory items document:
// a lock any number of nodes can take on a shared NFSv3 export, built from
// the two operations that are atomic on the server, and from nothing else.
// flock and fcntl locks are node-local on a nolock transport and are not the
// contract (brief section 4.8).
//
// # The contract
//
// A lock named N in directory D is the directory D/N.lock (Suffix is ".lock").
// It is held by whoever made it:
//
//   - Acquire makes the directory with mkdir, which the server executes
//     atomically: of any number of concurrent attempts exactly one succeeds
//     and the others see "exists", which is ErrContended. The winner then
//     writes two files inside, each created under a temporary name and
//     renamed into place so that a reader never sees a partial file: holder,
//     whose content is the holder string the caller gave (by default
//     "<hostname>:<pid>"), and since, an empty file whose modification time
//     is the renewal stamp.
//   - Renew rewrites since. The stamp is the modification time the server
//     gives the new file; the holder's clock is never used.
//   - Release renames the lock directory away, removes it and what is in it.
//   - The holder keeps the directory it made open, and everything it does to
//     the lock is relative to that handle, never to the path. A lock that is
//     broken and taken by someone else under the same path is therefore never
//     written to or removed by the holder that lost it: a stamp written in
//     that instant lands in the directory that was moved away, and Release
//     checks, by device and inode, that the directory it renamed is the one
//     it holds, and puts it back if not. Renew and Release read holder and
//     compare the path with the handle; they report ErrLost if the lock is
//     not the caller's, or has been removed.
//
// # Staleness
//
// A holder that dies cannot release. The lock is then stale, and an acquirer
// that finds it contended may break it. Age is measured on the server's
// clock only: the acquirer creates a probe file in D under a unique temporary
// name, reads its modification time (the server's "now"), and removes it. The
// lock is stale when since is older than the probe by more than Options.Stale.
// A lock without since (the holder died between mkdir and its first write) is
// stale when the lock directory's own modification time is older than the
// probe by more than Options.Stale. No local time enters the decision, so
// clock skew between nodes cannot break a live lock or keep a dead one.
//
// To break a stale lock the acquirer renames the lock directory to a unique
// hidden name in D (rename is atomic, so two breakers cannot both win), removes
// it, and makes the directory as usual. A breaker that loses the rename tries
// to make the directory once more and, if that finds it taken, reports
// ErrContended. A breaker checks, after winning the rename, that the directory
// it moved still has the holder and stamp it judged stale, and puts it back
// if not (a holder renewed, or the lock changed hands, between the judgement
// and the rename).
//
// # The floor on Stale
//
// A holder that cannot renew must stop its work before another acquirer may
// break the lock, and that takes time: a renewal period, a call that hangs
// until its deadline (Options.Timeout), the grace the work needs to stop
// (another Timeout) and the latency of the last stamp. The miami command
// requires renew plus twice the timeout to be less than Stale, and renew at
// most a third of Stale, so one missed renewal never makes a live lock stale.
// The smallest Stale accepted is MinStale, 60 seconds, which leaves room for
// that margin with the default timeout; the default is DefaultStale, five
// minutes.
//
// The nfs3 transport mounts with actimeo=30 and lookupcache=positive: a
// client may serve file attributes up to 30 seconds old, so another client's
// since can look older than it is. That is the secondary reason for the floor:
// every modification time the package reads is read after opening the file
// or directory, which revalidates its attributes, and the probe is a file this
// client just created, so a stale cache does not enter a decision.
//
// # Failure
//
// Every file system call runs under a deadline of its own (Options.Timeout,
// default DefaultTimeout) through package deadline, because a call on a hung
// export does not return and ignores its context. A call that times out
// returns an error wrapping mount.ErrTimeout and the call is abandoned: if it
// completes later it may still have its effect, which for the call that makes
// the lock directory means a lock nobody holds, which goes stale and is
// broken like any other.
//
// A process killed in the middle of a call can leave entries in the directory
// that holds the lock, hidden and named .NAME.probe-*, .NAME.tmp-*,
// .NAME.broken-* or .NAME.removed-*, and temporary files inside a lock
// directory. They are harmless to the lock (nothing reads them, and a lock
// directory holding one is removed with it) and can be deleted by hand.
//
// Errors other than ErrContended and ErrLost are *Error values carrying the
// operation and the lock name, never a path.
package lock
