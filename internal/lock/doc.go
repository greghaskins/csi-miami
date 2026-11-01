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
//   - Release removes holder, since and the directory. A holder that was
//     broken out of the lock must not remove the new holder's files, so
//     Release and Renew first read holder and report ErrLost if it is not
//     the caller's.
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
// # Attribute caching sets the floor on Stale
//
// The nfs3 transport mounts with actimeo=30 and lookupcache=positive: a
// client may serve file attributes up to 30 seconds old, so another client's
// since can look older than it is, and a directory created elsewhere can look
// younger. Every modification time the package reads is read after opening
// the file or directory, which revalidates its attributes, and the probe
// is a file this client just created; but the cache is the reason Stale must
// be well above its window. The smallest Stale accepted is MinStale, 60
// seconds, and the default is DefaultStale, five minutes. A holder must renew
// at least three times per Stale (the miami command enforces it), so one
// missed renewal never makes a live lock stale.
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
// Errors other than ErrContended and ErrLost are *Error values carrying the
// operation and the lock name, never a path.
package lock
