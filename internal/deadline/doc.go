// Package deadline runs a call that can hang under a deadline of its own.
//
// A stat, a name lookup or a path resolution on a dead NFS mount does not
// return and ignores its context. Run puts such a call in its own goroutine
// and gives the caller its answer when the call returns, when the context is
// done or when the deadline passes, whichever comes first. A call that
// outlives its deadline is abandoned, never joined: it writes to a buffered
// channel and exits whenever the kernel lets it go. The caller gets an error
// wrapping mount.ErrTimeout.
//
// A Set admits one call per key at a time, so a caller that polls a hung path
// cannot pile up stuck goroutines and OS threads: while the previous call on
// the key has not returned, the next fails at once wrapping mount.ErrBusy.
// The key names what is hung (a path, a server name), never the caller.
package deadline
