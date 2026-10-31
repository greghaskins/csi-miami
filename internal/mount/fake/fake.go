// Package fake is a mount.Mounter for other packages' tests. It validates
// paths and flags as the real one does, records every call it starts, returns
// errors configured per target, answers Statfs and Mounts from canned data,
// and can hang calls the way a dead NFS mount does, including the real
// Mounter's default deadline and its ErrBusy: a call that timed out keeps its
// path busy until the test releases it. That lets deadline handling and the
// reconciler's reaction to a hung transport be tested without a kernel.
//
// A fake does not simulate mounts: it never changes what a later call or
// Mounts returns, except through the configuration the test sets.
package fake

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/observe"
)

// Operation names, as recorded in Call.Op and used to configure errors.
const (
	OpMount           = "mount"
	OpBind            = "bind"
	OpRemountReadOnly = "remount"
	OpUnmount         = "unmount"
	OpStatfs          = "statfs"
	OpMounts          = "mounts"
)

// Call is one recorded call. Fields that the operation does not take are
// zero.
type Call struct {
	Op     string
	Source string
	Target string // Statfs records its path here
	FSType string
	Flags  uintptr // Mount: mount flags; Unmount: unmount flags
	Data   string
	// ReadOnly is the requested state for Bind and RemountReadOnly.
	ReadOnly bool
}

// Mounter implements mount.Mounter. The zero value is ready to use and safe
// for concurrent use.
type Mounter struct {
	// OnCall, when set before the first call, is invoked with each call as it
	// starts, outside the fake's lock and before any blocking, so a test can
	// learn that a call is hung without polling. It must not be changed while
	// calls are in flight.
	OnCall func(Call)

	// DefaultTimeout bounds a call whose context has no deadline, as New's
	// argument does for the real Mounter. Zero means mount.DefaultTimeout.
	// Set it before the first call.
	DefaultTimeout time.Duration

	mu       sync.Mutex
	calls    []Call
	errs     map[key]error
	statfs   map[string]unix.Statfs_t
	blocked  map[string]chan struct{}
	busy     map[busyKey]string // the blocked path that caused each busy key
	table    observe.Table
	tableErr error
}

var _ mount.Mounter = (*Mounter)(nil)

type key struct{ op, target string }

// busyKey is what the real Mounter tracks: one call at a time per path, with
// statfs and the resolving of a bind source apart from the calls that change
// mounts.
type busyKey struct {
	kind keyKind
	path string
}

type keyKind int

const (
	kindMount   keyKind = iota // calls that change mounts, by target
	kindStatfs                 // statfs, by path
	kindResolve                // resolving a bind source, by source
)

// anyOp matches every operation in the error table.
const anyOp = ""

// FailOn makes every operation on target return err, until cleared with a nil
// err.
func (f *Mounter) FailOn(target string, err error) { f.setErr(key{anyOp, target}, err) }

// FailOp makes op on target return err, taking precedence over FailOn.
func (f *Mounter) FailOp(op, target string, err error) { f.setErr(key{op, target}, err) }

func (f *Mounter) setErr(k key, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err == nil {
		delete(f.errs, k)
		return
	}
	if f.errs == nil {
		f.errs = map[key]error{}
	}
	f.errs[k] = err
}

// SetStatfs sets what Statfs returns for path. Other paths return a zero
// result.
func (f *Mounter) SetStatfs(path string, st unix.Statfs_t) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.statfs == nil {
		f.statfs = map[string]unix.Statfs_t{}
	}
	f.statfs[path] = st
}

// SetTable sets what Mounts returns: the mount table the code under test
// observes. A nil table is an empty one.
func (f *Mounter) SetTable(t observe.Table) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.table = append(observe.Table(nil), t...)
}

// SetMountsError makes Mounts fail with err, until cleared with nil.
func (f *Mounter) SetMountsError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tableErr = err
}

// Block makes every operation on path, or on anything below it, hang as a call
// on a dead NFS mount does: a bind from a dead transport, a statfs below it, an
// unmount of it. A blocked call returns only when its context is done, with an
// error wrapping mount.ErrTimeout (a deadline, including the default one) or
// the context's own error (a cancellation), or after Unblock. As with the real
// Mounter, a call that timed out leaves its path busy: later calls on that
// path fail at once with mount.ErrBusy until Unblock. For a bind from a
// blocked source, the path held busy is the source (it is resolved first, on
// a key of its own), not the target. Mounts is never blocked.
func (f *Mounter) Block(path string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.blocked == nil {
		f.blocked = map[string]chan struct{}{}
	}
	if _, ok := f.blocked[path]; !ok {
		f.blocked[path] = make(chan struct{})
	}
}

// Unblock releases the calls blocked by path, stops blocking it, and clears
// the busy state it caused: every key that was left busy because of this
// block, and every path at or below it.
func (f *Mounter) Unblock(path string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if ch, ok := f.blocked[path]; ok {
		close(ch)
		delete(f.blocked, path)
	}
	for k, cause := range f.busy {
		if cause == path || within(path, k.path) {
			delete(f.busy, k)
		}
	}
}

// Calls returns a copy of the recorded calls, in order. A call is recorded
// when it starts, so a blocked call is visible; a call refused for an invalid
// argument or because its path is busy never started and is not recorded.
func (f *Mounter) Calls() []Call {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Call(nil), f.calls...)
}

// Reset forgets the recorded calls, keeping the configuration.
func (f *Mounter) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = nil
}

// enter applies the deadline, busy and blocking behaviour of the real Mounter
// to c, records it, and returns the error configured for its target.
func (f *Mounter) enter(ctx context.Context, c Call) error {
	if _, ok := ctx.Deadline(); !ok {
		d := f.DefaultTimeout
		if d <= 0 {
			d = mount.DefaultTimeout
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d)
		defer cancel()
	}
	if err := ctx.Err(); err != nil {
		return ctxErr(c, err, "before the call started")
	}

	keys := keysOf(c)
	f.mu.Lock()
	for _, k := range keys {
		if _, busy := f.busy[k]; busy {
			f.mu.Unlock()
			return fmt.Errorf("%s %s: %w; re-observe and retry later", c.Op, k.path, mount.ErrBusy)
		}
	}
	f.calls = append(f.calls, c)
	var release chan struct{}
	for _, k := range keys {
		if cause, ch := f.blockerLocked(k.path); ch != nil {
			if f.busy == nil {
				f.busy = map[busyKey]string{}
			}
			f.busy[k] = cause
			release = ch
			break
		}
	}
	err, ok := f.errs[key{c.Op, c.Target}]
	if !ok {
		err = f.errs[key{anyOp, c.Target}]
	}
	f.mu.Unlock()

	if f.OnCall != nil {
		f.OnCall(c)
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return ctxErr(c, ctx.Err(), "call abandoned, it may still be running")
		}
	}
	return err
}

// keysOf is the in-flight keys the real Mounter takes for a call: a bind
// resolves its source under a key of its own, then takes the target's; statfs
// has its own; reading the table takes none.
func keysOf(c Call) []busyKey {
	switch c.Op {
	case OpMounts:
		return nil
	case OpStatfs:
		return []busyKey{{kindStatfs, c.Target}}
	case OpBind:
		return []busyKey{{kindResolve, c.Source}, {kindMount, c.Target}}
	default:
		return []busyKey{{kindMount, c.Target}}
	}
}

// blockerLocked returns the blocked path that covers path, and its release
// channel.
func (f *Mounter) blockerLocked(path string) (string, chan struct{}) {
	for p, ch := range f.blocked {
		if within(p, path) {
			return p, ch
		}
	}
	return "", nil
}

func ctxErr(c Call, cause error, what string) error {
	if cause == context.DeadlineExceeded {
		return fmt.Errorf("%s %s: %w (%s): %w", c.Op, c.Target, mount.ErrTimeout, what, cause)
	}
	return fmt.Errorf("%s %s: %w (%s)", c.Op, c.Target, cause, what)
}

func within(dir, path string) bool {
	return path == dir || dir == "/" || strings.HasPrefix(path, dir+"/")
}

// Mount validates its arguments as the real Mounter does, then records the call.
func (f *Mounter) Mount(ctx context.Context, source, target, fstype string, flags uintptr, data string) error {
	if err := mount.CheckPath("mount", "target", target); err != nil {
		return err
	}
	return f.enter(ctx, Call{Op: OpMount, Source: source, Target: target, FSType: fstype, Flags: flags, Data: data})
}

// Bind validates its arguments as the real Mounter does, then records the call.
func (f *Mounter) Bind(ctx context.Context, source, target string, readOnly bool) error {
	if err := mount.CheckPath("bind", "source", source); err != nil {
		return err
	}
	if err := mount.CheckPath("bind", "target", target); err != nil {
		return err
	}
	return f.enter(ctx, Call{Op: OpBind, Source: source, Target: target, ReadOnly: readOnly})
}

// RemountReadOnly validates its arguments as the real Mounter does, then
// records the call.
func (f *Mounter) RemountReadOnly(ctx context.Context, target string, readOnly bool) error {
	if err := mount.CheckPath("remount", "target", target); err != nil {
		return err
	}
	return f.enter(ctx, Call{Op: OpRemountReadOnly, Target: target, ReadOnly: readOnly})
}

// Unmount validates its arguments as the real Mounter does (including its flags
// and its refusal of "/"), then records the call.
func (f *Mounter) Unmount(ctx context.Context, target string, flags int) error {
	if err := mount.CheckUnmount(target, flags); err != nil {
		return err
	}
	return f.enter(ctx, Call{Op: OpUnmount, Target: target, Flags: uintptr(flags)})
}

// Statfs validates its path as the real Mounter does, records the call and
// returns the configured result for path.
func (f *Mounter) Statfs(ctx context.Context, path string) (unix.Statfs_t, error) {
	if err := mount.CheckPath("statfs", "path", path); err != nil {
		return unix.Statfs_t{}, err
	}
	if err := f.enter(ctx, Call{Op: OpStatfs, Target: path}); err != nil {
		return unix.Statfs_t{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.statfs[path], nil
}

// Mounts records the call and returns the configured table (see SetTable), or
// the configured error.
func (f *Mounter) Mounts(ctx context.Context) (observe.Table, error) {
	if err := f.enter(ctx, Call{Op: OpMounts, Target: observe.MountinfoPath}); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.tableErr != nil {
		return nil, f.tableErr
	}
	return append(observe.Table(nil), f.table...), nil
}
