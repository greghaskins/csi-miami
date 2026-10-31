// Package fake is a mount.Mounter for other packages' tests. It records every
// call, returns errors configured per target, can answer Statfs with a
// canned result, and can hang a call the way a dead NFS mount does, so that
// deadline handling can be tested without a kernel.
//
// A fake does not simulate mounts: it never changes what a later call
// returns, except through the configuration the test sets.
package fake

import (
	"context"
	"fmt"
	"sync"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/mount"
)

// Operation names, as recorded in Call.Op and used to configure errors.
const (
	OpMount           = "mount"
	OpBind            = "bind"
	OpRemountReadOnly = "remount"
	OpUnmount         = "unmount"
	OpStatfs          = "statfs"
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

	mu      sync.Mutex
	calls   []Call
	errs    map[key]error
	statfs  map[string]unix.Statfs_t
	blocked map[string]chan struct{}
}

var _ mount.Mounter = (*Mounter)(nil)

type key struct{ op, target string }

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

// Block makes every operation on target hang, as a call on a dead NFS mount
// does: it returns only when its context is done, with an error wrapping
// mount.ErrTimeout (a context deadline) or the context's own error (a
// cancellation), or after Unblock. A call blocked in the fake is simply
// returned from; no goroutine is left behind.
func (f *Mounter) Block(target string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.blocked == nil {
		f.blocked = map[string]chan struct{}{}
	}
	if _, ok := f.blocked[target]; !ok {
		f.blocked[target] = make(chan struct{})
	}
}

// Unblock releases the calls blocked on target and stops blocking it.
func (f *Mounter) Unblock(target string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if ch, ok := f.blocked[target]; ok {
		close(ch)
		delete(f.blocked, target)
	}
}

// Calls returns a copy of the recorded calls, in order. A call is recorded
// when it starts, so a blocked call is visible.
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

// enter records c and applies the blocking and error configuration for its
// target.
func (f *Mounter) enter(ctx context.Context, c Call) error {
	f.mu.Lock()
	f.calls = append(f.calls, c)
	release := f.blocked[c.Target]
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
			if ctx.Err() == context.DeadlineExceeded {
				return fmt.Errorf("%s %s: %w: %w", c.Op, c.Target, mount.ErrTimeout, ctx.Err())
			}
			return fmt.Errorf("%s %s: %w", c.Op, c.Target, ctx.Err())
		}
	}
	return err
}

// Mount records the call.
func (f *Mounter) Mount(ctx context.Context, source, target, fstype string, flags uintptr, data string) error {
	return f.enter(ctx, Call{Op: OpMount, Source: source, Target: target, FSType: fstype, Flags: flags, Data: data})
}

// Bind records the call.
func (f *Mounter) Bind(ctx context.Context, source, target string, readOnly bool) error {
	return f.enter(ctx, Call{Op: OpBind, Source: source, Target: target, ReadOnly: readOnly})
}

// RemountReadOnly records the call.
func (f *Mounter) RemountReadOnly(ctx context.Context, target string, readOnly bool) error {
	return f.enter(ctx, Call{Op: OpRemountReadOnly, Target: target, ReadOnly: readOnly})
}

// Unmount records the call.
func (f *Mounter) Unmount(ctx context.Context, target string, flags int) error {
	return f.enter(ctx, Call{Op: OpUnmount, Target: target, Flags: uintptr(flags)})
}

// Statfs records the call and returns the configured result for path.
func (f *Mounter) Statfs(ctx context.Context, path string) (unix.Statfs_t, error) {
	if err := f.enter(ctx, Call{Op: OpStatfs, Target: path}); err != nil {
		return unix.Statfs_t{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.statfs[path], nil
}
