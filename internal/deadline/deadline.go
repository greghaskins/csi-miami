package deadline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/greghaskins/csi-miami/internal/mount"
)

// Set is the set of keys with a call that has not returned. A call on a path
// that hangs ignores its context and keeps an OS thread, so a caller that
// polls must not start another while the last one is still stuck. The zero
// value is an empty set, ready to use, and safe for concurrent use.
type Set struct {
	mu   sync.Mutex
	keys map[string]struct{}
}

func (f *Set) acquire(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, busy := f.keys[key]; busy {
		return false
	}
	if f.keys == nil {
		f.keys = map[string]struct{}{}
	}
	f.keys[key] = struct{}{}
	return true
}

func (f *Set) release(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.keys, key)
}

// Run runs fn in its own goroutine and waits for it, for ctx, or for d,
// whichever comes first. It exists for calls that can hang without honouring
// a context (resolving a path on a dead NFS mount, a resolver stuck on a
// socket): when the deadline passes the goroutine is abandoned, never joined,
// and the error wraps mount.ErrTimeout. The channel is buffered, so an
// abandoned call can finish and exit whenever it likes.
//
// While a call with the same key in g has not returned, a new one fails at
// once, wrapping mount.ErrBusy, and starts nothing: the stuck goroutine is
// the only one there will ever be per key. The goroutine releases its key
// when fn returns, however it returns.
func Run[T any](ctx context.Context, d time.Duration, what string, g *Set, key string, fn func(context.Context) (T, error)) (T, error) {
	var zero T
	ctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return zero, ctxError(what, err)
	}
	if !g.acquire(key) {
		return zero, fmt.Errorf("%s: %w; a previous call has not returned, re-observe and retry later", what, mount.ErrBusy)
	}
	type result struct {
		v   T
		err error
	}
	ch := make(chan result, 1)
	go func() {
		var r result
		// Released before the result is sent, so a caller that gets the
		// result can call again at once; deferred, so a panic frees it too.
		func() {
			defer g.release(key)
			r.v, r.err = fn(ctx)
		}()
		ch <- r
	}()
	// A function that honours ctx fails with ctx's error when the deadline
	// passes; that is a timeout of ours and is reported as one.
	finish := func(r result) (T, error) {
		if cause := ctx.Err(); r.err != nil && cause != nil && errors.Is(r.err, cause) {
			return zero, ctxError(what, cause)
		}
		return r.v, r.err
	}
	select {
	case r := <-ch:
		return finish(r)
	case <-ctx.Done():
		// A result that arrived at the same moment is the real answer.
		select {
		case r := <-ch:
			return finish(r)
		default:
		}
		return zero, ctxError(what, ctx.Err())
	}
}

func ctxError(what string, cause error) error {
	if errors.Is(cause, context.DeadlineExceeded) {
		return fmt.Errorf("%s: %w: %w", what, mount.ErrTimeout, cause)
	}
	return fmt.Errorf("%s: %w", what, cause)
}
