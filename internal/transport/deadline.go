package transport

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/greghaskins/csi-miami/internal/mount"
)

// within runs fn in its own goroutine and waits for it, for ctx, or for d,
// whichever comes first. It exists for calls that can hang without honouring
// a context (resolving a path on a dead NFS mount, a resolver stuck on a
// socket): when the deadline passes the goroutine is abandoned, never joined,
// and the error wraps mount.ErrTimeout. The channel is buffered, so an
// abandoned call can finish and exit whenever it likes.
func within[T any](ctx context.Context, d time.Duration, what string, fn func(context.Context) (T, error)) (T, error) {
	var zero T
	ctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return zero, ctxError(what, err)
	}
	type result struct {
		v   T
		err error
	}
	ch := make(chan result, 1)
	go func() {
		v, err := fn(ctx)
		ch <- result{v, err}
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
