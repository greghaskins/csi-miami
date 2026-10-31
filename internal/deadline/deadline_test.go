package deadline

import (
	"context"
	"errors"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/mount"
)

func TestRunReturnsAResultThatWonTheRace(t *testing.T) {
	t.Parallel()
	got, err := Run(context.Background(), time.Minute, "x", &Set{}, "k", func(context.Context) (int, error) { return 7, nil })
	if err != nil || got != 7 {
		t.Errorf("got %v, %v; want 7", got, err)
	}
	if _, err := Run(context.Background(), time.Minute, "x", &Set{}, "k", func(context.Context) (int, error) { return 0, os.ErrClosed }); !errors.Is(err, os.ErrClosed) {
		t.Errorf("got %v, want the function's error", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = Run(ctx, time.Minute, "x", &Set{}, "k", func(context.Context) (int, error) { return 1, nil })
	if !errors.Is(err, context.Canceled) || errors.Is(err, mount.ErrTimeout) {
		t.Errorf("got %v, want context.Canceled and not a timeout for a context that is done before the call", err)
	}
}

func TestRunKeysAreIndependent(t *testing.T) {
	t.Parallel()
	var g Set
	hang := make(chan struct{})
	defer close(hang)
	_, err := Run(context.Background(), 5*time.Millisecond, "a", &g, "k1", func(context.Context) (int, error) { <-hang; return 0, nil })
	if !errors.Is(err, mount.ErrTimeout) {
		t.Fatalf("got %v, want ErrTimeout", err)
	}
	// Another key is not held back.
	if v, err := Run(context.Background(), time.Minute, "b", &g, "k2", func(context.Context) (int, error) { return 7, nil }); err != nil || v != 7 {
		t.Errorf("another key: %v, %v", v, err)
	}
}

// A call that timed out holds its key until it returns; a second call on the
// key is ErrBusy and starts nothing, and after the first returns the key is
// free again.
func TestRunBusyUntilTheAbandonedCallReturns(t *testing.T) {
	t.Parallel()
	var g Set
	hang := make(chan struct{})
	started := make(chan struct{}, 2)
	_, err := Run(context.Background(), 5*time.Millisecond, "a", &g, "k", func(context.Context) (int, error) {
		started <- struct{}{}
		<-hang
		return 0, nil
	})
	if !errors.Is(err, mount.ErrTimeout) {
		t.Fatalf("got %v, want ErrTimeout", err)
	}
	<-started
	ran := false
	_, err = Run(context.Background(), time.Minute, "a", &g, "k", func(context.Context) (int, error) { ran = true; return 0, nil })
	if !errors.Is(err, mount.ErrBusy) || ran {
		t.Fatalf("got %v (ran=%v), want ErrBusy without starting the call", err, ran)
	}

	close(hang)
	// The abandoned goroutine releases the key when it returns; the first
	// call that finds it free proves it. Yield rather than sleep.
	for range 10_000_000 {
		runtime.Gosched()
		v, err := Run(context.Background(), time.Minute, "a", &g, "k", func(context.Context) (int, error) { return 9, nil })
		if err == nil {
			if v != 9 {
				t.Fatalf("got %d, want 9", v)
			}
			return
		}
		if !errors.Is(err, mount.ErrBusy) {
			t.Fatalf("got %v, want nil or ErrBusy", err)
		}
	}
	t.Fatal("the key was never released")
}

// A function that honours its context and fails with the context's error when
// the deadline passes is reported as a timeout of ours.
func TestRunReportsAContextErrorFromTheFunctionAsATimeout(t *testing.T) {
	t.Parallel()
	_, err := Run(context.Background(), 5*time.Millisecond, "a", &Set{}, "k", func(ctx context.Context) (int, error) {
		<-ctx.Done()
		return 0, ctx.Err()
	})
	if !errors.Is(err, mount.ErrTimeout) || !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("got %v, want ErrTimeout wrapping DeadlineExceeded", err)
	}
}

// A call that returns, with a result or an error, frees its key at once.
func TestRunReleasesTheKeyWhenTheFunctionReturns(t *testing.T) {
	t.Parallel()
	var g Set
	for range 3 {
		if _, err := Run(context.Background(), time.Minute, "a", &g, "k", func(context.Context) (int, error) { return 0, os.ErrClosed }); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("got %v, want the function's error each time, never ErrBusy", err)
		}
	}
}
