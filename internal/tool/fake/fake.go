// Package fake is a tool.Runner for tests: it records every call and answers
// from a script, so no test needs cryptsetup or e2fsprogs installed.
package fake

import (
	"context"
	"os"
	"sync"
)

// Reply is one scripted answer.
type Reply struct {
	Stdout, Stderr string
	Code           int
	Err            error
}

// Call is one recorded call. Extra is the number of extra descriptors passed.
type Call struct {
	Argv  []string
	Extra int
}

// Runner answers Replies in order, then Func if set, then success with no
// output. It is safe for concurrent use.
type Runner struct {
	Replies []Reply
	// Func, when set, answers calls once Replies is exhausted.
	Func func(argv []string) Reply

	mu    sync.Mutex
	next  int
	calls []Call
}

// Run records the call and returns the next scripted answer.
func (r *Runner) Run(_ context.Context, argv []string, extra []*os.File) ([]byte, []byte, int, error) {
	r.mu.Lock()
	r.calls = append(r.calls, Call{Argv: append([]string(nil), argv...), Extra: len(extra)})
	var rep Reply
	switch {
	case r.next < len(r.Replies):
		rep = r.Replies[r.next]
		r.next++
	case r.Func != nil:
		rep = r.Func(argv)
	}
	r.mu.Unlock()
	return []byte(rep.Stdout), []byte(rep.Stderr), rep.Code, rep.Err
}

// Calls returns a copy of the recorded calls.
func (r *Runner) Calls() []Call {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Call(nil), r.calls...)
}
