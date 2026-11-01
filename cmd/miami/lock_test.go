package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/lock"
)

// The lock tests run `miami lock` in this process, against a temporary
// directory. The commands `lock run` starts are this test binary again
// (helperCommand), in the mode its arguments name.

// helperMarker is the marker after which the test binary acts as a helper.
const helperMarker = "lock-helper"

// TestLockHelperProcess is not a test: it is the command that `lock run`
// starts in the tests below. It does nothing unless the marker is among its
// arguments.
func TestLockHelperProcess(_ *testing.T) {
	var mode string
	for i, a := range os.Args {
		if a == helperMarker && i+1 < len(os.Args) {
			mode = os.Args[i+1]
		}
	}
	if mode == "" {
		return
	}
	switch {
	case strings.HasPrefix(mode, "exit:"):
		n, _ := strconv.Atoi(strings.TrimPrefix(mode, "exit:"))
		os.Exit(n)
	case mode == "selfkill":
		_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
	case mode == "ready-wait":
		fmt.Println("ready")
		select {}
	case mode == "ready-term7":
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGTERM)
		fmt.Println("ready")
		<-ch
		os.Exit(7)
	case mode == "group":
		// A command that has started a process of its own, as a shell wrapper
		// does. It prints the pid before "ready".
		child := exec.Command("sleep", "600")
		if err := child.Start(); err != nil {
			os.Exit(98)
		}
		fmt.Printf("pid=%d\nready\n", child.Process.Pid)
		select {}
	case mode == "ready-ignore-term":
		signal.Ignore(syscall.SIGTERM)
		fmt.Println("ready")
		select {}
	}
	os.Exit(99)
}

func helperCommand(mode string) []string {
	return []string{"--", os.Args[0], "-test.run=^TestLockHelperProcess$", "--", helperMarker, mode}
}

// syncBuffer is a writer the test can wait on.
type syncBuffer struct {
	mu    sync.Mutex
	buf   bytes.Buffer
	ready chan struct{}
	once  sync.Once
}

func newSyncBuffer() *syncBuffer { return &syncBuffer{ready: make(chan struct{})} }

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n, err := b.buf.Write(p)
	if strings.Contains(b.buf.String(), "ready\n") {
		b.once.Do(func() { close(b.ready) })
	}
	return n, err
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// lockCmd runs `miami lock <args>` and returns the exit code and the output.
func lockCmd(args ...string) (code int, stdout, stderr string) {
	var so, se bytes.Buffer
	code = run(append([]string{"lock"}, args...), &so, &se)
	return code, so.String(), se.String()
}

func lockArgs(verb, dir string, extra ...string) []string {
	return append([]string{verb, "--dir", dir, "--name", "web"}, extra...)
}

func TestLockFlagValidation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	tests := []struct {
		name string
		args []string
		want string // in stderr
	}{
		{"unknown verb", []string{"hold"}, `unknown verb "hold"`},
		{"unknown flag", lockArgs("acquire", dir, "--nope"), "flag provided but not defined: -nope"},
		{"no dir", []string{"acquire", "--name", "web", "--holder", "h"}, "--dir is required"},
		{"no name", []string{"acquire", "--dir", dir, "--holder", "h"}, "--name is required"},
		{"acquire without holder", lockArgs("acquire", dir), "--holder is required"},
		{"release without holder", lockArgs("release", dir), "--holder is required"},
		{"acquire with a command", lockArgs("acquire", dir, "--holder", "h", "--", "true"), `unexpected argument "true"`},
		{"run without a command", lockArgs("run", dir), "a command is required after --"},
		{"stale below the floor", lockArgs("acquire", dir, "--holder", "h", "--stale", "59s"), "below the minimum 1m0s"},
		{"renew over a third of stale", lockArgs("run", dir, "--stale", "90s", "--renew", "31s", "--", "true"), "at most a third"},
		{"no room for the timeouts", lockArgs("run", dir, "--stale", "60s", "--renew", "20s", "--timeout", "20s", "--", "true"), "less than --stale"},
		{"renew zero", lockArgs("run", dir, "--renew", "0", "--", "true"), "--renew must be positive"},
		{"timeout zero", lockArgs("acquire", dir, "--holder", "h", "--timeout", "0"), "--timeout must be positive"},
		{"bad duration", lockArgs("acquire", dir, "--holder", "h", "--stale", "soon"), "invalid value"},
		{"bad name", []string{"acquire", "--dir", dir, "--name", "a/b", "--holder", "h"}, "separator"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			code, stdout, stderr := lockCmd(tt.args...)
			if code != 2 || stdout != "" || !strings.Contains(stderr, tt.want) {
				t.Errorf("exit %d, stdout %q, stderr %q; want exit 2, no stdout, stderr containing %q", code, stdout, stderr, tt.want)
			}
			if _, err := os.Stat(filepath.Join(dir, "web"+lock.Suffix)); err == nil {
				t.Error("a rejected command made the lock")
			}
		})
	}
}

func TestLockHelp(t *testing.T) {
	t.Parallel()
	for _, verb := range []string{"acquire", "release", "run"} {
		code, stdout, stderr := lockCmd(verb, "--help")
		if code != 0 || stderr != "" || !strings.Contains(stdout, "-stale") || !strings.Contains(stdout, "-dir") {
			t.Errorf("%s --help: exit %d, stdout %q, stderr %q", verb, code, stdout, stderr)
		}
	}
	code, stdout, _ := lockCmd("help")
	if code != 0 || stdout != lockUsage {
		t.Errorf("lock help: exit %d, stdout %q", code, stdout)
	}
}

func TestLockAcquireAndReleaseExitCodes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	steps := []struct {
		name string
		args []string
		code int
		err  string // in stderr; empty means nothing is printed
	}{
		{"acquire", lockArgs("acquire", dir, "--holder", "a"), 0, ""},
		{"acquire again, another holder", lockArgs("acquire", dir, "--holder", "b"), 1, "held by another holder"},
		{"release by another holder", lockArgs("release", dir, "--holder", "b"), 1, "not held by this holder"},
		{"release", lockArgs("release", dir, "--holder", "a"), 0, ""},
		{"release again", lockArgs("release", dir, "--holder", "a"), 1, "not held by this holder"},
		{"acquire after release", lockArgs("acquire", dir, "--holder", "b"), 0, ""},
		{"acquire in a directory that does not exist", []string{"acquire", "--dir", filepath.Join(dir, "missing"), "--name", "web", "--holder", "a"}, 2, "mkdir: no such file or directory"},
		{"release in a directory that does not exist", []string{"release", "--dir", filepath.Join(dir, "missing"), "--name", "web", "--holder", "a"}, 1, "not held"},
	}
	// --renew is for run only: a short --stale with the default --renew is fine.
	steps = append(steps, struct {
		name string
		args []string
		code int
		err  string
	}{"acquire with the minimum stale and the default renew", lockArgs("acquire", t.TempDir(), "--holder", "a", "--stale", "60s"), 0, ""})
	for _, s := range steps {
		code, stdout, stderr := lockCmd(s.args...)
		if code != s.code || stdout != "" {
			t.Errorf("%s: exit %d, stdout %q (stderr %q); want exit %d and no stdout", s.name, code, stdout, stderr, s.code)
		}
		if (s.err == "") != (stderr == "") || !strings.Contains(stderr, s.err) {
			t.Errorf("%s: stderr %q, want %q", s.name, stderr, s.err)
		}
		if strings.Contains(stderr, dir) {
			t.Errorf("%s: stderr %q contains a path", s.name, stderr)
		}
	}
}

func TestLockRunExitsWithTheCommandsStatus(t *testing.T) {
	t.Parallel()
	tests := []struct {
		mode string
		want int
	}{
		{"exit:0", 0},
		{"exit:3", 3},
		{"exit:255", 255},
		{"selfkill", 128 + int(syscall.SIGKILL)},
	}
	for _, tt := range tests {
		t.Run(tt.mode, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			var stdout, stderr bytes.Buffer
			code := run(append([]string{"lock"}, append(lockArgs("run", dir, "--holder", "h"), helperCommand(tt.mode)...)...), &stdout, &stderr)
			if code != tt.want {
				t.Errorf("exit %d, want %d (stderr %q)", code, tt.want, stderr.String())
			}
			if entries, _ := os.ReadDir(dir); len(entries) != 0 {
				t.Errorf("the lock was not released: %v", entries)
			}
		})
	}
}

func TestLockRunWhileTheLockIsHeld(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if code, _, stderr := lockCmd(lockArgs("acquire", dir, "--holder", "other")...); code != 0 {
		t.Fatal(stderr)
	}
	var stdout, stderr bytes.Buffer
	code := run(append([]string{"lock"}, append(lockArgs("run", dir), helperCommand("exit:9")...)...), &stdout, &stderr)
	if code != 1 || stdout.Len() != 0 || !strings.Contains(stderr.String(), "held by another holder") {
		t.Errorf("exit %d, stdout %q, stderr %q; want exit 1 without running the command", code, stdout.String(), stderr.String())
	}
	if h, err := os.ReadFile(filepath.Join(dir, "web.lock", "holder")); err != nil || string(h) != "other" { //nolint:gosec // a test reads its own temporary files
		t.Errorf("holder = %q, %v; the contender must leave the lock alone", h, err)
	}
}

func TestLockRunCommandThatCannotStart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	var stdout, stderr bytes.Buffer
	code := run(append([]string{"lock"}, lockArgs("run", dir, "--", filepath.Join(dir, "no-such-command"))...), &stdout, &stderr)
	if code != 2 || !strings.Contains(stderr.String(), "starting the command") {
		t.Errorf("exit %d, stderr %q; want exit 2", code, stderr.String())
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Errorf("the lock was not released: %v", entries)
	}
}

// A renewal that finds the lock lost stops the command and releases nothing.
func TestLockRunTerminatesTheCommandWhenTheLockIsLost(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		mode     string
		wantKill bool
	}{
		{"command stops at SIGTERM", "ready-wait", false},
		{"command ignores SIGTERM", "ready-ignore-term", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			stdout, stderr := newSyncBuffer(), newSyncBuffer()
			result := make(chan int, 1)
			go func() {
				args := append(lockArgs("run", dir, "--holder", "me", "--renew", "10ms", "--timeout", "200ms"), helperCommand(tt.mode)...)
				result <- run(append([]string{"lock"}, args...), stdout, stderr)
			}()
			select {
			case <-stdout.ready:
			case <-time.After(60 * time.Second):
				t.Fatalf("the command never started; stderr %q", stderr.String())
			}
			// Another holder takes over, as if our lock had been broken.
			holder := filepath.Join(dir, "web.lock", "holder")
			if err := os.WriteFile(holder, []byte("other"), 0o600); err != nil {
				t.Fatal(err)
			}
			var code int
			select {
			case code = <-result:
			case <-time.After(60 * time.Second):
				t.Fatal("miami lock run did not return after the lock was lost")
			}
			if code != 2 {
				t.Errorf("exit %d, want 2; stderr %q", code, stderr.String())
			}
			msg := stderr.String()
			if !strings.Contains(msg, "not held by this holder") || !strings.Contains(msg, "terminating the command") {
				t.Errorf("stderr %q does not say the lock was lost and the command terminated", msg)
			}
			if killed := strings.Contains(msg, "killing it"); killed != tt.wantKill {
				t.Errorf("killed = %v, want %v (stderr %q)", killed, tt.wantKill, msg)
			}
			// Nothing of the new holder's was touched.
			if got, err := os.ReadFile(holder); err != nil || string(got) != "other" { //nolint:gosec // a test reads its own temporary files
				t.Errorf("holder = %q, %v; the lost lock must not be released", got, err)
			}
		})
	}
}

// A lost lock stops what the command started too, not only the command.
func TestLockRunSignalsTheWholeProcessGroup(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	stdout, stderr := newSyncBuffer(), newSyncBuffer()
	result := make(chan int, 1)
	go func() {
		args := append(lockArgs("run", dir, "--holder", "me", "--renew", "10ms", "--timeout", "5s"), helperCommand("group")...)
		result <- run(append([]string{"lock"}, args...), stdout, stderr)
	}()
	select {
	case <-stdout.ready:
	case <-time.After(60 * time.Second):
		t.Fatalf("the command never started; stderr %q", stderr.String())
	}
	var pid int
	if _, err := fmt.Sscanf(stdout.String(), "pid=%d", &pid); err != nil {
		t.Fatalf("no pid in %q: %v", stdout.String(), err)
	}
	t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
	if err := os.WriteFile(filepath.Join(dir, "web.lock", "holder"), []byte("other"), 0o600); err != nil { //nolint:gosec // a test's own file
		t.Fatal(err)
	}
	select {
	case code := <-result:
		if code != 2 {
			t.Errorf("exit %d, want 2; stderr %q", code, stderr.String())
		}
	case <-time.After(60 * time.Second):
		t.Fatal("miami lock run did not return after the lock was lost")
	}
	// The process the command started is gone (or a zombie nobody reaped).
	deadline := time.Now().Add(10 * time.Second)
	for {
		raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid)) //nolint:gosec // a pid
		if err != nil || strings.Contains(string(raw), ") Z ") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("process %d, started by the command, is still running after the lock was lost: %s", pid, raw)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// SIGTERM to `lock run` goes to the command, and the lock is released after
// the command exits. It signals this process, so it must not run beside the
// other tests that run the command: it is the only test here that does not
// call t.Parallel.
func TestLockRunForwardsSIGTERM(t *testing.T) {
	dir := t.TempDir()
	stdout, stderr := newSyncBuffer(), newSyncBuffer()
	result := make(chan int, 1)
	go func() {
		args := append(lockArgs("run", dir, "--holder", "me"), helperCommand("ready-term7")...)
		result <- run(append([]string{"lock"}, args...), stdout, stderr)
	}()
	select {
	case <-stdout.ready:
	case <-time.After(60 * time.Second):
		t.Fatalf("the command never started; stderr %q", stderr.String())
	}
	// While the command runs the lock is held, and nobody else can take it.
	if code, _, msg := lockCmd(lockArgs("acquire", dir, "--holder", "other")...); code != 1 {
		t.Errorf("acquire while the command runs: exit %d (%s), want 1", code, msg)
	}
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	var code int
	select {
	case code = <-result:
	case <-time.After(60 * time.Second):
		t.Fatal("miami lock run did not return after SIGTERM")
	}
	if code != 7 {
		t.Errorf("exit %d, want the command's 7; stderr %q", code, stderr.String())
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Errorf("the lock was not released after the command exited: %v", entries)
	}
}

// fakeExpiry is a timer the test fires by hand.
type fakeExpiry struct {
	ch     chan time.Time
	mu     sync.Mutex
	resets []time.Duration
}

func newFakeExpiry() *fakeExpiry { return &fakeExpiry{ch: make(chan time.Time, 1)} }

func (f *fakeExpiry) C() <-chan time.Time { return f.ch }
func (f *fakeExpiry) Stop()               {}
func (f *fakeExpiry) Reset(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resets = append(f.resets, d)
}
func (f *fakeExpiry) fire() { f.ch <- time.Time{} }
func (f *fakeExpiry) resetCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.resets)
}

// scriptedRenewer lets the test decide what each Renew returns, and when.
type scriptedRenewer struct {
	calls   chan context.Context // each Renew announces itself here
	answers chan error           // and waits for an answer, or for its context
}

func newScriptedRenewer() *scriptedRenewer {
	return &scriptedRenewer{calls: make(chan context.Context), answers: make(chan error)}
}

func (r *scriptedRenewer) Renew(ctx context.Context) error {
	r.calls <- ctx
	select {
	case err := <-r.answers:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestRenewLoop(t *testing.T) {
	t.Parallel()
	const limit = 20 * time.Second
	errIO := errors.New("input/output error")
	lost := fmt.Errorf("renew: %w", lock.ErrLost)

	type harness struct {
		ctx    context.Context
		cancel context.CancelFunc
		ticks  chan time.Time
		exp    *fakeExpiry
		r      *scriptedRenewer
		result chan error
	}
	start := func(t *testing.T) *harness {
		t.Helper()
		h := &harness{ticks: make(chan time.Time), exp: newFakeExpiry(), r: newScriptedRenewer(), result: make(chan error, 1)}
		h.ctx, h.cancel = context.WithCancel(context.Background())
		t.Cleanup(h.cancel)
		go func() { h.result <- renewLoop(h.ctx, h.r, h.ticks, h.exp, limit) }()
		return h
	}
	// tick makes the loop start a renewal and returns its context.
	tick := func(h *harness) context.Context {
		h.ticks <- time.Time{}
		return <-h.r.calls
	}

	t.Run("the timer is armed at the start and after every success", func(t *testing.T) {
		t.Parallel()
		h := start(t)
		tick(h)
		h.r.answers <- nil
		tick(h)
		h.r.answers <- nil
		tick(h) // the loop has handled the second success before it takes this tick
		h.cancel()
		if err := <-h.result; err != nil {
			t.Errorf("error = %v, want nil after cancellation", err)
		}
		if got := h.exp.resetCount(); got != 3 {
			t.Errorf("timer reset %d times, want 3 (start and two successes)", got)
		}
		if h.exp.resets[0] != limit {
			t.Errorf("timer armed for %s, want %s", h.exp.resets[0], limit)
		}
	})
	t.Run("lost stops at once", func(t *testing.T) {
		t.Parallel()
		h := start(t)
		tick(h)
		h.r.answers <- lost
		if err := <-h.result; !errors.Is(err, lock.ErrLost) {
			t.Errorf("error = %v, want ErrLost", err)
		}
	})
	t.Run("a failure is retried and a success restarts the timer", func(t *testing.T) {
		t.Parallel()
		h := start(t)
		tick(h)
		h.r.answers <- errIO
		tick(h)
		h.r.answers <- nil
		tick(h)
		if got := h.exp.resetCount(); got != 2 {
			t.Errorf("timer reset %d times, want 2 (start and the success, not the failure)", got)
		}
		h.cancel()
		if err := <-h.result; err != nil {
			t.Errorf("error = %v, want nil", err)
		}
	})
	t.Run("the timer ends it while waiting for a tick, whatever the tick phase", func(t *testing.T) {
		t.Parallel()
		h := start(t)
		h.exp.fire() // no tick has come: the limit does not wait for one
		err := <-h.result
		if err == nil || !strings.Contains(err.Error(), "could not be renewed for 20s") {
			t.Errorf("error = %v, want the limit", err)
		}
	})
	t.Run("the error that preceded the limit is reported", func(t *testing.T) {
		t.Parallel()
		h := start(t)
		tick(h)
		h.r.answers <- errIO
		tick(h) // the loop has recorded the failure before it takes this tick
		h.exp.fire()
		err := <-h.result
		if err == nil || !errors.Is(err, errIO) || !strings.Contains(err.Error(), "could not be renewed for 20s") {
			t.Errorf("error = %v, want the limit and the last error", err)
		}
	})
	t.Run("the timer ends it and interrupts a renewal that is in flight", func(t *testing.T) {
		t.Parallel()
		h := start(t)
		rctx := tick(h) // the renewal now waits for an answer that never comes
		h.exp.fire()
		err := <-h.result
		if err == nil || !strings.Contains(err.Error(), "could not be renewed for 20s") {
			t.Errorf("error = %v, want the limit", err)
		}
		select {
		case <-rctx.Done():
		default:
			t.Error("the renewal in flight was not cancelled")
		}
	})
	t.Run("cancellation during a renewal is not a failure", func(t *testing.T) {
		t.Parallel()
		h := start(t)
		tick(h)
		h.cancel()
		if err := <-h.result; err != nil {
			t.Errorf("error = %v, want nil", err)
		}
	})
}
