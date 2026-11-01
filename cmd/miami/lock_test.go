package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
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

// fakeRenewer answers Renew from a script.
type fakeRenewer struct {
	results []error
	calls   int
	onCall  func(n int)
}

func (f *fakeRenewer) Renew(context.Context) error {
	n := f.calls
	f.calls++
	if f.onCall != nil {
		f.onCall(n)
	}
	if n >= len(f.results) {
		return nil
	}
	return f.results[n]
}

func TestRenewLoop(t *testing.T) {
	t.Parallel()
	errIO := errors.New("input/output error")
	lost := fmt.Errorf("renew: %w", lock.ErrLost)
	const limit = 50 * time.Second
	tests := []struct {
		name    string
		results []error
		tick    time.Duration // each renewal advances the fake clock by this
		cancel  int           // cancel the context during this call; -1 never
		wantErr error
		want    string
		calls   int
	}{
		{name: "renews until cancelled", results: []error{nil, nil, nil}, tick: 10 * time.Second, cancel: 3, calls: 3},
		{name: "lost stops at once", results: []error{nil, lost}, tick: 10 * time.Second, cancel: -1, wantErr: lock.ErrLost, calls: 2},
		{name: "a short outage is retried", results: []error{errIO, errIO, nil, errIO, nil}, tick: 10 * time.Second, cancel: 5, calls: 5},
		{name: "a success restarts the clock", results: []error{errIO, errIO, errIO, errIO, nil, errIO, errIO, errIO, errIO}, tick: 10 * time.Second, cancel: 9, calls: 9},
		{name: "an outage longer than the limit ends it", results: []error{errIO, errIO, errIO, errIO, errIO, errIO}, tick: 10 * time.Second, cancel: -1, wantErr: errIO, want: "could not be renewed for 1m0s", calls: 6},
		{name: "an error caused by cancellation is not a failure", results: []error{context.Canceled}, tick: time.Hour, cancel: 1, calls: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			clock := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
			ticks := make(chan time.Time)
			r := &fakeRenewer{results: tt.results}
			r.onCall = func(n int) {
				// The loop's own goroutine runs this, so it is the only one
				// that touches the clock.
				clock = clock.Add(tt.tick)
				if n+1 == tt.cancel {
					cancel()
				}
			}
			errc := make(chan error, 1)
			go func() { errc <- renewLoop(ctx, r, ticks, func() time.Time { return clock }, limit) }()
			for {
				select {
				case err := <-errc:
					if tt.wantErr == nil && err != nil || tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
						t.Errorf("error = %v, want %v", err, tt.wantErr)
					}
					if tt.want != "" && (err == nil || !strings.Contains(err.Error(), tt.want)) {
						t.Errorf("error = %v, want it to contain %q", err, tt.want)
					}
					if r.calls != tt.calls {
						t.Errorf("Renew called %d times, want %d", r.calls, tt.calls)
					}
					return
				case ticks <- time.Time{}:
				}
			}
		})
	}
}
