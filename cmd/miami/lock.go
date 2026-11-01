package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/greghaskins/csi-miami/internal/lock"
)

// Exit codes of `miami lock`. A command run by `lock run` exits with its own
// status, which can be any of these; the messages on stderr tell them apart.
const (
	exitHeld      = 0 // acquire: held; release: released; run: the command's status
	exitContended = 1 // acquire and run: someone else holds it; release: not held by --holder
	exitError     = 2 // the lock could not be decided, or a usage error
	exitNotALock  = 3 // acquire and run: the lock path holds something that is not a lock; nothing was touched
)

const lockUsage = `usage: miami lock acquire --dir <path> --name <lock> --holder <string> [flags]
       miami lock release --dir <path> --name <lock> --holder <string> [flags]
       miami lock run     --dir <path> --name <lock> [flags] -- <command> [args...]

A lock is the directory <dir>/<lock>.lock on a shared export.

acquire   one attempt; exit 0 held, 1 held by another, 2 error, 3 the lock path
          holds something that is not a lock (a file, a symbolic link, a
          directory with other contents); it is reported and left alone. Prints
          nothing, except a line on stderr when it broke a stale lock
release   exit 0 released, 1 not held by --holder, 2 error, 3 not a lock
run       acquire, run the command with stdio inherited, renew while it runs,
          release when it exits; exits with the command's status (128+n if
          killed by signal n), 1 if the lock is held by another, 3 if the lock
          path is not a lock, and 2 on an error or if the lock was lost, in
          which case the command was terminated

The command runs in a process group of its own, and a signal (SIGTERM,
SIGINT, or the SIGTERM and SIGKILL of a lost lock) goes to the whole group.
It is still best for a wrapper script to exec its command: a process that
leaves the group, or a command that outlives the group's leader, is not
reached, and the helper cannot stop what it cannot signal.

--renew must be at most a third of --stale, and --renew plus twice --timeout
must be less than --stale: after a failed renewal run stops the command at
--stale minus twice --timeout, so that SIGTERM (up to --timeout) and the last
stamp's own latency fit before another acquirer may break the lock.

The default holder of run is <hostname>:<pid>. Two containers of one pod that
are both PID 1 get the same one, and each would then treat the other's lock as
its own: containers that share a pod pass --holder with a value that tells
them apart.
`

// lockConfig is what the flags of `miami lock` set.
type lockConfig struct {
	dir     string
	name    string
	holder  string
	stale   time.Duration
	renew   time.Duration
	timeout time.Duration
}

// runLock parses `miami lock <verb> ...` and runs it. Help goes to stdout
// and exits 0; a usage error goes to stderr and exits 2.
func runLock(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		_, _ = fmt.Fprint(stderr, lockUsage)
		return exitError
	}
	verb := args[0]
	switch verb {
	case "-h", "-help", "--help", "help":
		_, _ = fmt.Fprint(stdout, lockUsage)
		return exitHeld
	case "acquire", "release", "run":
	default:
		_, _ = fmt.Fprintf(stderr, "miami lock: unknown verb %q\n\n%s", verb, lockUsage)
		return exitError
	}

	var out bytes.Buffer
	var cfg lockConfig
	fs := flag.NewFlagSet("miami lock "+verb, flag.ContinueOnError)
	fs.SetOutput(&out)
	fs.StringVar(&cfg.dir, "dir", "", "the directory that holds the lock (required)")
	fs.StringVar(&cfg.name, "name", "", "the lock's name; the lock is <dir>/<name>.lock (required)")
	fs.StringVar(&cfg.holder, "holder", "", "who holds the lock (required for acquire and release; default for run is <hostname>:<pid>)")
	fs.DurationVar(&cfg.stale, "stale", lock.DefaultStale, "a lock not renewed for this long, on the server's clock, can be broken; at least "+lock.MinStale.String())
	fs.DurationVar(&cfg.renew, "renew", 30*time.Second, "run: renew the lock this often; at most a third of --stale")
	fs.DurationVar(&cfg.timeout, "timeout", lock.DefaultTimeout, "deadline of each file system call; run: how long the command gets to stop after SIGTERM")
	if err := fs.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			_, _ = stdout.Write(out.Bytes())
			return exitHeld
		}
		_, _ = stderr.Write(out.Bytes())
		return exitError
	}
	command := fs.Args()
	if err := cfg.validate(verb, command); err != nil {
		_, _ = fmt.Fprintf(stderr, "miami lock %s: %v\n", verb, err)
		return exitError
	}

	switch verb {
	case "acquire":
		return lockAcquire(cfg, stderr)
	case "release":
		return lockRelease(cfg, stderr)
	}
	return lockRun(cfg, command, stdout, stderr)
}

// validate checks the flags of one verb and the command words after them.
func (c lockConfig) validate(verb string, command []string) error {
	switch {
	case c.dir == "":
		return errors.New("--dir is required")
	case c.name == "":
		return errors.New("--name is required")
	case verb != "run" && len(command) > 0:
		return fmt.Errorf("unexpected argument %q", command[0])
	case verb == "run" && len(command) == 0:
		return errors.New("a command is required after --")
	case verb != "run" && c.holder == "":
		// A separate process cannot guess the holder of another, and a lock
		// held as "<hostname>:<pid>" of a process that has exited could only
		// be released by guessing the pid.
		return errors.New("--holder is required")
	case c.stale < lock.MinStale:
		return fmt.Errorf("--stale %s is below the minimum %s (the attribute cache of the nfs3 transport is 30s)", c.stale, lock.MinStale)
	// --renew means nothing to a single attempt.
	case verb == "run" && c.renew <= 0:
		return errors.New("--renew must be positive")
	case verb == "run" && c.renew+2*c.timeout >= c.stale:
		return fmt.Errorf("--renew %s plus twice --timeout %s must be less than --stale %s", c.renew, c.timeout, c.stale)
	case verb == "run" && c.renew > c.stale/3:
		return fmt.Errorf("--renew %s must be at most a third of --stale %s", c.renew, c.stale)
	case c.timeout <= 0:
		return errors.New("--timeout must be positive")
	}
	return nil
}

func (c lockConfig) options() lock.Options {
	return lock.Options{Holder: c.holder, Stale: c.stale, Timeout: c.timeout}
}

// interrupted returns a context that is done at SIGTERM or SIGINT.
func interrupted() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
}

func lockAcquire(cfg lockConfig, stderr io.Writer) int {
	ctx, stop := interrupted()
	defer stop()
	l, err := lock.Acquire(ctx, cfg.dir, cfg.name, cfg.options())
	if err == nil {
		reportBroke("acquire", l, stderr)
	}
	return lockResult("acquire", err, stderr)
}

func lockRelease(cfg lockConfig, stderr io.Writer) int {
	ctx, stop := interrupted()
	defer stop()
	err := lock.Release(ctx, cfg.dir, cfg.name, cfg.options())
	return lockResult("release", err, stderr)
}

// lockResult turns an error of acquire or release into an exit code, and a
// message on stderr when there is something to say.
func lockResult(verb string, err error, stderr io.Writer) int {
	if err == nil {
		return exitHeld
	}
	_, _ = fmt.Fprintf(stderr, "miami lock %s: %v\n", verb, err)
	if errors.Is(err, lock.ErrContended) || errors.Is(err, lock.ErrLost) {
		return exitContended
	}
	if errors.Is(err, lock.ErrNotALock) {
		return exitNotALock
	}
	return exitError
}

// reportBroke tells stderr why a stale lock was broken to take this one.
func reportBroke(verb string, l *lock.Lock, stderr io.Writer) {
	if r := l.Broke(); r != "" {
		_, _ = fmt.Fprintf(stderr, "miami lock %s: broke a stale lock: %s\n", verb, r)
	}
}

// renewer is the part of a lock the renewal loop uses.
type renewer interface {
	Renew(ctx context.Context) error
}

// expiry is the timer that bounds how long renewals may keep failing.
type expiry interface {
	C() <-chan time.Time
	// Reset restarts the timer: it fires d from now.
	Reset(d time.Duration)
	Stop()
}

type realExpiry struct{ t *time.Timer }

func newRealExpiry() *realExpiry {
	t := time.NewTimer(time.Hour)
	t.Stop()
	return &realExpiry{t}
}

func (r *realExpiry) C() <-chan time.Time   { return r.t.C }
func (r *realExpiry) Reset(d time.Duration) { r.t.Reset(d) }
func (r *realExpiry) Stop()                 { r.t.Stop() }

// renewLoop renews on every tick until ctx is done. It returns nil then, and
// an error when the lock is lost or when no renewal has succeeded for limit.
// The limit is a timer armed at the start of the last successful renewal (the
// acquisition counts), so it fires at that instant whatever the phase of the
// ticks, and it interrupts a renewal that is still in flight. It measures how long this process can go
// without knowing that its own lock may have been broken, on the local
// monotonic clock; it is not a staleness decision about anyone's lock. Errors
// shorter than that are retried at the next tick.
func renewLoop(ctx context.Context, r renewer, ticks <-chan time.Time, exp expiry, limit time.Duration) error {
	exp.Reset(limit)
	defer exp.Stop()
	var lastErr error
	tooLong := func() error {
		if lastErr != nil {
			return fmt.Errorf("the lock could not be renewed for %s: %w", limit, lastErr)
		}
		return fmt.Errorf("the lock could not be renewed for %s", limit)
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-exp.C():
			return tooLong()
		case <-ticks:
		}
		began := time.Now() // the renewal's stamp is set during the call, no later than its end
		rctx, cancel := context.WithCancel(ctx)
		res := make(chan error, 1)
		go func() { res <- r.Renew(rctx) }()
		var err error
		select {
		case err = <-res:
		case <-exp.C():
			cancel()
			return tooLong()
		case <-ctx.Done():
			cancel()
			return nil
		}
		cancel()
		switch {
		case err == nil:
			lastErr = nil
			// From when the write began, not from when the call returned: a
			// slow success must not push the stop past the moment a breaker
			// may act on the stamp it wrote.
			exp.Reset(limit - time.Since(began))
		case ctx.Err() != nil:
			return nil
		case errors.Is(err, lock.ErrLost):
			return err
		default:
			lastErr = err
		}
	}
}

// exitStatus is the shell's view of how a process ended: its exit code, or
// 128 plus the number of the signal that killed it.
func exitStatus(ps *os.ProcessState) int {
	if ws, ok := ps.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
		return 128 + int(ws.Signal())
	}
	return ps.ExitCode()
}

// lockRun acquires the lock, runs the command under it and releases it.
func lockRun(cfg lockConfig, argv []string, stdout, stderr io.Writer) int {
	sigs := make(chan os.Signal, 8)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sigs)

	// A signal while acquiring gives up the attempt; one that arrives just
	// as the attempt succeeds is acted on below.
	actx, cancel := context.WithCancel(context.Background())
	var early os.Signal
	watched := make(chan struct{})
	acquired := make(chan struct{})
	go func() {
		defer close(watched)
		select {
		case early = <-sigs:
			cancel()
		case <-acquired:
		}
	}()
	l, err := lock.Acquire(actx, cfg.dir, cfg.name, cfg.options())
	close(acquired)
	<-watched
	cancel()
	if err != nil {
		if early != nil {
			_, _ = fmt.Fprintf(stderr, "miami lock run: %v while acquiring the lock\n", early)
			return exitError
		}
		return lockResult("run", err, stderr)
	}
	reportBroke("run", l, stderr)
	if early != nil {
		_, _ = fmt.Fprintf(stderr, "miami lock run: %v while acquiring the lock\n", early)
		return releaseAfter(l, exitError, stderr)
	}

	// The command dies with this process: a command that outlives the lock's
	// renewals would run unprotected. The signal fires when the thread that
	// started the command exits, so the thread is kept for as long as the
	// command runs.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	cmd := exec.Command(argv[0], argv[1:]...) //nolint:gosec // running the operator's command is the purpose
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, stdout, stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL, Setpgid: true}
	if err := cmd.Start(); err != nil {
		_, _ = fmt.Fprintf(stderr, "miami lock run: starting the command: %v\n", err)
		return releaseAfter(l, exitError, stderr)
	}
	exited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(exited)
	}()

	rctx, stopRenewing := context.WithCancel(context.Background())
	defer stopRenewing()
	ticker := time.NewTicker(cfg.renew)
	defer ticker.Stop()
	renewed := make(chan error, 1)
	go func() {
		renewed <- renewLoop(rctx, l, ticker.C, newRealExpiry(), cfg.stale-2*cfg.timeout)
	}()

	for {
		select {
		case s := <-sigs:
			signalGroup(cmd, s.(syscall.Signal))
		case err := <-renewed:
			// Not ours any more: stop the command, and release nothing.
			_, _ = fmt.Fprintf(stderr, "miami lock run: %v; terminating the command\n", err)
			terminate(cmd, exited, cfg.timeout, stderr)
			return exitError
		case <-exited:
			stopRenewing()
			// A loss found as the command exited is reported; the release below
			// finds the lock lost too.
			if err := <-renewed; err != nil {
				_, _ = fmt.Fprintf(stderr, "miami lock run: %v\n", err)
			}
			return releaseAfter(l, exitStatus(cmd.ProcessState), stderr)
		}
	}
}

// releaseAfter releases the lock and returns code, or exitError if the
// release fails: a lock that was lost while the command ran means it did not
// have the protection it was run for.
func releaseAfter(l *lock.Lock, code int, stderr io.Writer) int {
	if err := l.Release(context.Background()); err != nil {
		_, _ = fmt.Fprintf(stderr, "miami lock run: releasing: %v\n", err)
		return exitError
	}
	return code
}

// terminate sends SIGTERM to the command, waits up to grace for it to exit
// and then kills it.
func terminate(cmd *exec.Cmd, exited <-chan struct{}, grace time.Duration, stderr io.Writer) {
	signalGroup(cmd, syscall.SIGTERM)
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case <-exited:
		return
	case <-timer.C:
	}
	_, _ = fmt.Fprintf(stderr, "miami lock run: the command did not stop within %s; killing it\n", grace)
	signalGroup(cmd, syscall.SIGKILL)
	select {
	case <-exited:
	case <-time.After(grace):
		_, _ = fmt.Fprintln(stderr, "miami lock run: the command did not exit after SIGKILL")
	}
}

// signalGroup sends sig to the command's process group, which the command
// leads (Setpgid), so that what it started is signalled with it.
func signalGroup(cmd *exec.Cmd, sig syscall.Signal) {
	_ = syscall.Kill(-cmd.Process.Pid, sig)
}
