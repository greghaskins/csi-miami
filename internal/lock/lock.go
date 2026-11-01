package lock

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/greghaskins/csi-miami/internal/deadline"
)

// Suffix is appended to the lock's name to name the lock directory:
// the lock "web" in directory D is the directory D/web.lock.
const Suffix = ".lock"

// Names of the two files inside a lock directory.
const (
	holderFile = "holder"
	sinceFile  = "since"
)

// Bounds and defaults of Options.
const (
	// MinStale is the smallest Options.Stale accepted. It is more than twice
	// the 30-second attribute cache of the nfs3 transport, so a stamp that
	// looks old because of the cache never looks stale.
	MinStale = 60 * time.Second
	// DefaultStale is the Options.Stale used when it is zero.
	DefaultStale = 5 * time.Minute
	// DefaultTimeout is the Options.Timeout used when it is zero.
	DefaultTimeout = 10 * time.Second
	// maxHolder bounds the holder string.
	maxHolder = 255
)

var (
	// ErrContended is returned by Acquire when the lock is held by someone
	// else and is not stale.
	ErrContended = errors.New("the lock is held by another holder")
	// ErrLost is returned by Renew and Release when the lock is not held by
	// this holder: it was released, or it went stale and was broken.
	ErrLost = errors.New("the lock is not held by this holder")
)

// Error is a failure of one operation on one lock. It carries the operation
// and the lock's name, never a path. ErrContended and ErrLost are returned
// inside an Error, so test for them with errors.Is.
type Error struct {
	Op   string // "acquire", "renew" or "release"
	Name string // the lock's name
	Err  error
}

func (e *Error) Error() string { return "lock " + e.Op + " " + e.Name + ": " + e.Err.Error() }

// Unwrap returns the cause.
func (e *Error) Unwrap() error { return e.Err }

// Options tune Acquire and Release. The zero value is valid.
type Options struct {
	// Holder identifies the caller; it is written to the lock and compared on
	// renewal and release. Default DefaultHolder(). At most 255 bytes, no
	// control characters.
	Holder string
	// Stale is how much older than the server's clock a lock's stamp may be
	// before the lock can be broken. At least MinStale; default DefaultStale.
	Stale time.Duration
	// Timeout bounds each file system call. Default DefaultTimeout.
	Timeout time.Duration
}

// DefaultHolder is "<hostname>:<pid>".
func DefaultHolder() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("%s:%d", host, os.Getpid())
}

// Lock is a lock this process holds. Renew and Release are safe to call
// from different goroutines; they run one at a time.
type Lock struct {
	e    *env
	mu   sync.Mutex
	done bool // released, or found lost
}

// Name is the lock's name.
func (l *Lock) Name() string { return l.e.name }

// Holder is the holder string written to the lock.
func (l *Lock) Holder() string { return l.e.holder }

// env is one operation's view of one lock.
type env struct {
	fs      fsys
	set     deadline.Set
	dir     string
	name    string
	lockDir string
	holder  string
	stale   time.Duration
	timeout time.Duration
}

func newEnv(fsys fsys, dir, name string, opts Options) (*env, error) {
	switch {
	case dir == "":
		return nil, errors.New("the directory is empty")
	case name == "" || name == "." || name == "..":
		return nil, fmt.Errorf("the lock name %q is not a name", name)
	case strings.ContainsAny(name, "/\x00"):
		return nil, fmt.Errorf("the lock name %q contains a path separator or NUL", name)
	case len(name) > 200:
		return nil, errors.New("the lock name is longer than 200 bytes")
	}
	if opts.Holder == "" {
		opts.Holder = DefaultHolder()
	}
	if len(opts.Holder) > maxHolder {
		return nil, fmt.Errorf("the holder is longer than %d bytes", maxHolder)
	}
	for _, r := range opts.Holder {
		if r < 0x20 || r == 0x7f {
			return nil, errors.New("the holder contains a control character")
		}
	}
	if opts.Stale == 0 {
		opts.Stale = DefaultStale
	}
	if opts.Stale < MinStale {
		return nil, fmt.Errorf("the stale period %s is below the minimum %s", opts.Stale, MinStale)
	}
	if opts.Timeout == 0 {
		opts.Timeout = DefaultTimeout
	}
	if opts.Timeout < 0 {
		return nil, errors.New("the timeout is negative")
	}
	return &env{
		fs: fsys, dir: dir, name: name, lockDir: filepath.Join(dir, name+Suffix),
		holder: opts.Holder, stale: opts.Stale, timeout: opts.Timeout,
	}, nil
}

// fail wraps err for the caller. Paths are stripped by step already.
func (e *env) fail(op string, err error) error { return &Error{Op: op, Name: e.name, Err: err} }

// clean drops the path from a file system error, keeping its cause.
func clean(err error) error {
	var pe *fs.PathError
	if errors.As(err, &pe) {
		return pe.Err
	}
	var le *os.LinkError
	if errors.As(err, &le) {
		return le.Err
	}
	return err
}

// step runs one file system call under the deadline. what names the call in
// the error and never contains a path.
func (e *env) step(ctx context.Context, what string, fn func() error) error {
	_, err := deadline.Run(ctx, e.timeout, what, &e.set, e.dir, func(context.Context) (struct{}, error) {
		if err := fn(); err != nil {
			return struct{}{}, fmt.Errorf("%s: %w", what, clean(err))
		}
		return struct{}{}, nil
	})
	return err
}

// stepValue is step for a call that returns a value.
func stepValue[T any](ctx context.Context, e *env, what string, fn func() (T, error)) (T, error) {
	return deadline.Run(ctx, e.timeout, what, &e.set, e.dir, func(context.Context) (T, error) {
		v, err := fn()
		if err != nil {
			var zero T
			return zero, fmt.Errorf("%s: %w", what, clean(err))
		}
		return v, nil
	})
}

// Acquire takes the lock name in dir with one attempt: it returns the held
// lock, ErrContended (inside an *Error) if someone else holds it and it is
// not stale, or another error. A stale lock is broken and taken.
func Acquire(ctx context.Context, dir, name string, opts Options) (*Lock, error) {
	return acquire(ctx, osFS{}, dir, name, opts)
}

func acquire(ctx context.Context, fsys fsys, dir, name string, opts Options) (*Lock, error) {
	e, err := newEnv(fsys, dir, name, opts)
	if err != nil {
		return nil, &Error{Op: "acquire", Name: name, Err: err}
	}
	created, err := e.mkdir(ctx)
	if err != nil {
		return nil, e.fail("acquire", err)
	}
	if !created {
		retry, err := e.breakIfStale(ctx)
		if err != nil {
			return nil, e.fail("acquire", err)
		}
		if !retry {
			return nil, e.fail("acquire", ErrContended)
		}
		// One more attempt, whether we broke it, lost the break to another
		// acquirer or found the lock gone; whoever got there first keeps it.
		if created, err = e.mkdir(ctx); err != nil {
			return nil, e.fail("acquire", err)
		}
		if !created {
			return nil, e.fail("acquire", ErrContended)
		}
	}
	if err := e.publish(ctx); err != nil {
		return nil, e.fail("acquire", err)
	}
	return &Lock{e: e}, nil
}

// mkdir reports whether it made the lock directory; false means it exists.
func (e *env) mkdir(ctx context.Context) (bool, error) {
	err := e.step(ctx, "mkdir", func() error { return e.fs.Mkdir(e.lockDir) })
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, fs.ErrExist):
		return false, nil
	}
	return false, err
}

// publish writes the holder and the first stamp into the lock directory we
// just made. On failure it removes the directory so that no half-made lock
// waits out Stale.
func (e *env) publish(ctx context.Context) error {
	err := e.step(ctx, "write holder", func() error { return e.fs.WriteAtomic(e.lockDir, holderFile, []byte(e.holder)) })
	if err == nil {
		err = e.step(ctx, "write since", func() error { return e.fs.WriteAtomic(e.lockDir, sinceFile, nil) })
	}
	if err != nil {
		_ = e.step(context.WithoutCancel(ctx), "remove", func() error { return e.fs.RemoveAll(e.lockDir) })
	}
	return err
}

// observation is what an acquirer saw in a lock directory.
type observation struct {
	holder   string
	stamp    time.Time // since's modification time, or the directory's if there is no since
	hasSince bool
}

// same reports whether b is the lock a saw: same holder, same stamp.
func (a observation) same(b observation) bool {
	return a.holder == b.holder && a.hasSince == b.hasSince && (!a.hasSince || a.stamp.Equal(b.stamp))
}

// isStale is the comparison: the stamp is older than the server's now by
// more than after. Both times are the server's; a stamp newer than now
// (attribute skew) is not stale.
func isStale(now, stamp time.Time, after time.Duration) bool {
	return now.Sub(stamp) > after
}

// observe reads the holder and the stamp of the lock directory at dir.
// found is false if the directory does not exist.
func (e *env) observe(ctx context.Context, dir string) (obs observation, found bool, err error) {
	obs.stamp, err = stepValue(ctx, e, "stat since", func() (time.Time, error) { return e.fs.ModTime(filepath.Join(dir, sinceFile)) })
	switch {
	case err == nil:
		obs.hasSince = true
	case errors.Is(err, fs.ErrNotExist):
		// The holder died between mkdir and its first write, or the lock is
		// gone. The directory's own stamp tells which.
		obs.stamp, err = stepValue(ctx, e, "stat lock", func() (time.Time, error) { return e.fs.ModTime(dir) })
		if errors.Is(err, fs.ErrNotExist) {
			return observation{}, false, nil
		}
		if err != nil {
			return observation{}, false, err
		}
	default:
		return observation{}, false, err
	}
	raw, err := stepValue(ctx, e, "read holder", func() ([]byte, error) { return e.fs.ReadFile(filepath.Join(dir, holderFile)) })
	switch {
	case err == nil:
		obs.holder = string(raw)
	case errors.Is(err, fs.ErrNotExist):
	default:
		return observation{}, false, err
	}
	return obs, true, nil
}

// breakIfStale looks at the contended lock and breaks it if it is stale. It
// reports whether the caller should try to make the directory again.
func (e *env) breakIfStale(ctx context.Context) (retry bool, err error) {
	// The server's now is read first: a renewal that lands after it makes the
	// stamp newer than now, which is never stale.
	now, err := stepValue(ctx, e, "probe", func() (time.Time, error) { return e.fs.Probe(e.dir, e.name) })
	if err != nil {
		return false, err
	}
	seen, found, err := e.observe(ctx, e.lockDir)
	if err != nil {
		return false, err
	}
	if !found {
		return true, nil
	}
	if !isStale(now, seen.stamp, e.stale) {
		return false, nil
	}

	broken := filepath.Join(e.dir, "."+e.name+".broken-"+randomSuffix())
	if err := e.step(ctx, "rename", func() error { return e.fs.Rename(e.lockDir, broken) }); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return true, nil // another acquirer broke it, or the holder released
		}
		return false, err
	}

	// Between the judgement and the rename the lock may have been renewed, or
	// broken and taken by someone else; what we moved must be what we judged.
	moved, found, err := e.observe(ctx, broken)
	if err == nil && (!found || !seen.same(moved)) {
		err = errors.New("the lock changed while it was being broken")
	}
	if err != nil {
		restore := e.step(context.WithoutCancel(ctx), "restore", func() error { return e.fs.Rename(broken, e.lockDir) })
		if restore != nil {
			return false, fmt.Errorf("%w (and putting it back failed: %w)", err, restore)
		}
		if found {
			return false, nil // it was alive after all: contended
		}
		return false, err
	}
	if err := e.step(ctx, "remove broken lock", func() error { return e.fs.RemoveAll(broken) }); err != nil {
		return false, err
	}
	return true, nil
}

// held checks that the lock is still ours.
func (e *env) held(ctx context.Context) error {
	raw, err := stepValue(ctx, e, "read holder", func() ([]byte, error) { return e.fs.ReadFile(filepath.Join(e.lockDir, holderFile)) })
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return ErrLost
	case err != nil:
		return err
	case string(raw) != e.holder:
		return ErrLost
	}
	return nil
}

// Renew stamps the lock again: it rewrites since, and the server sets its
// modification time. It returns ErrLost (inside an *Error) if the lock is
// no longer ours, and after that every call does.
func (l *Lock) Renew(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	e := l.e
	if l.done {
		return e.fail("renew", ErrLost)
	}
	if err := e.held(ctx); err != nil {
		l.done = errors.Is(err, ErrLost)
		return e.fail("renew", err)
	}
	err := e.step(ctx, "write since", func() error { return e.fs.WriteAtomic(e.lockDir, sinceFile, nil) })
	if errors.Is(err, fs.ErrNotExist) {
		// The directory was moved away between the check and the write.
		l.done = true
		return e.fail("renew", ErrLost)
	}
	if err != nil {
		return e.fail("renew", err)
	}
	return nil
}

// Release gives the lock up: it removes holder, since and the directory. It
// returns ErrLost (inside an *Error) and removes nothing if the lock is not
// ours, so a holder that was broken out cannot take the new holder's lock
// away.
func (l *Lock) Release(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.done {
		return l.e.fail("release", ErrLost)
	}
	err := l.e.release(ctx)
	if err == nil || errors.Is(err, ErrLost) {
		l.done = true
	}
	if err != nil {
		return l.e.fail("release", err)
	}
	return nil
}

// Release releases the lock name in dir if opts.Holder holds it, without a
// Lock: for a process that is not the one that acquired it. It returns
// ErrLost (inside an *Error) if the lock does not exist or is held by another
// holder.
func Release(ctx context.Context, dir, name string, opts Options) error {
	return releaseFS(ctx, osFS{}, dir, name, opts)
}

func releaseFS(ctx context.Context, fsys fsys, dir, name string, opts Options) error {
	e, err := newEnv(fsys, dir, name, opts)
	if err != nil {
		return &Error{Op: "release", Name: name, Err: err}
	}
	if err := e.release(ctx); err != nil {
		return e.fail("release", err)
	}
	return nil
}

func (e *env) release(ctx context.Context) error {
	if err := e.held(ctx); err != nil {
		return err
	}
	for _, f := range []string{holderFile, sinceFile} {
		err := e.step(ctx, "remove "+f, func() error { return e.fs.Remove(filepath.Join(e.lockDir, f)) })
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	err := e.step(ctx, "remove lock", func() error { return e.fs.Remove(e.lockDir) })
	if errors.Is(err, fs.ErrNotExist) {
		return ErrLost
	}
	return err
}
