package lock

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/deadline"
	"github.com/greghaskins/csi-miami/internal/mount"
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
	// MinStale is the smallest Options.Stale accepted. A holder that cannot
	// renew needs a renewal period, a hung call's deadline, the time its work
	// takes to stop and the latency of its last stamp before the lock may be
	// broken; 60 seconds leaves room for that with the default timeout. The
	// 30-second attribute cache of the nfs3 transport is the secondary reason.
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
	// ErrNotALock is returned by Acquire when the entry at the lock's path is
	// not a lock directory, or the directory holds something the protocol
	// never writes. The entry is reported and left alone: a person has to look
	// at it. The error names what was found.
	ErrNotALock = errors.New("the lock path holds something that is not a lock")
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
	dir  dirHandle // the lock directory, pinned when it was made
	mu   sync.Mutex
	done bool // released, or found lost; dir is closed
	// broke is why a stale lock was broken to take this one; empty if the
	// lock was free.
	broke string
}

// Broke is the reason the stale lock that was in the way was broken to take
// this one, such as "the stamp is 6m0s old" or "the stamp is 1h0m0s ahead of
// the server's clock"; empty if the lock was free.
func (l *Lock) Broke() string { return l.broke }

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

	// hung is set once a call has been abandoned at its deadline. Such a call
	// may still use the pinned directory's descriptor, so the descriptor is
	// then never closed: a closed number can be reused, and the late call would
	// act on whatever got it.
	hung atomic.Bool
}

// closeDir closes a pinned directory unless a call may still be using it.
func (e *env) closeDir(h dirHandle) {
	if !e.hung.Load() {
		_ = h.Close()
	}
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
	_, err := stepValue(ctx, e, what, func() (struct{}, error) { return struct{}{}, fn() })
	return err
}

// cleanupStep is step for the calls that put things right after an earlier
// call (restore a moved directory, leave the breakers' directory, remove what
// a reclaim moved). The deadline set refuses a second call on a key while an
// abandoned one is outstanding, so these run under a key of their own: an
// observation that hung must not strand a possibly live lock under a hidden
// name.
func (e *env) cleanupStep(ctx context.Context, what string, fn func() error) error {
	_, err := stepKeyed(ctx, e, e.dir+"\x00cleanup", what, func() (struct{}, error) { return struct{}{}, fn() })
	return err
}

// lookKey is the deadline key of the looks that decide a cleanup.
func (e *env) lookKey() string { return e.dir + "\x00look" }

// stepValue is step for a call that returns a value.
func stepValue[T any](ctx context.Context, e *env, what string, fn func() (T, error)) (T, error) {
	return stepKeyed(ctx, e, e.dir, what, fn)
}

func stepKeyed[T any](ctx context.Context, e *env, key, what string, fn func() (T, error)) (T, error) {
	v, err := deadline.Run(ctx, e.timeout, what, &e.set, key, func(context.Context) (T, error) {
		v, err := fn()
		if err != nil {
			var zero T
			return zero, fmt.Errorf("%s: %w", what, clean(err))
		}
		return v, nil
	})
	if errors.Is(err, mount.ErrTimeout) || errors.Is(err, mount.ErrBusy) {
		e.hung.Store(true)
	}
	return v, err
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
	broke := ""
	if !created {
		retry, reason, err := e.breakIfStale(ctx)
		if err != nil {
			return nil, e.fail("acquire", err)
		}
		if !retry {
			return nil, e.fail("acquire", ErrContended)
		}
		broke = reason
		// One more attempt, whether we broke it, lost the break to another
		// acquirer or found the lock gone; whoever got there first keeps it.
		if created, err = e.mkdir(ctx); err != nil {
			return nil, e.fail("acquire", err)
		}
		if !created {
			return nil, e.fail("acquire", ErrContended)
		}
	}
	// Pin the directory we made. Everything after this works relative to the
	// handle, so a lock that is broken and taken by someone else under the same
	// path is never written to or removed by us.
	h, err := stepValue(ctx, e, "open lock", func() (dirHandle, error) { return e.fs.OpenDir(e.lockDir) })
	if err != nil {
		// A directory that is gone was moved by a breaker (or removed); any
		// other failure leaves an empty lock directory, which goes stale like
		// any other. It is not removed by path: the path may not be ours now.
		return nil, e.fail("acquire", err)
	}
	if err := e.publish(ctx, h); err != nil {
		e.closeDir(h)
		return nil, e.fail("acquire", err)
	}
	return &Lock{e: e, dir: h, broke: broke}, nil
}

// mkdir reports whether it made the lock directory; false means it exists.
func (e *env) mkdir(ctx context.Context) (bool, error) { return e.mkdirAt(ctx, e.lockDir) }

func (e *env) mkdirAt(ctx context.Context, path string) (bool, error) {
	err := e.step(ctx, "mkdir", func() error { return e.fs.Mkdir(path) })
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, fs.ErrExist):
		return false, nil
	}
	return false, err
}

// publish writes the holder and the first stamp into the lock directory we
// just made, through its handle: a breaker that moves the directory while it is
// half made (it judged the previous lock stale and was slow) does not lose
// the writes, which go with the directory and are in place when it puts it
// back. On failure it removes the directory so that no half-made lock waits
// out Stale.
func (e *env) publish(ctx context.Context, h dirHandle) error {
	err := e.step(ctx, "write holder", func() error { return h.WriteAtomic(holderFile, []byte(e.holder)) })
	if err == nil {
		err = e.step(ctx, "write since", func() error { return h.WriteAtomic(sinceFile, nil) })
	}
	if err != nil {
		_ = e.removeDir(context.WithoutCancel(ctx), h)
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

// maxSkew is how far ahead of the server's now a stamp may be before it is
// taken for corrupt. A renewal that lands between the probe and the
// observation is ahead by a round trip; the server's clock stepped backwards,
// or a stamp set into the future (utimensat), is ahead by the step or the
// lie. Such a stamp would keep the lock unbreakable for that long, so beyond
// maxSkew it is stale, and a live holder's next renewal restamps it with the
// server's clock.
const maxSkew = 30 * time.Second

// staleness is the comparison, on the server's times only: the stamp is older
// than now by more than after, or ahead of it by more than maxSkew. reason
// says which.
func staleness(now, stamp time.Time, after time.Duration) (stale bool, reason string) {
	return stalenessAt(now, now, stamp, after)
}

// stalenessAt is staleness with two probes: now was taken before the stamp
// was read and later after. A stamp written before it was read cannot lead a
// probe taken after the read, however slow the calls in between were, so the
// "ahead" rule uses later; the age rule uses the earlier now, which can only
// make a lock look younger.
func stalenessAt(now, later, stamp time.Time, after time.Duration) (stale bool, reason string) {
	if ahead := stamp.Sub(later); ahead > maxSkew {
		return true, fmt.Sprintf("the stamp is %s ahead of the server's clock", ahead.Round(time.Second))
	}
	if age := now.Sub(stamp); age > after {
		return true, fmt.Sprintf("the stamp is %s old", age.Round(time.Second))
	}
	return false, ""
}

func isStale(now, stamp time.Time, after time.Duration) bool {
	stale, _ := staleness(now, stamp, after)
	return stale
}

// protocolName reports whether name is a file the protocol writes inside a
// lock directory: holder, since, the temporary name WriteAtomic gives either
// one while it writes it, or a silly-renamed .nfs file.
func protocolName(name string) bool {
	if name == holderFile || name == sinceFile {
		return true
	}
	// On NFSv3 a rename over a file another client has open leaves the old
	// file under a silly-rename name for as long as it is open: .nfs and hex
	// digits.
	if rest, ok := strings.CutPrefix(name, ".nfs"); ok && rest != "" && strings.Trim(rest, "0123456789abcdefABCDEF") == "" {
		return true
	}
	for _, f := range []string{holderFile, sinceFile} {
		if rest, ok := strings.CutPrefix(name, "."+f+".tmp-"); ok && len(rest) == 16 && strings.Trim(rest, "0123456789abcdef") == "" {
			return true
		}
	}
	return false
}

// maxEntries is the most a lock directory holds: holder, since, a temporary
// file for each, and a few silly-renamed files. More than that is not a lock,
// and listing it is not worth the time.
const maxEntries = 9

// notALock is ErrNotALock with what was found.
func notALock(found string) error { return fmt.Errorf("%w: %s", ErrNotALock, found) }

// checkShape returns ErrNotALock unless the entry at path is a lock-shaped
// directory: a real directory (not a symbolic link) that holds regular files
// the protocol writes and nothing else. An empty one is lock-shaped, it is a
// holder that died before its first write. Anything else may be somebody's
// data and is never broken. A directory that is gone answers fs.ErrNotExist.
func (e *env) checkShape(ctx context.Context, path string) error {
	mode, err := stepValue(ctx, e, "stat lock", func() (fs.FileMode, error) { return e.fs.Lstat(path) })
	if err != nil {
		return err
	}
	if !mode.IsDir() {
		return notALock("it is " + describe(mode))
	}
	ents, err := stepValue(ctx, e, "list lock", func() ([]fs.DirEntry, error) { return e.fs.ReadDir(path, maxEntries+1) })
	if err != nil {
		return err
	}
	if len(ents) > maxEntries {
		return notALock("the directory holds more entries than a lock does")
	}
	var odd []string
	for _, ent := range ents {
		if !protocolName(ent.Name()) || !ent.Type().IsRegular() {
			odd = append(odd, strconv.Quote(ent.Name()))
		}
	}
	if len(odd) > 0 {
		if len(odd) > 5 {
			odd = append(odd[:5], "...")
		}
		return notALock("the directory holds entries the protocol never writes: " + strings.Join(odd, ", "))
	}
	return nil
}

// describe says what kind of entry mode is.
func describe(mode fs.FileMode) string {
	switch {
	case mode&fs.ModeSymlink != 0:
		return "a symbolic link"
	case mode.IsRegular():
		return "a regular file"
	case mode.IsDir():
		return "a directory"
	}
	return "a special file"
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

// verdict is one look at a contended lock.
type verdict struct {
	now    time.Time // the server's now, read before the lock was
	later  time.Time // and read again after it
	seen   observation
	found  bool // false: the lock is gone
	stale  bool
	reason string // why it is stale
}

// judge looks at the lock at its path: the server's now first (a renewal that
// lands after it makes the stamp newer than now, which is not stale), then
// whether the entry is a lock at all, then its holder and stamp. It returns
// ErrNotALock for anything that is not lock-shaped.
func (e *env) judge(ctx context.Context) (v verdict, err error) {
	v.now, err = stepValue(ctx, e, "probe", func() (time.Time, error) { return e.fs.Probe(e.dir, e.name) })
	if err != nil {
		return v, err
	}
	if err = e.checkShape(ctx, e.lockDir); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return v, nil
		}
		return v, err
	}
	v.seen, v.found, err = e.observe(ctx, e.lockDir)
	if err != nil || !v.found {
		return v, err
	}
	// Probe again now that the stamp is read: see stalenessAt.
	v.later, err = stepValue(ctx, e, "probe", func() (time.Time, error) { return e.fs.Probe(e.dir, e.name) })
	if err != nil {
		return v, err
	}
	v.stale, v.reason = stalenessAt(v.now, v.later, v.seen.stamp, e.stale)
	return v, nil
}

// breakIfStale looks at the contended lock and breaks it if it is stale. It
// reports whether the caller should try to make the directory again, and why
// it broke the lock if it did.
//
// Only lock-shaped directories are ever broken (see checkShape). Breakers are
// serialised by a directory of their own, and each judges the lock again once
// it holds that directory: a breaker that stalled between judging and
// renaming, for as long as another breaker took to break the lock and take
// it, would otherwise move that live lock, and putting it back races with the
// next acquirer. With the serialisation it judges the live lock fresh, finds
// it not stale and leaves it. What remains is a holder renewing in the
// instant between the second judgement and the rename; the directory is then
// put back (see the doc comment of the package for what that can cost).
func (e *env) breakIfStale(ctx context.Context) (retry bool, reason string, err error) {
	v, err := e.judge(ctx)
	if err != nil {
		return false, "", err
	}
	if !v.found {
		return true, "", nil
	}
	if !v.stale {
		return false, "", nil
	}

	unlock, ok, err := e.enterBreakers(ctx)
	if err != nil || !ok {
		return false, "", err // not ok: another breaker is at work, the lock is contended
	}
	defer func() {
		if uerr := unlock(); uerr != nil {
			err = errors.Join(err, fmt.Errorf("leaving the breakers' directory: %w", uerr))
		}
	}()
	if v, err = e.judge(ctx); err != nil {
		return false, "", err
	}
	if !v.found {
		return true, "", nil
	}
	if !v.stale {
		return false, "", nil
	}

	broken := filepath.Join(e.dir, "."+e.name+".broken-"+randomSuffix())
	if err := e.step(ctx, "rename", func() error { return e.fs.Rename(e.lockDir, broken) }); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return true, "", nil // the holder released
		}
		return false, "", err
	}

	// Between the judgement and the rename the lock may have been renewed, or
	// changed in some other way; what we moved must be what we judged, and a
	// lock. Whatever goes wrong from here, the directory is put back: these
	// steps run under their own deadline key so that a look that hung does not
	// refuse the restore.
	alive, err := e.checkMoved(ctx, broken, v.seen)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, "", nil // gone: someone else moved it; nothing to put back
		}
		restore := e.cleanupStep(context.WithoutCancel(ctx), "restore", func() error { return e.fs.Rename(broken, e.lockDir) })
		if restore != nil {
			return false, "", fmt.Errorf("%w (and putting it back failed: %w)", err, restore)
		}
		if alive {
			return false, "", nil // it was alive after all: contended
		}
		return false, "", err
	}
	if err := e.step(ctx, "remove broken lock", func() error { return e.fs.RemoveAll(broken) }); err != nil {
		// A breaker running as another user than the holder can rename the
		// directory but not empty it. The lock is broken all the same: what is
		// left is a hidden directory nothing reads, so the attempt goes on.
		if !errors.Is(err, unix.EACCES) && !errors.Is(err, unix.EPERM) {
			return false, "", err
		}
	}
	return true, v.reason, nil
}

// checkMoved checks that the directory the breaker moved is a lock and is the
// one it judged. alive is true if it is a lock that was renewed meanwhile. An
// error wrapping fs.ErrNotExist means the directory is gone from where it was
// moved to.
func (e *env) checkMoved(ctx context.Context, broken string, judged observation) (alive bool, err error) {
	if err = e.checkShape(ctx, broken); err != nil {
		return false, err
	}
	moved, found, err := e.observe(ctx, broken)
	switch {
	case errors.Is(err, unix.ENOTDIR):
		return false, notALock("what was moved is not a directory")
	case errors.Is(err, unix.ENOENT):
		return false, fmt.Errorf("the moved lock is gone: %w", fs.ErrNotExist)
	case err != nil:
		return false, err
	case !found:
		return false, fmt.Errorf("the moved lock is gone: %w", fs.ErrNotExist)
	case !judged.same(moved):
		return true, errors.New("the lock changed while it was being broken")
	}
	return false, nil
}

// enterBreakers takes the directory that serialises breakers of this lock. ok
// is false if another breaker holds it. A breaker that died leaves it behind:
// one older than the stale period on the server's clock is reclaimed, by
// renaming it away so that two reclaimers cannot both remove it, and looking
// at what was moved: a directory made live in the meantime is put back. It is
// a directory the protocol makes empty; anything else under that name is left
// alone and reported.
//
// The directory is judged against a probe taken after its stamp was read, not
// the one the lock was judged by: a breaker that stalled since then would
// otherwise find a live breaker's directory "ahead" of its old probe.
//
// unlock removes the directory only if it is still the one this breaker made:
// a breaker stalled for more than Stale and reclaimed must not remove the
// directory of the one that took its place.
func (e *env) enterBreakers(ctx context.Context) (unlock func() error, ok bool, err error) {
	mutex := filepath.Join(e.dir, "."+e.name+".breaking")
	created, err := e.mkdirAt(ctx, mutex)
	if err != nil {
		return nil, false, err
	}
	if !created {
		reclaimed, err := e.reclaimBreakers(ctx, mutex)
		if err != nil || !reclaimed {
			return nil, false, err
		}
		if created, err = e.mkdirAt(ctx, mutex); err != nil || !created {
			return nil, false, err
		}
	}
	h, err := stepValue(ctx, e, "open breakers", func() (dirHandle, error) { return e.fs.OpenDir(mutex) })
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, false, nil // reclaimed under us: not ours
		}
		return nil, false, err
	}
	unlock = func() error {
		err := e.removeAt(context.WithoutCancel(ctx), h, mutex, ".breaking-dead-", true, nil)
		e.closeDir(h)
		if errors.Is(err, ErrLost) {
			err = nil // not ours any more: reclaimed while we stalled
		}
		return err
	}
	return unlock, true, nil
}

// reclaimBreakers removes a breakers' directory left by a dead breaker. It
// reports whether the path is now free to make.
func (e *env) reclaimBreakers(ctx context.Context, mutex string) (bool, error) {
	if err := e.checkEmptyDir(ctx, mutex); err != nil {
		return errors.Is(err, fs.ErrNotExist), noExist(err)
	}
	stamp, err := stepValue(ctx, e, "stat breakers", func() (time.Time, error) { return e.fs.ModTime(mutex) })
	if err != nil {
		return errors.Is(err, fs.ErrNotExist), noExist(err)
	}
	now, err := stepValue(ctx, e, "probe", func() (time.Time, error) { return e.fs.Probe(e.dir, e.name) })
	if err != nil {
		return false, err
	}
	if stale, _ := staleness(now, stamp, e.stale); !stale {
		return false, nil
	}
	junk := filepath.Join(e.dir, "."+e.name+".breaking-dead-"+randomSuffix())
	if err = e.step(ctx, "rename", func() error { return e.fs.Rename(mutex, junk) }); err != nil {
		return errors.Is(err, fs.ErrNotExist), noExist(err)
	}
	// What was moved may not be what was judged: a breaker that finished and
	// another that made the directory anew, between the stat and the rename.
	ctx = context.WithoutCancel(ctx)
	// The looks run under a key of their own and only the steps that put
	// things right under the cleanup key: a look that hangs leaves its key
	// busy, and the restore must not wait behind it.
	stamp, err = stepKeyed(ctx, e, e.lookKey(), "stat breakers", func() (time.Time, error) { return e.fs.ModTime(junk) })
	if err == nil {
		now, err = stepKeyed(ctx, e, e.lookKey(), "probe", func() (time.Time, error) { return e.fs.Probe(e.dir, e.name) })
	}
	if stale, _ := staleness(now, stamp, e.stale); err != nil || !stale {
		back := e.cleanupStep(ctx, "restore breakers", func() error { return e.fs.Rename(junk, mutex) })
		return false, errors.Join(err, back)
	}
	if err = e.cleanupStep(ctx, "remove dead breakers", func() error { return e.fs.RemoveAll(junk) }); err != nil {
		return false, err
	}
	return true, nil
}

// noExist turns "does not exist" into no error: the caller reports it as a
// path that is free.
func noExist(err error) error {
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

// checkEmptyDir returns ErrNotALock unless path is an empty directory.
func (e *env) checkEmptyDir(ctx context.Context, path string) error {
	mode, err := stepValue(ctx, e, "stat breakers", func() (fs.FileMode, error) { return e.fs.Lstat(path) })
	if err != nil {
		return err
	}
	if !mode.IsDir() {
		return notALock("the breakers' directory is " + describe(mode))
	}
	ents, err := stepValue(ctx, e, "list breakers", func() ([]fs.DirEntry, error) { return e.fs.ReadDir(path, 1) })
	if err != nil {
		return err
	}
	if len(ents) > 0 {
		return notALock("the breakers' directory is not empty")
	}
	return nil
}

// held checks that the directory we pinned still says it is ours. It reads
// through the handle, so it answers for our directory wherever it now is; that
// it is still at the lock's path is checked by whoever acts on it.
func (l *Lock) held(ctx context.Context) error {
	e := l.e
	raw, err := stepValue(ctx, e, "read holder", func() ([]byte, error) { return l.dir.ReadFile(holderFile) })
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

// atPath reports whether the lock's path still names the directory we pinned.
func (l *Lock) atPath(ctx context.Context) error {
	same, err := stepValue(ctx, l.e, "check lock", func() (bool, error) { return l.dir.Same(l.e.lockDir) })
	if err != nil {
		return err
	}
	if !same {
		return ErrLost
	}
	return nil
}

// finish marks the lock finished and closes the handle. l.mu is held.
func (l *Lock) finish() {
	if !l.done {
		l.done = true
		l.e.closeDir(l.dir)
	}
}

// Renew stamps the lock again: it rewrites since inside the pinned
// directory, and the server sets its modification time. It returns ErrLost
// (inside an *Error) if the lock is no longer ours, and after that every call
// does. A stamp written in the instant the lock was broken lands in the
// directory that was moved away, never in the new holder's.
func (l *Lock) Renew(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	e := l.e
	if l.done {
		return e.fail("renew", ErrLost)
	}
	err := l.held(ctx)
	if err == nil {
		err = e.step(ctx, "write since", func() error { return l.dir.WriteAtomic(sinceFile, nil) })
	}
	if errors.Is(err, fs.ErrNotExist) {
		err = ErrLost // the directory was removed
	}
	if err == nil {
		err = l.atPath(ctx)
	}
	if errors.Is(err, ErrLost) {
		l.finish()
	}
	if err != nil {
		return e.fail("renew", err)
	}
	return nil
}

// Release gives the lock up: the directory is renamed away, checked to be
// the one we pinned, and removed with what is in it. It returns ErrLost
// (inside an *Error) and removes nothing if the lock is not ours, so a holder
// that was broken out cannot take the new holder's lock away.
func (l *Lock) Release(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.done {
		return l.e.fail("release", ErrLost)
	}
	err := l.held(ctx)
	if err == nil {
		err = l.e.removeDir(ctx, l.dir)
	}
	if err == nil || errors.Is(err, ErrLost) {
		l.finish()
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
	h, err := stepValue(ctx, e, "open lock", func() (dirHandle, error) { return e.fs.OpenDir(e.lockDir) })
	if errors.Is(err, fs.ErrNotExist) {
		return e.fail("release", ErrLost)
	}
	if errors.Is(err, unix.ENOTDIR) || errors.Is(err, unix.ELOOP) {
		return e.fail("release", notALock("the lock path is not a directory"))
	}
	if err != nil {
		return e.fail("release", err)
	}
	l := &Lock{e: e, dir: h}
	defer l.finish()
	err = l.held(ctx)
	if err == nil {
		err = e.removeDir(ctx, h)
	}
	if err != nil {
		return e.fail("release", err)
	}
	return nil
}

// removeDir removes the lock directory we pinned: it renames the directory at
// the lock's path to a unique hidden name (atomic, and it frees the name at
// once), checks that what it moved is the directory it pinned by device and
// inode and is lock-shaped, and removes it with whatever is in it, such as a
// stamp an abandoned call wrote late. If it moved someone else's directory
// (the lock was broken and taken between our last look and the rename) it puts
// it back and reports ErrLost; if it is not lock-shaped, ErrNotALock.
func (e *env) removeDir(ctx context.Context, h dirHandle) error {
	return e.removeAt(ctx, h, e.lockDir, ".removed-", false, e.checkShape)
}

// removeAt is removeDir for any directory we pinned, at path. tag names the
// hidden directory it is moved to. verify, if not nil, is run on the moved
// directory. cleanup runs the steps under the cleanup deadline key.
func (e *env) removeAt(ctx context.Context, h dirHandle, path, tag string, cleanup bool, verify func(context.Context, string) error) error {
	step := e.step
	if cleanup {
		step = e.cleanupStep
	}
	junk := filepath.Join(e.dir, "."+e.name+tag+randomSuffix())
	if err := step(ctx, "rename", func() error { return e.fs.Rename(path, junk) }); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return ErrLost
		}
		return err
	}
	// The look never shares a key with the restore that follows it, so that a
	// look that hangs cannot refuse it. Cleanup's look has a key of its own, so
	// that an earlier hung call does not refuse it either.
	key := e.dir
	if cleanup {
		key = e.lookKey()
	}
	same, err := stepKeyed(ctx, e, key, "check lock", func() (bool, error) { return h.Same(junk) })
	if err == nil && same && verify != nil {
		if verr := verify(ctx, junk); verr != nil {
			if !errors.Is(verr, ErrNotALock) {
				err = verr
			} else {
				err, same = verr, true
			}
		}
	}
	if err != nil || !same {
		restore := e.cleanupStep(context.WithoutCancel(ctx), "restore", func() error { return e.fs.Rename(junk, path) })
		switch {
		case err != nil && restore != nil:
			return fmt.Errorf("%w (and putting the lock back failed: %w)", err, restore)
		case err != nil:
			return err
		case restore != nil && !errors.Is(restore, fs.ErrNotExist):
			return fmt.Errorf("the lock changed while it was being removed and putting it back failed: %w", restore)
		}
		return ErrLost
	}
	return step(ctx, "remove lock", func() error { return e.fs.RemoveAll(junk) })
}
