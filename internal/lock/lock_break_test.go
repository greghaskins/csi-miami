package lock

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/mount"
)

const testStale = 2 * time.Minute

// staleLock makes a lock held by "dead" and a fake whose clock says it is
// stale.
func staleLock(t *testing.T) (dir string, f *fakeFS) {
	t.Helper()
	dir = t.TempDir()
	if _, err := Acquire(context.Background(), dir, "web", Options{Holder: "dead"}); err != nil {
		t.Fatal(err)
	}
	f = newFake(t)
	f.since = serverNow.Add(-2 * testStale)
	return dir, f
}

func opts(holder string) Options {
	return Options{Holder: holder, Stale: testStale, Timeout: time.Minute}
}

// Two acquirers judge the same lock stale; one wins the rename and the other
// gets ENOENT. The loser tries to make the directory once and finds it taken.
func TestBreakerThatLosesTheRenameReportsContended(t *testing.T) {
	t.Parallel()
	dir, f := staleLock(t)
	lockDir := filepath.Join(dir, "web"+Suffix)
	f.rename = func(oldpath, _ string) error {
		// The other breaker wins: it moves the lock away, removes it and takes
		// the name.
		if err := os.RemoveAll(oldpath); err != nil {
			return err
		}
		if err := os.Mkdir(oldpath, 0o750); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(oldpath, "holder"), []byte("winner"), 0o600); err != nil {
			return err
		}
		return &fs.PathError{Op: "rename", Path: oldpath, Err: fs.ErrNotExist}
	}
	_, err := acquire(context.Background(), f, dir, "web", opts("loser"))
	if !errors.Is(err, ErrContended) {
		t.Fatalf("acquire = %v, want ErrContended", err)
	}
	if got := read(t, filepath.Join(lockDir, "holder")); got != "winner" {
		t.Errorf("holder = %q, want the winner's", got)
	}
	if f.called("Mkdir") != 2 || f.called("RemoveAll") != 0 {
		t.Errorf("Mkdir %d times (want 2), RemoveAll %d times (want 0): the loser retries once and removes nothing", f.called("Mkdir"), f.called("RemoveAll"))
	}
}

// A rename that finds the lock gone (released, or broken and not retaken)
// lets the retry take it.
func TestBreakerThatLosesTheRenameTakesAFreeLock(t *testing.T) {
	t.Parallel()
	dir, f := staleLock(t)
	f.rename = func(oldpath, _ string) error {
		if err := os.RemoveAll(oldpath); err != nil {
			return err
		}
		return &fs.PathError{Op: "rename", Path: oldpath, Err: fs.ErrNotExist}
	}
	if _, err := acquire(context.Background(), f, dir, "web", opts("second")); err != nil {
		t.Fatal(err)
	}
	if got := read(t, filepath.Join(dir, "web"+Suffix, "holder")); got != "second" {
		t.Errorf("holder = %q", got)
	}
}

// The lock vanishing between the failed mkdir and the look at it is a free
// lock, not an error.
func TestLockReleasedWhileInspectedIsTaken(t *testing.T) {
	t.Parallel()
	dir, f := staleLock(t)
	// mkdir finds the lock; it is released before the inspection reads it.
	f.block = func(method string, _ int) bool {
		if method == "ModTime" {
			_ = os.RemoveAll(filepath.Join(dir, "web"+Suffix))
		}
		return false
	}
	if _, err := acquire(context.Background(), f, dir, "web", opts("second")); err != nil {
		t.Fatal(err)
	}
}

// What the breaker moved is not what it judged: the holder renewed (or the
// lock changed hands) between the judgement and the rename. The directory is
// put back and the attempt is contended.
func TestBreakerPutsBackALockThatChangedBeforeTheRename(t *testing.T) {
	t.Parallel()
	dir, f := staleLock(t)
	lockDir := filepath.Join(dir, "web"+Suffix)
	f.rename = func(oldpath, newpath string) error {
		if err := os.WriteFile(filepath.Join(oldpath, "holder"), []byte("new holder"), 0o600); err != nil {
			return err
		}
		return os.Rename(oldpath, newpath)
	}
	_, err := acquire(context.Background(), f, dir, "web", opts("breaker"))
	if !errors.Is(err, ErrContended) {
		t.Fatalf("acquire = %v, want ErrContended", err)
	}
	if got := read(t, filepath.Join(lockDir, "holder")); got != "new holder" {
		t.Errorf("holder = %q, want the live holder's, put back", got)
	}
	if got := names(t, dir); !slices.Equal(got, []string{"web" + Suffix}) {
		t.Errorf("directory holds %v, want the lock only", got)
	}
}

// If it cannot be put back, the error says so.
func TestBreakerReportsAFailureToPutBack(t *testing.T) {
	t.Parallel()
	dir, f := staleLock(t)
	renames := 0
	f.rename = func(oldpath, newpath string) error {
		renames++
		if renames == 1 {
			if err := os.WriteFile(filepath.Join(oldpath, "holder"), []byte("new holder"), 0o600); err != nil {
				return err
			}
			return os.Rename(oldpath, newpath)
		}
		return &fs.PathError{Op: "rename", Path: oldpath, Err: fs.ErrPermission}
	}
	_, err := acquire(context.Background(), f, dir, "web", opts("breaker"))
	if err == nil || errors.Is(err, ErrContended) || !strings.Contains(err.Error(), "putting it back failed") {
		t.Fatalf("acquire = %v, want an error naming the failed restore", err)
	}
}

func TestBreakRemoveFailureIsAnError(t *testing.T) {
	t.Parallel()
	if os.Geteuid() == 0 {
		t.Skip("root removes anything")
	}
	dir, f := staleLock(t)
	// The moved directory is made read-only, so removing its files fails.
	f.rename = func(oldpath, newpath string) error {
		if err := os.Rename(oldpath, newpath); err != nil {
			return err
		}
		return os.Chmod(newpath, 0o500) //nolint:gosec // a directory that cannot be written
	}
	_, err := acquire(context.Background(), f, dir, "web", opts("breaker"))
	// Let the temporary directory be removed.
	ents, _ := os.ReadDir(dir)
	for _, e := range ents {
		_ = os.Chmod(filepath.Join(dir, e.Name()), 0o700) //nolint:gosec // let the temporary directory be removed
	}
	if err == nil || errors.Is(err, ErrContended) {
		t.Fatalf("acquire = %v, want the removal error", err)
	}
}

// A holder that was broken out must not remove the new holder's lock.
func TestReleaseAndRenewAfterLossTouchNothing(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()
	old, err := Acquire(ctx, dir, "web", Options{Holder: "old"})
	if err != nil {
		t.Fatal(err)
	}
	// The lock goes stale and is broken by "new".
	f := newFake(t)
	f.since = serverNow.Add(-2 * testStale)
	if _, err := acquire(ctx, f, dir, "web", opts("new")); err != nil {
		t.Fatal(err)
	}
	lockDir := filepath.Join(dir, "web"+Suffix)
	before := names(t, lockDir)

	if err := old.Renew(ctx); !errors.Is(err, ErrLost) {
		t.Errorf("Renew by the broken holder = %v, want ErrLost", err)
	}
	if err := old.Release(ctx); !errors.Is(err, ErrLost) {
		t.Errorf("Release by the broken holder = %v, want ErrLost", err)
	}
	if err := old.Release(ctx); !errors.Is(err, ErrLost) {
		t.Errorf("second Release = %v, want ErrLost", err)
	}
	if got := names(t, lockDir); !slices.Equal(got, before) || read(t, filepath.Join(lockDir, "holder")) != "new" {
		t.Errorf("the new holder's lock changed: %v (was %v), holder %q", got, before, read(t, filepath.Join(lockDir, "holder")))
	}
}

func TestReleaseByHolderString(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()
	if err := Release(ctx, dir, "web", Options{Holder: "a"}); !errors.Is(err, ErrLost) {
		t.Errorf("Release of a lock that does not exist = %v, want ErrLost", err)
	}
	if _, err := Acquire(ctx, dir, "web", Options{Holder: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := Release(ctx, dir, "web", Options{Holder: "b"}); !errors.Is(err, ErrLost) {
		t.Errorf("Release by another holder = %v, want ErrLost", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "web"+Suffix, "holder")); err != nil {
		t.Errorf("the other holder's release removed the holder file: %v", err)
	}
	if err := Release(ctx, dir, "web", Options{Holder: "a"}); err != nil {
		t.Errorf("Release by the holder: %v", err)
	}
	if got := names(t, dir); len(got) != 0 {
		t.Errorf("directory holds %v after Release", got)
	}
}

// A lock directory moved away between the holder check and the write of
// since is a loss, not an error.
func TestRenewWhenTheDirectoryIsMovedAway(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	l, err := Acquire(context.Background(), dir, "web", Options{Holder: "a"})
	if err != nil {
		t.Fatal(err)
	}
	f := newFake(t)
	f.write = func(d, _ string, _ []byte) error {
		return &fs.PathError{Op: "open", Path: d, Err: fs.ErrNotExist}
	}
	l.e.fs = f
	if err := l.Renew(context.Background()); !errors.Is(err, ErrLost) {
		t.Errorf("Renew = %v, want ErrLost", err)
	}
	if err := l.Renew(context.Background()); !errors.Is(err, ErrLost) {
		t.Errorf("Renew after loss = %v, want ErrLost without asking the file system again", err)
	}
	if f.called("WriteAtomic") != 1 {
		t.Errorf("WriteAtomic called %d times, want 1", f.called("WriteAtomic"))
	}
}

// A lock whose holder or stamp cannot be written is not left behind to wait
// out Stale.
func TestAcquireRemovesAHalfMadeLock(t *testing.T) {
	t.Parallel()
	for _, failing := range []string{"holder", "since"} {
		t.Run(failing, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			f := newFake(t)
			f.write = func(d, name string, data []byte) error {
				if name == failing {
					return &fs.PathError{Op: "write", Path: d, Err: fs.ErrPermission}
				}
				return f.osFS.WriteAtomic(d, name, data)
			}
			_, err := acquire(context.Background(), f, dir, "web", opts("a"))
			if err == nil || errors.Is(err, ErrContended) || !errors.Is(err, fs.ErrPermission) {
				t.Fatalf("acquire = %v, want the write error", err)
			}
			if got := names(t, dir); len(got) != 0 {
				t.Errorf("directory holds %v, want the half-made lock removed", got)
			}
		})
	}
}

func TestErrorsNameTheOperationAndTheLockNeverThePath(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "missing", "deeper")
	_, err := Acquire(context.Background(), dir, "web", Options{})
	if err == nil {
		t.Fatal("Acquire in a directory that does not exist succeeded")
	}
	var le *Error
	if !errors.As(err, &le) || le.Op != "acquire" || le.Name != "web" {
		t.Fatalf("error = %#v", err)
	}
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("error %v does not wrap ErrNotExist", err)
	}
	if strings.Contains(err.Error(), "missing") || strings.Contains(err.Error(), "/") {
		t.Errorf("error message %q contains a path", err.Error())
	}
	if want := "lock acquire web: mkdir: no such file or directory"; err.Error() != want {
		t.Errorf("error message = %q, want %q", err.Error(), want)
	}
}

func TestOptionsAreValidated(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		dir  string
		lock string
		opts Options
		want string
	}{
		{"empty directory", "", "web", Options{}, "directory is empty"},
		{"empty name", "/d", "", Options{}, "not a name"},
		{"dot name", "/d", ".", Options{}, "not a name"},
		{"dotdot name", "/d", "..", Options{}, "not a name"},
		{"separator in name", "/d", "a/b", Options{}, "separator"},
		{"NUL in name", "/d", "a\x00b", Options{}, "separator"},
		{"long name", "/d", strings.Repeat("a", 201), Options{}, "200 bytes"},
		{"stale below the floor", "/d", "web", Options{Stale: MinStale - time.Second}, "below the minimum"},
		{"negative stale", "/d", "web", Options{Stale: -time.Minute}, "below the minimum"},
		{"holder with a newline", "/d", "web", Options{Holder: "a\nb"}, "control character"},
		{"long holder", "/d", "web", Options{Holder: strings.Repeat("h", 256)}, "255 bytes"},
		{"negative timeout", "/d", "web", Options{Timeout: -1}, "negative"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := newFake(t)
			_, err := acquire(context.Background(), f, tt.dir, tt.lock, tt.opts)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("acquire = %v, want an error containing %q", err, tt.want)
			}
			if err := releaseFS(context.Background(), f, tt.dir, tt.lock, tt.opts); err == nil {
				t.Errorf("release accepted the options")
			}
			if len(f.calls) != 0 {
				t.Errorf("the file system was used before the options were validated: %v", f.calls)
			}
		})
	}
	for _, o := range []Options{{}, {Stale: MinStale}, {Holder: "host:1", Timeout: time.Second}} {
		if _, err := newEnv(osFS{}, "/d", "web", o); err != nil {
			t.Errorf("newEnv(%+v) = %v", o, err)
		}
	}
	if h := DefaultHolder(); !strings.Contains(h, ":") {
		t.Errorf("DefaultHolder = %q, want host:pid", h)
	}
}

// Every call is under a deadline: a file system that hangs gives an error
// wrapping mount.ErrTimeout, never a hang.
func TestEveryCallIsBoundedByTheDeadline(t *testing.T) {
	t.Parallel()
	hangOn := func(method string, nth int) func(string, int) bool {
		return func(m string, n int) bool { return m == method && n == nth }
	}
	tests := []struct {
		name string
		// setup prepares the directory and the fake; op runs the call that
		// must time out.
		setup func(t *testing.T, dir string, f *fakeFS)
		op    func(ctx context.Context, f *fakeFS, dir string) error
		block func(string, int) bool
	}{
		{
			name: "acquire: mkdir", block: hangOn("Mkdir", 1),
			op: acquireOp,
		},
		{
			name: "acquire: probe", block: hangOn("Probe", 1),
			setup: heldBy("dead"), op: acquireOp,
		},
		{
			name: "acquire: stat since", block: hangOn("ModTime", 1),
			setup: heldBy("dead"), op: acquireOp,
		},
		{
			name: "acquire: stat lock", block: hangOn("ModTime", 2),
			setup: func(t *testing.T, dir string, f *fakeFS) {
				heldBy("dead")(t, dir, f)
				if err := os.Remove(filepath.Join(dir, "web"+Suffix, "since")); err != nil {
					t.Fatal(err)
				}
			},
			op: acquireOp,
		},
		{
			name: "acquire: read holder", block: hangOn("ReadFile", 1),
			setup: heldBy("dead"), op: acquireOp,
		},
		{
			name: "acquire: rename", block: hangOn("Rename", 1),
			setup: staleBy("dead"), op: acquireOp,
		},
		{
			name: "acquire: read holder of the moved lock", block: hangOn("ReadFile", 2),
			setup: staleBy("dead"), op: acquireOp,
		},
		{
			name: "acquire: remove the broken lock", block: hangOn("RemoveAll", 1),
			setup: staleBy("dead"), op: acquireOp,
		},
		{
			name: "acquire: write holder", block: hangOn("WriteAtomic", 1),
			op: acquireOp,
		},
		{
			name: "acquire: write since", block: hangOn("WriteAtomic", 2),
			op: acquireOp,
		},
		{
			name: "renew: read holder", block: hangOn("ReadFile", 1),
			setup: ownedBy("me"), op: renewOp,
		},
		{
			name: "renew: write since", block: hangOn("WriteAtomic", 1),
			setup: ownedBy("me"), op: renewOp,
		},
		{
			name: "release: read holder", block: hangOn("ReadFile", 1),
			setup: ownedBy("me"), op: releaseOp,
		},
		{
			name: "release: remove holder", block: hangOn("Remove", 1),
			setup: ownedBy("me"), op: releaseOp,
		},
		{
			name: "release: remove since", block: hangOn("Remove", 2),
			setup: ownedBy("me"), op: releaseOp,
		},
		{
			name: "release: remove the directory", block: hangOn("Remove", 3),
			setup: ownedBy("me"), op: releaseOp,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			f := newFake(t)
			if tt.setup != nil {
				tt.setup(t, dir, f)
			}
			f.block = tt.block
			f.count = map[string]int{}
			err := tt.op(context.Background(), f, dir)
			if !errors.Is(err, mount.ErrTimeout) {
				t.Fatalf("error = %v, want one wrapping mount.ErrTimeout", err)
			}
			if strings.Contains(err.Error(), dir) {
				t.Errorf("error %q contains a path", err)
			}
		})
	}
}

// shortTimeout is the deadline of each call in the deadline tests: long enough
// that only the call that hangs is ever late, short enough to keep the suite fast.
const shortTimeout = 250 * time.Millisecond

func acquireOp(ctx context.Context, f *fakeFS, dir string) error {
	_, err := acquire(ctx, f, dir, "web", Options{Holder: "me", Stale: testStale, Timeout: shortTimeout})
	return err
}

func renewOp(ctx context.Context, f *fakeFS, dir string) error {
	l, err := acquireWith(ctx, f, dir)
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.count = map[string]int{}
	f.mu.Unlock()
	return l.Renew(ctx)
}

func releaseOp(ctx context.Context, f *fakeFS, dir string) error {
	return releaseFS(ctx, f, dir, "web", Options{Holder: "me", Stale: testStale, Timeout: shortTimeout})
}

// acquireWith builds a Lock over f for a lock that setup made held by "me".
func acquireWith(_ context.Context, f *fakeFS, dir string) (*Lock, error) {
	e, err := newEnv(f, dir, "web", Options{Holder: "me", Stale: testStale, Timeout: shortTimeout})
	return &Lock{e: e}, err
}

// heldBy makes a fresh lock held by holder; staleBy one that the fake's
// server clock says is stale; ownedBy a lock held by holder with the fake
// blocking nothing yet.
func heldBy(holder string) func(*testing.T, string, *fakeFS) {
	return func(t *testing.T, dir string, f *fakeFS) {
		t.Helper()
		if _, err := Acquire(context.Background(), dir, "web", Options{Holder: holder}); err != nil {
			t.Fatal(err)
		}
		f.since = serverNow // fresh
	}
}

func staleBy(holder string) func(*testing.T, string, *fakeFS) {
	return func(t *testing.T, dir string, f *fakeFS) {
		t.Helper()
		heldBy(holder)(t, dir, f)
		f.since = serverNow.Add(-2 * testStale)
	}
}

func ownedBy(holder string) func(*testing.T, string, *fakeFS) { return heldBy(holder) }
