package lock

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// userDir makes the directory a user keeps under the lock's name, with
// something in it that must survive.
func userDir(t *testing.T, dir string) (lockDir, precious string) {
	t.Helper()
	lockDir = filepath.Join(dir, "web"+Suffix)
	if err := os.MkdirAll(filepath.Join(lockDir, "sub"), 0o750); err != nil {
		t.Fatal(err)
	}
	precious = filepath.Join(lockDir, "sub", "precious.txt")
	if err := os.WriteFile(precious, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	return lockDir, precious
}

func mustExist(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); err != nil {
		t.Errorf("%s was removed: %v", filepath.Base(path), err)
	}
}

// I-1: a directory that only happens to be named like a lock, and is old, is
// reported and left alone.
func TestAcquireNeverBreaksADirectoryThatIsNotLockShaped(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		make func(t *testing.T, lockDir string)
		want string
	}{
		{"subdirectory", nil, `"sub"`},
		{"extra file beside a holder", func(t *testing.T, lockDir string) {
			t.Helper()
			if err := os.WriteFile(filepath.Join(lockDir, "holder"), []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(lockDir, "notes.txt"), nil, 0o600); err != nil {
				t.Fatal(err)
			}
		}, `"notes.txt"`},
		{"a directory called holder", func(t *testing.T, lockDir string) {
			t.Helper()
			if err := os.Mkdir(filepath.Join(lockDir, "holder"), 0o750); err != nil {
				t.Fatal(err)
			}
		}, `"holder"`},
		{"a temporary name that is not ours", func(t *testing.T, lockDir string) {
			t.Helper()
			if err := os.WriteFile(filepath.Join(lockDir, ".holder.tmp-x"), nil, 0o600); err != nil {
				t.Fatal(err)
			}
		}, `".holder.tmp-x"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			lockDir, precious := userDir(t, dir)
			if tt.make != nil {
				tt.make(t, lockDir)
			}
			f := newFake(t)
			f.dirStamp = serverNow.Add(-time.Hour) // old, with no since: "a holder that died"
			_, err := acquire(context.Background(), f, dir, "web", opts("breaker"))
			if !errors.Is(err, ErrNotALock) || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("acquire = %v, want ErrNotALock naming %s", err, tt.want)
			}
			if strings.Contains(err.Error(), dir) {
				t.Errorf("error %q contains a path", err)
			}
			mustExist(t, precious)
			if f.called("Rename") != 0 || f.called("RemoveAll") != 0 {
				t.Errorf("Rename %d, RemoveAll %d: nothing is to be moved or removed", f.called("Rename"), f.called("RemoveAll"))
			}
			if got := names(t, dir); !slices.Equal(got, []string{"web" + Suffix}) {
				t.Errorf("directory holds %v, want only the user's directory", got)
			}
		})
	}
}

// What the protocol writes, including the temporary names, is lock-shaped and
// is broken.
func TestAcquireBreaksALockWithLeftoverTemporaryFiles(t *testing.T) {
	t.Parallel()
	dir, f := staleLock(t)
	lockDir := filepath.Join(dir, "web"+Suffix)
	if err := os.WriteFile(filepath.Join(lockDir, ".since.tmp-0123456789abcdef"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := acquire(context.Background(), f, dir, "web", opts("breaker")); err != nil {
		t.Fatal(err)
	}
	if got := read(t, filepath.Join(lockDir, "holder")); got != "breaker" {
		t.Errorf("holder = %q", got)
	}
}

// Something is put into a stale lock between the first look and the rename:
// the second look, at the moved directory, refuses and puts it back.
func TestBreakerPutsBackADirectoryThatStoppedBeingLockShaped(t *testing.T) {
	t.Parallel()
	dir, f := staleLock(t)
	lockDir := filepath.Join(dir, "web"+Suffix)
	f.rename = func(oldpath, newpath string) error {
		if err := os.WriteFile(filepath.Join(oldpath, "precious.txt"), []byte("data"), 0o600); err != nil {
			return err
		}
		return os.Rename(oldpath, newpath)
	}
	_, err := acquire(context.Background(), f, dir, "web", opts("breaker"))
	if !errors.Is(err, ErrNotALock) {
		t.Fatalf("acquire = %v, want ErrNotALock", err)
	}
	mustExist(t, filepath.Join(lockDir, "precious.txt"))
	if got := names(t, dir); !slices.Equal(got, []string{"web" + Suffix}) {
		t.Errorf("directory holds %v, want the directory put back", got)
	}
}

// I-2: the entry at the lock's path is not a directory. It is reported with
// its own error, never moved, never followed.
func TestAcquireReportsAnEntryThatIsNotADirectory(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"regular file", "dangling symlink", "symlink to a directory"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			path := filepath.Join(dir, "web"+Suffix)
			target := filepath.Join(dir, "elsewhere")
			switch kind {
			case "regular file":
				if err := os.WriteFile(path, []byte("mine"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "dangling symlink":
				if err := os.Symlink(filepath.Join(dir, "missing"), path); err != nil {
					t.Fatal(err)
				}
			default:
				if err := os.MkdirAll(filepath.Join(target, "x"), 0o750); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			}
			before := names(t, dir)
			// Old by any clock: still never broken.
			f := newFake(t)
			f.dirStamp = serverNow.Add(-time.Hour)
			_, err := acquire(context.Background(), f, dir, "web", opts("breaker"))
			if !errors.Is(err, ErrNotALock) || errors.Is(err, ErrContended) {
				t.Fatalf("acquire = %v, want ErrNotALock and not ErrContended", err)
			}
			if !strings.Contains(err.Error(), "regular file") && !strings.Contains(err.Error(), "symbolic link") {
				t.Errorf("error %q does not say what was found", err)
			}
			if got := names(t, dir); !slices.Equal(got, before) {
				t.Errorf("directory holds %v, want %v", got, before)
			}
			if kind == "symlink to a directory" {
				mustExist(t, filepath.Join(target, "x"))
			}
		})
	}
}

func TestReleaseOfAnEntryThatIsNotADirectoryIsNotALock(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "web"+Suffix), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Release(context.Background(), dir, "web", opts("h")); !errors.Is(err, ErrNotALock) {
		t.Fatalf("Release = %v, want ErrNotALock", err)
	}
}

// I-2: a stamp in the future (set by a tenant with utimensat, or left by a
// server clock that was stepped back) does not keep the lock unbreakable. On
// the real file system and the real clock.
func TestAcquireBreaksALockStampedInTheFuture(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"since", "directory without since"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			ctx := context.Background()
			if _, err := Acquire(ctx, dir, "web", Options{Holder: "dead"}); err != nil {
				t.Fatal(err)
			}
			lockDir := filepath.Join(dir, "web"+Suffix)
			future := time.Now().Add(24 * time.Hour)
			target := filepath.Join(lockDir, "since")
			if kind != "since" {
				if err := os.Remove(target); err != nil {
					t.Fatal(err)
				}
				target = lockDir
			}
			if err := os.Chtimes(target, future, future); err != nil {
				t.Fatal(err)
			}
			l, err := Acquire(ctx, dir, "web", Options{Holder: "breaker"})
			if err != nil {
				t.Fatalf("acquire = %v, want the lock broken", err)
			}
			if !strings.Contains(l.Broke(), "ahead of the server's clock") {
				t.Errorf("Broke = %q, want the reason recorded", l.Broke())
			}
			if got := read(t, filepath.Join(lockDir, "holder")); got != "breaker" {
				t.Errorf("holder = %q", got)
			}
		})
	}
}

func TestStaleness(t *testing.T) {
	t.Parallel()
	const after = time.Minute
	tests := []struct {
		name  string
		stamp time.Time
		stale bool
		want  string
	}{
		{"fresh", serverNow, false, ""},
		{"ahead within the tolerance", serverNow.Add(maxSkew), false, ""},
		{"ahead beyond the tolerance", serverNow.Add(maxSkew + time.Second), true, "ahead of the server's clock"},
		{"a clock stepped back an hour", serverNow.Add(time.Hour), true, "1h0m0s ahead"},
		{"old", serverNow.Add(-2 * after), true, "2m0s old"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stale, reason := staleness(serverNow, tt.stamp, after)
			if stale != tt.stale || !strings.Contains(reason, tt.want) || (!stale && reason != "") {
				t.Errorf("staleness = %v, %q; want %v, containing %q", stale, reason, tt.stale, tt.want)
			}
		})
	}
}

// stallFS runs a hook just before the breaker takes the breakers' directory:
// the point where the reviewer's stalled breaker sits.
type stallFS struct {
	*fakeFS
	stall func()
	once  bool
}

func (s *stallFS) Mkdir(p string) error {
	if strings.HasSuffix(p, ".breaking") && !s.once {
		s.once = true
		s.stall()
	}
	return s.fakeFS.Mkdir(p)
}

// I-3: B2 judges the lock stale and stalls. B1 breaks it and takes it (L1).
// B2 resumes. It must not move L1, and no third acquirer may take the lock
// while B1 holds it.
func TestStalledBreakerLeavesTheLockTheOtherBreakerTook(t *testing.T) {
	t.Parallel()
	dir, f := staleLock(t)
	lockDir := filepath.Join(dir, "web"+Suffix)
	ctx := context.Background()
	var b1 *Lock
	s := &stallFS{fakeFS: f}
	s.stall = func() {
		var err error
		// B1 runs on the real file system, whose clock says the lock is old.
		old := time.Now().Add(-time.Hour)
		if err = os.Chtimes(filepath.Join(lockDir, "since"), old, old); err != nil {
			t.Error(err)
			return
		}
		if b1, err = Acquire(ctx, dir, "web", Options{Holder: "b1", Stale: testStale}); err != nil {
			t.Errorf("B1: %v", err)
			return
		}
		// B2's injected clock keeps saying "stale"; what B2 reads of the new
		// lock is fresh, as the server would answer.
		f.mu.Lock()
		f.since = serverNow
		f.mu.Unlock()
	}
	_, err := acquire(ctx, s, dir, "web", opts("b2"))
	if !errors.Is(err, ErrContended) {
		t.Fatalf("B2 = %v, want ErrContended", err)
	}
	if b1 == nil {
		t.Fatal("B1 did not acquire")
	}
	// B3 arrives right after: the lock is B1's, intact, at its path.
	if _, err := Acquire(ctx, dir, "web", Options{Holder: "b3", Stale: testStale}); !errors.Is(err, ErrContended) {
		t.Fatalf("B3 = %v, want ErrContended", err)
	}
	if err := b1.Renew(ctx); err != nil {
		t.Errorf("B1 lost its lock: %v", err)
	}
	if got := read(t, filepath.Join(lockDir, "holder")); got != "b1" {
		t.Errorf("holder = %q, want b1", got)
	}
	if got := names(t, dir); !slices.Equal(got, []string{"web" + Suffix}) {
		t.Errorf("directory holds %v, want only the lock (no stranded broken directory, no breakers' directory)", got)
	}
}

// Breakers take turns: a live breakers' directory makes the second contended,
// one left by a dead breaker is reclaimed, and one that is not an empty
// directory is reported.
func TestBreakersDirectory(t *testing.T) {
	t.Parallel()
	t.Run("held by a live breaker", func(t *testing.T) {
		t.Parallel()
		dir, f := staleLock(t)
		mutex := filepath.Join(dir, ".web.breaking")
		if err := os.Mkdir(mutex, 0o750); err != nil {
			t.Fatal(err)
		}
		f.dirStamp = serverNow // the breakers' directory is fresh
		if _, err := acquire(context.Background(), f, dir, "web", opts("b")); !errors.Is(err, ErrContended) {
			t.Fatalf("acquire = %v, want ErrContended", err)
		}
		mustExist(t, mutex)
		if f.called("Rename") != 0 {
			t.Error("the lock was moved without the breakers' directory")
		}
	})
	t.Run("left by a dead breaker", func(t *testing.T) {
		t.Parallel()
		dir, f := staleLock(t)
		if err := os.Mkdir(filepath.Join(dir, ".web.breaking"), 0o750); err != nil {
			t.Fatal(err)
		}
		f.dirStamp = serverNow.Add(-time.Hour)
		if _, err := acquire(context.Background(), f, dir, "web", opts("b")); err != nil {
			t.Fatalf("acquire = %v, want the dead breaker's directory reclaimed", err)
		}
		if got := names(t, dir); !slices.Equal(got, []string{"web" + Suffix}) {
			t.Errorf("directory holds %v", got)
		}
	})
	t.Run("not ours", func(t *testing.T) {
		t.Parallel()
		dir, f := staleLock(t)
		mutex := filepath.Join(dir, ".web.breaking")
		if err := os.WriteFile(mutex, []byte("mine"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := acquire(context.Background(), f, dir, "web", opts("b")); !errors.Is(err, ErrNotALock) {
			t.Fatalf("acquire = %v, want ErrNotALock", err)
		}
		mustExist(t, mutex)
	})
}
