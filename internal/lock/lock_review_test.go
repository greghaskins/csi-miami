package lock

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/mount"
)

// advanceAt makes the fake's server clock jump to now at the nth probe.
func advanceAt(f *fakeFS, n int, now time.Time) {
	f.block = func(method string, i int) bool {
		if method == "Probe" && i == n {
			f.mu.Lock()
			f.now = now
			f.mu.Unlock()
		}
		return false
	}
}

// A breaker that stalled since it judged the lock sees a live breaker's
// directory, made after its first probe, ahead of that probe by more than the
// tolerance. The directory is judged against a probe of its own.
func TestLiveBreakersDirectoryIsNotAheadOfAFreshProbe(t *testing.T) {
	t.Parallel()
	dir, f := staleLock(t)
	mutex := filepath.Join(dir, ".web.breaking")
	if err := os.Mkdir(mutex, 0o750); err != nil {
		t.Fatal(err)
	}
	f.dirStamp = serverNow.Add(45 * time.Second)
	advanceAt(f, 3, serverNow.Add(time.Minute)) // probes 1 and 2 judged the lock
	if _, err := acquire(context.Background(), f, dir, "web", opts("b")); !errors.Is(err, ErrContended) {
		t.Fatalf("acquire = %v, want ErrContended", err)
	}
	mustExist(t, mutex)
	if f.called("Rename") != 0 {
		t.Error("the live breaker's directory was moved")
	}
}

// A renewal that lands between the probe and the read of the stamp leads the
// first probe by the calls in between, however slow. The ahead rule uses a
// probe taken after the read.
func TestStampWrittenBetweenTheProbesIsNotAhead(t *testing.T) {
	t.Parallel()
	dir, f := staleLock(t)
	f.since = serverNow.Add(45 * time.Second)
	advanceAt(f, 2, serverNow.Add(time.Minute))
	if _, err := acquire(context.Background(), f, dir, "web", opts("b")); !errors.Is(err, ErrContended) {
		t.Fatalf("acquire = %v, want ErrContended", err)
	}
	if got := read(t, filepath.Join(dir, "web"+Suffix, "holder")); got != "dead" {
		t.Errorf("holder = %q, want the live lock untouched", got)
	}
}

func TestStalenessWithTwoProbes(t *testing.T) {
	t.Parallel()
	later := serverNow.Add(time.Minute)
	if stale, _ := stalenessAt(serverNow, later, serverNow.Add(45*time.Second), time.Hour); stale {
		t.Error("a stamp between the probes is stale")
	}
	if stale, reason := stalenessAt(serverNow, later, later.Add(maxSkew+time.Second), time.Hour); !stale || !strings.Contains(reason, "ahead") {
		t.Errorf("a stamp ahead of the later probe: %v %q", stale, reason)
	}
	// Age is judged by the earlier probe: the lock looks younger.
	if stale, _ := stalenessAt(serverNow, later, serverNow.Add(-time.Hour+30*time.Second), time.Hour); stale {
		t.Error("age was judged by the later probe")
	}
}

// NFSv3 silly-renames a file that is renamed over while open.
func TestSillyRenamedFilesAreLockShaped(t *testing.T) {
	t.Parallel()
	t.Run("allowed", func(t *testing.T) {
		t.Parallel()
		dir, f := staleLock(t)
		lockDir := filepath.Join(dir, "web"+Suffix)
		for _, n := range []string{".nfs0123456789abcdef0001", ".nfs00ff"} {
			if err := os.WriteFile(filepath.Join(lockDir, n), nil, 0o600); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := acquire(context.Background(), f, dir, "web", opts("b")); err != nil {
			t.Fatalf("acquire = %v, want the lock broken", err)
		}
	})
	for _, name := range []string{".nfs", ".nfsxyz", ".nfs12g"} {
		t.Run("not "+name, func(t *testing.T) {
			t.Parallel()
			dir, f := staleLock(t)
			lockDir := filepath.Join(dir, "web"+Suffix)
			if err := os.WriteFile(filepath.Join(lockDir, name), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := acquire(context.Background(), f, dir, "web", opts("b")); !errors.Is(err, ErrNotALock) {
				t.Fatalf("acquire = %v, want ErrNotALock", err)
			}
			mustExist(t, filepath.Join(lockDir, name))
		})
	}
}

// The listing is capped: more entries than a lock holds is not a lock.
func TestTooManyEntriesIsNotALock(t *testing.T) {
	t.Parallel()
	dir, f := staleLock(t)
	lockDir := filepath.Join(dir, "web"+Suffix)
	for i := range 12 {
		if err := os.WriteFile(filepath.Join(lockDir, fmt.Sprintf(".nfs%04x", i)), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := acquire(context.Background(), f, dir, "web", opts("b")); !errors.Is(err, ErrNotALock) {
		t.Fatalf("acquire = %v, want ErrNotALock", err)
	}
	if f.called("Rename") != 0 {
		t.Error("it was moved")
	}
}

// A breakers' directory that looked dead when it was judged but is live when
// looked at after the rename is put back.
func TestReclaimPutsBackABreakersDirectoryThatIsLive(t *testing.T) {
	t.Parallel()
	dir, f := staleLock(t)
	mutex := filepath.Join(dir, ".web.breaking")
	if err := os.Mkdir(mutex, 0o750); err != nil {
		t.Fatal(err)
	}
	f.dirStamp = serverNow.Add(-time.Hour)
	f.rename = func(oldpath, newpath string) error {
		if filepath.Base(oldpath) == ".web.breaking" {
			// A breaker finished and another made the directory anew.
			f.mu.Lock()
			f.dirStamp = serverNow
			f.mu.Unlock()
		}
		return os.Rename(oldpath, newpath)
	}
	if _, err := acquire(context.Background(), f, dir, "web", opts("b")); !errors.Is(err, ErrContended) {
		t.Fatalf("acquire = %v, want ErrContended", err)
	}
	if got := names(t, dir); !slices.Equal(got, []string{".web.breaking", "web" + Suffix}) {
		t.Errorf("directory holds %v, want the breakers' directory put back and the lock untouched", got)
	}
}

// A breaker stalled inside the breakers' directory for longer than Stale is
// reclaimed by another. When it wakes it must not remove that one's directory.
func TestStalledBreakerDoesNotRemoveTheDirectoryOfTheOneThatReclaimedIt(t *testing.T) {
	t.Parallel()
	dir, f := staleLock(t)
	mutex := filepath.Join(dir, ".web.breaking")
	f.rename = func(oldpath, newpath string) error {
		if filepath.Base(oldpath) == "web"+Suffix {
			// The stall: another breaker reclaimed our directory and made its own.
			if err := os.Rename(mutex, filepath.Join(dir, ".web.breaking-dead-0123")); err != nil {
				return err
			}
			if err := os.Mkdir(mutex, 0o750); err != nil {
				return err
			}
		}
		return os.Rename(oldpath, newpath)
	}
	if _, err := acquire(context.Background(), f, dir, "web", opts("b")); err != nil {
		t.Fatalf("acquire = %v", err)
	}
	if got := names(t, dir); !slices.Equal(got, []string{".web.breaking", ".web.breaking-dead-0123", "web" + Suffix}) {
		t.Errorf("directory holds %v, want the new owner's directory left in place", got)
	}
}

// The restore of a moved directory that was not what was judged, onto a path
// a new acquirer took since.
func TestRestoreOntoAPathANewAcquirerTook(t *testing.T) {
	t.Parallel()
	// movedNotALock makes the breaker's second look refuse, and lets newcomer
	// act after the rename.
	setup := func(t *testing.T, newcomer func(lockDir string)) (dir string, f *fakeFS) {
		dir, f = staleLock(t)
		lockDir := filepath.Join(dir, "web"+Suffix)
		f.rename = func(oldpath, newpath string) error {
			if filepath.Base(oldpath) != "web"+Suffix || f.called("Rename") != 1 {
				return os.Rename(oldpath, newpath)
			}
			if err := os.WriteFile(filepath.Join(oldpath, "precious.txt"), []byte("data"), 0o600); err != nil {
				return err
			}
			if err := os.Rename(oldpath, newpath); err != nil {
				return err
			}
			newcomer(lockDir)
			return nil
		}
		return dir, f
	}
	t.Run("an empty directory is not replaced either", func(t *testing.T) {
		t.Parallel()
		var l *Lock
		dir, f := setup(t, func(lockDir string) {
			if err := os.Mkdir(lockDir, 0o750); err != nil {
				t.Error(err)
				return
			}
			e, err := newEnv(osFS{}, filepath.Dir(lockDir), "web", Options{Holder: "newcomer"})
			if err != nil {
				t.Error(err)
				return
			}
			h, err := osFS{}.OpenDir(lockDir)
			if err != nil {
				t.Error(err)
				return
			}
			l = &Lock{e: e, dir: h}
		})
		_, err := acquire(context.Background(), f, dir, "web", opts("b"))
		if !errors.Is(err, ErrNotALock) || !strings.Contains(err.Error(), "putting it back failed") {
			t.Fatalf("acquire = %v, want ErrNotALock and the failed restore", err)
		}
		if l == nil {
			t.Fatal("the newcomer did not act")
		}
		// os.Rename does not replace a directory, empty or not: the newcomer
		// keeps its directory, which is still the one it pinned, and the moved
		// directory is stranded under a hidden name.
		if same, err := l.dir.Same(filepath.Join(dir, "web"+Suffix)); err != nil || !same {
			t.Errorf("the newcomer's directory was replaced: %v, %v", same, err)
		}
		var broken []string
		for _, n := range names(t, dir) {
			if strings.HasPrefix(n, ".web.broken-") {
				broken = append(broken, n)
			}
		}
		if len(broken) != 1 {
			t.Fatalf("hidden directories %v, want one", broken)
		}
		mustExist(t, filepath.Join(dir, broken[0], "precious.txt"))
	})
	t.Run("a directory with content is not replaced and the error says so", func(t *testing.T) {
		t.Parallel()
		dir, f := setup(t, func(lockDir string) {
			if err := os.Mkdir(lockDir, 0o750); err != nil {
				t.Error(err)
				return
			}
			if err := os.WriteFile(filepath.Join(lockDir, "holder"), []byte("newcomer"), 0o600); err != nil {
				t.Error(err)
			}
		})
		_, err := acquire(context.Background(), f, dir, "web", opts("b"))
		if !errors.Is(err, ErrNotALock) || !strings.Contains(err.Error(), "putting it back failed") {
			t.Fatalf("acquire = %v, want ErrNotALock and the failed restore", err)
		}
		if got := read(t, filepath.Join(dir, "web"+Suffix, "holder")); got != "newcomer" {
			t.Errorf("holder = %q, want the newcomer's lock intact", got)
		}
		var broken []string
		for _, n := range names(t, dir) {
			if strings.HasPrefix(n, ".web.broken-") {
				broken = append(broken, n)
			}
		}
		if len(broken) != 1 {
			t.Fatalf("hidden directories %v, want the moved directory stranded under one", broken)
		}
		mustExist(t, filepath.Join(dir, broken[0], "precious.txt"))
	})
}

// A look at the moved directory that hangs must not stop the directory being
// put back: the cleanup runs under a deadline key of its own.
func TestRestoreAfterAHungLookOfTheMovedDirectory(t *testing.T) {
	t.Parallel()
	dir, f := staleLock(t)
	// ModTime calls: the two judgements, then the look at the moved directory.
	f.block = func(method string, n int) bool { return method == "ModTime" && n == 3 }
	_, err := acquire(context.Background(), f, dir, "web", Options{Holder: "b", Stale: testStale, Timeout: 100 * time.Millisecond})
	if !errors.Is(err, mount.ErrTimeout) {
		t.Fatalf("acquire = %v, want the timeout", err)
	}
	if got := names(t, dir); !slices.Equal(got, []string{"web" + Suffix}) {
		t.Errorf("directory holds %v, want the lock put back and nothing stranded", got)
	}
	if got := read(t, filepath.Join(dir, "web"+Suffix, "holder")); got != "dead" {
		t.Errorf("holder = %q", got)
	}
}

// The moved directory vanishing is contention, not a bare errno.
func TestMovedDirectoryThatIsGoneIsContended(t *testing.T) {
	t.Parallel()
	dir, f := staleLock(t)
	f.rename = func(oldpath, newpath string) error {
		if filepath.Base(oldpath) != "web"+Suffix {
			return os.Rename(oldpath, newpath)
		}
		if err := os.Rename(oldpath, newpath); err != nil {
			return err
		}
		return os.RemoveAll(newpath)
	}
	if _, err := acquire(context.Background(), f, dir, "web", opts("b")); !errors.Is(err, ErrContended) {
		t.Fatalf("acquire = %v, want ErrContended", err)
	}
}

// Release runs the shape check too: a directory holding more than a lock
// does is not removed.
func TestReleaseDoesNotRemoveADirectoryThatIsNotLockShaped(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()
	l, err := Acquire(ctx, dir, "web", Options{Holder: "a"})
	if err != nil {
		t.Fatal(err)
	}
	lockDir := filepath.Join(dir, "web"+Suffix)
	if err := os.WriteFile(filepath.Join(lockDir, "mine.txt"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := l.Release(ctx); !errors.Is(err, ErrNotALock) {
		t.Fatalf("Release = %v, want ErrNotALock", err)
	}
	mustExist(t, filepath.Join(lockDir, "mine.txt"))
	if got := read(t, filepath.Join(lockDir, "holder")); got != "a" {
		t.Errorf("holder = %q, want the directory put back", got)
	}
}
