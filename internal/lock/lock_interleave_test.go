package lock

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
)

// takeover is another acquirer breaking the lock at lockDir and taking it:
// it moves the directory away, optionally removes it, makes a new one under
// the same path and writes its own holder and stamp. It returns what is in the
// new directory.
type takeover struct {
	lockDir, side string
	remove        bool
	newSince      os.FileInfo
}

func (o *takeover) run(t *testing.T) {
	t.Helper()
	if err := os.Rename(o.lockDir, o.side); err != nil {
		t.Error(err)
		return
	}
	if o.remove {
		_ = os.RemoveAll(o.side)
	}
	if err := os.Mkdir(o.lockDir, 0o750); err != nil {
		t.Error(err)
		return
	}
	for name, content := range map[string]string{"holder": "B", "since": "B's stamp"} {
		if err := os.WriteFile(filepath.Join(o.lockDir, name), []byte(content), 0o600); err != nil {
			t.Error(err)
		}
	}
	o.newSince, _ = os.Stat(filepath.Join(o.lockDir, "since"))
}

// assertUntouched checks that B's directory has exactly its holder and its
// stamp, unchanged.
func (o *takeover) assertUntouched(t *testing.T) {
	t.Helper()
	if got := names(t, o.lockDir); !slices.Equal(got, []string{"holder", "since"}) {
		t.Errorf("the new holder's directory holds %v, want holder and since", got)
	}
	if got := read(t, filepath.Join(o.lockDir, "holder")); got != "B" {
		t.Errorf("holder = %q, want B", got)
	}
	if got := read(t, filepath.Join(o.lockDir, "since")); got != "B's stamp" {
		t.Errorf("since = %q, want B's, unchanged", got)
	}
	if st, err := os.Stat(filepath.Join(o.lockDir, "since")); err != nil || !os.SameFile(st, o.newSince) {
		t.Errorf("since was replaced in the new holder's directory (%v)", err)
	}
}

// The holder checks the lock, is broken out and replaced, and then acts. The
// stamp or the removal must not land in the successor's directory, and the
// caller must be told the lock is lost.
func TestRenewAndReleaseAfterBreakAndRetakeTouchNothingOfTheNewHolder(t *testing.T) {
	t.Parallel()
	for _, remove := range []bool{false, true} {
		for _, op := range []string{"renew", "release"} {
			name := op
			if remove {
				name += " after the old directory was removed"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				dir := t.TempDir()
				f := newFake(t)
				ctx := context.Background()
				o, err := acquire(ctx, f, dir, "web", opts("O"))
				if err != nil {
					t.Fatal(err)
				}
				tk := &takeover{lockDir: filepath.Join(dir, "web"+Suffix), side: filepath.Join(dir, "side"), remove: remove}
				var once sync.Once
				// After the holder check has read our holder (it says O in the
				// directory we pinned, wherever it is now), B takes over.
				f.afterRead = func(string) { once.Do(func() { tk.run(t) }) }

				if op == "renew" {
					err = o.Renew(ctx)
				} else {
					err = o.Release(ctx)
				}
				if !errors.Is(err, ErrLost) {
					t.Fatalf("%s = %v, want ErrLost", op, err)
				}
				tk.assertUntouched(t)
				// Whatever the release moved to check it was put back, and
				// nothing of ours is left in the export beside the old directory.
				for _, n := range names(t, dir) {
					if n != "web"+Suffix && n != "side" {
						t.Errorf("%s left %q in the export", op, n)
					}
				}
				if err := o.Renew(ctx); !errors.Is(err, ErrLost) {
					t.Errorf("Renew after loss = %v, want ErrLost", err)
				}
			})
		}
	}
}

// A breaker that judged the previous lock stale and was slow moves the
// winner's half-made directory away. The winner writes through the directory
// it pinned, so the files are in it when the breaker puts it back.
func TestAcquireSurvivesTheMoveOfItsHalfMadeDirectory(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	f := newFake(t)
	lockDir, side := filepath.Join(dir, "web"+Suffix), filepath.Join(dir, "side")
	f.afterOpen = func() {
		if err := os.Rename(lockDir, side); err != nil {
			t.Error(err)
		}
	}
	l, err := acquire(context.Background(), f, dir, "web", opts("W"))
	if err != nil {
		t.Fatalf("acquire = %v, want it to succeed: the writes go through the pinned directory", err)
	}
	// The breaker's same() check failed, so it puts the directory back.
	if err := os.Rename(side, lockDir); err != nil {
		t.Fatal(err)
	}
	if got := names(t, lockDir); !slices.Equal(got, []string{"holder", "since"}) {
		t.Errorf("the restored directory holds %v, want holder and since", got)
	}
	if got := read(t, filepath.Join(lockDir, "holder")); got != "W" {
		t.Errorf("holder = %q, want W", got)
	}
	if err := l.Renew(context.Background()); err != nil {
		t.Errorf("Renew after the directory was put back: %v", err)
	}
	if err := l.Release(context.Background()); err != nil {
		t.Errorf("Release: %v", err)
	}
}

// A stamp that an abandoned call writes while the holder releases is removed
// with the directory.
func TestReleaseRemovesLateFiles(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	f := newFake(t)
	l, err := acquire(context.Background(), f, dir, "web", opts("a"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "web"+Suffix, ".since.tmp-0123456789abcdef"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := l.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := names(t, dir); len(got) != 0 {
		t.Errorf("the export holds %v after Release", got)
	}
}
