package lock

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The times below are the server's, injected; none of them is near the local
// clock, which the package must never consult.
var serverNow = time.Date(1999, 12, 31, 12, 0, 0, 0, time.UTC)

// fakeFS runs the real file system and lets a test inject what the server's
// clock says, make a call block, observe calls and interfere with one.
type fakeFS struct {
	osFS

	mu       sync.Mutex
	now      time.Time // the probe's modification time
	since    time.Time // since's modification time
	dirStamp time.Time // a lock directory's modification time
	calls    []string

	hang    chan struct{}
	running int                             // calls in flight, abandoned ones included
	idle    *sync.Cond                      // signalled when running reaches zero
	block   func(method string, n int) bool // blocks the nth call of method when true
	count   map[string]int

	// Hooks run in place of the real call (they may call it).
	rename func(oldpath, newpath string) error
	write  func(dir, name string, data []byte) error
}

func newFake(t *testing.T) *fakeFS {
	t.Helper()
	f := &fakeFS{now: serverNow, since: serverNow, dirStamp: serverNow, hang: make(chan struct{}), count: map[string]int{}}
	// Release the calls that hang and let them finish before the directory
	// they work in is removed.
	f.idle = sync.NewCond(&f.mu)
	t.Cleanup(func() {
		close(f.hang)
		f.mu.Lock()
		defer f.mu.Unlock()
		for f.running > 0 {
			f.idle.Wait()
		}
	})
	return f
}

// enter records a call and blocks it if the test asked. The caller defers the
// function it returns.
func (f *fakeFS) enter(method string) func() {
	f.mu.Lock()
	f.running++
	f.calls = append(f.calls, method)
	f.count[method]++
	n := f.count[method]
	block := f.block
	f.mu.Unlock()
	if block != nil && block(method, n) {
		<-f.hang
	}
	return func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.running--; f.running == 0 {
			f.idle.Broadcast()
		}
	}
}

func (f *fakeFS) called(method string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.count[method]
}

func (f *fakeFS) Mkdir(p string) error { defer f.enter("Mkdir")(); return f.osFS.Mkdir(p) }

func (f *fakeFS) ReadFile(p string) ([]byte, error) {
	defer f.enter("ReadFile")()
	return f.osFS.ReadFile(p)
}

func (f *fakeFS) Remove(p string) error { defer f.enter("Remove")(); return f.osFS.Remove(p) }

func (f *fakeFS) RemoveAll(p string) error { defer f.enter("RemoveAll")(); return f.osFS.RemoveAll(p) }

func (f *fakeFS) Rename(o, n string) error {
	defer f.enter("Rename")()
	if f.rename != nil {
		return f.rename(o, n)
	}
	return f.osFS.Rename(o, n)
}

func (f *fakeFS) WriteAtomic(dir, name string, data []byte) error {
	defer f.enter("WriteAtomic")()
	if f.write != nil {
		return f.write(dir, name, data)
	}
	return f.osFS.WriteAtomic(dir, name, data)
}

// ModTime answers with the injected stamps, after checking the path exists.
func (f *fakeFS) ModTime(p string) (time.Time, error) {
	defer f.enter("ModTime")()
	if _, err := f.osFS.ModTime(p); err != nil {
		return time.Time{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if filepath.Base(p) == sinceFile {
		return f.since, nil
	}
	return f.dirStamp, nil
}

// Probe makes and removes a real probe file and answers with the injected now.
func (f *fakeFS) Probe(dir, name string) (time.Time, error) {
	defer f.enter("Probe")()
	if _, err := f.osFS.Probe(dir, name); err != nil {
		return time.Time{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now, nil
}

func names(t *testing.T, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range ents {
		out = append(out, e.Name())
	}
	sort.Strings(out)
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func read(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestAcquireWritesTheLockAndReleaseRemovesIt(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	l, err := Acquire(ctx, dir, "web", Options{Holder: "node-a:1"})
	if err != nil {
		t.Fatal(err)
	}
	if l.Name() != "web" || l.Holder() != "node-a:1" {
		t.Errorf("Name, Holder = %q, %q", l.Name(), l.Holder())
	}
	lockDir := filepath.Join(dir, "web"+Suffix)
	if got := names(t, lockDir); !equalStrings(got, []string{"holder", "since"}) {
		t.Fatalf("lock directory holds %v, want holder and since only (no temporary files)", got)
	}
	if got := read(t, filepath.Join(lockDir, "holder")); got != "node-a:1" {
		t.Errorf("holder = %q", got)
	}
	if st, err := os.Stat(filepath.Join(lockDir, "since")); err != nil || st.Size() != 0 {
		t.Errorf("since: %v, %v; want an empty file", st, err)
	}

	if err := l.Renew(ctx); err != nil {
		t.Errorf("Renew: %v", err)
	}
	if got := names(t, lockDir); !equalStrings(got, []string{"holder", "since"}) {
		t.Errorf("after Renew the lock directory holds %v", got)
	}

	if err := l.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if got := names(t, dir); len(got) != 0 {
		t.Errorf("after Release %s holds %v, want nothing", dir, got)
	}
	// A released lock can be taken again, by anyone.
	l2, err := Acquire(ctx, dir, "web", Options{Holder: "node-b:2"})
	if err != nil {
		t.Fatalf("Acquire after Release: %v", err)
	}
	if err := l2.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestAcquireContended(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()
	if _, err := Acquire(ctx, dir, "web", Options{Holder: "a"}); err != nil {
		t.Fatal(err)
	}

	_, err := Acquire(ctx, dir, "web", Options{Holder: "b"})
	if !errors.Is(err, ErrContended) {
		t.Fatalf("second Acquire = %v, want ErrContended", err)
	}
	var le *Error
	if !errors.As(err, &le) || le.Op != "acquire" || le.Name != "web" {
		t.Errorf("error = %#v, want an *Error for acquire of web", err)
	}
	// The holder, the stamp and the directory are untouched, and the probe
	// the second attempt made is gone.
	if got := read(t, filepath.Join(dir, "web"+Suffix, "holder")); got != "a" {
		t.Errorf("holder = %q after a contended attempt, want a", got)
	}
	if got := names(t, dir); !equalStrings(got, []string{"web" + Suffix}) {
		t.Errorf("directory holds %v after a contended attempt", got)
	}
	// A different name is a different lock.
	if _, err := Acquire(ctx, dir, "other", Options{Holder: "b"}); err != nil {
		t.Errorf("Acquire of another name: %v", err)
	}
}

func TestOnlyOneConcurrentAcquirerWins(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	const n = 16
	var wins, contended atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := Acquire(context.Background(), dir, "web", Options{Holder: "h"})
			switch {
			case err == nil:
				wins.Add(1)
			case errors.Is(err, ErrContended):
				contended.Add(1)
			default:
				t.Errorf("Acquire: %v", err)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 || contended.Load() != n-1 {
		t.Errorf("%d won and %d were contended, want 1 and %d", wins.Load(), contended.Load(), n-1)
	}
	if got := names(t, dir); !equalStrings(got, []string{"web" + Suffix}) {
		t.Errorf("directory holds %v, want the lock only", got)
	}
}

func TestIsStale(t *testing.T) {
	t.Parallel()
	const after = time.Minute
	tests := []struct {
		name  string
		stamp time.Time
		want  bool
	}{
		{"fresh", serverNow.Add(-10 * time.Second), false},
		{"just under", serverNow.Add(-after + time.Nanosecond), false},
		{"exactly the period", serverNow.Add(-after), false},
		{"just over", serverNow.Add(-after - time.Nanosecond), true},
		{"long dead", serverNow.Add(-24 * time.Hour), true},
		{"stamp ahead of the probe", serverNow.Add(5 * time.Second), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := isStale(serverNow, tt.stamp, after); got != tt.want {
				t.Errorf("isStale = %v, want %v", got, tt.want)
			}
		})
	}
}

// The comparison runs on injected server times through the whole acquire
// path, on a real directory: the break renames the directory away and removes
// it, leaves no probe or broken directory, and the new holder is ours.
func TestAcquireBreaksOnlyAStaleLock(t *testing.T) {
	t.Parallel()
	stale := 2 * time.Minute
	tests := []struct {
		name      string
		dropSince bool
		since     time.Time // since's stamp (or the directory's, if dropSince)
		wantBreak bool
	}{
		{name: "fresh since", since: serverNow.Add(-stale / 2)},
		{name: "since exactly stale", since: serverNow.Add(-stale)},
		{name: "stale since", since: serverNow.Add(-stale - time.Second), wantBreak: true},
		{name: "stamp ahead of the probe", since: serverNow.Add(time.Hour)},
		{name: "no since, directory fresh", dropSince: true, since: serverNow.Add(-stale / 2)},
		{name: "no since, directory stale", dropSince: true, since: serverNow.Add(-stale - time.Second), wantBreak: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			ctx := context.Background()
			if _, err := Acquire(ctx, dir, "web", Options{Holder: "dead"}); err != nil {
				t.Fatal(err)
			}
			lockDir := filepath.Join(dir, "web"+Suffix)
			f := newFake(t)
			if tt.dropSince {
				if err := os.Remove(filepath.Join(lockDir, "since")); err != nil {
					t.Fatal(err)
				}
				f.dirStamp = tt.since
				// Were it consulted, the since stamp would say fresh.
				f.since = serverNow
			} else {
				f.since = tt.since
				f.dirStamp = serverNow // the directory's stamp is not consulted when there is a since
			}

			_, err := acquire(ctx, f, dir, "web", Options{Holder: "breaker", Stale: stale, Timeout: time.Minute})
			if tt.wantBreak {
				if err != nil {
					t.Fatalf("acquire: %v", err)
				}
				if got := read(t, filepath.Join(lockDir, "holder")); got != "breaker" {
					t.Errorf("holder = %q, want breaker", got)
				}
				if f.called("Probe") != 1 {
					t.Errorf("probes = %d, want 1", f.called("Probe"))
				}
			} else {
				if !errors.Is(err, ErrContended) {
					t.Fatalf("acquire = %v, want ErrContended", err)
				}
				if got := read(t, filepath.Join(lockDir, "holder")); got != "dead" {
					t.Errorf("holder = %q, want the original", got)
				}
				if f.called("Rename") != 0 {
					t.Error("a lock that is not stale was renamed")
				}
			}
			if got := names(t, dir); !equalStrings(got, []string{"web" + Suffix}) {
				t.Errorf("directory holds %v, want only the lock (no probe, no broken directory)", got)
			}
		})
	}
}
