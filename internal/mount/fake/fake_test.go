package fake

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/observe"
)

func TestRecordsCalls(t *testing.T) {
	t.Parallel()
	var f Mounter
	ctx := context.Background()

	if err := f.Mount(ctx, "198.51.100.10:/export/data", "/mnt/nfs", "nfs", unix.MS_NOATIME, "vers=3"); err != nil {
		t.Fatal(err)
	}
	if err := f.Bind(ctx, "", "/mnt/nfs/a", "/mnt/stage", true); err != nil {
		t.Fatal(err)
	}
	if err := f.RemountReadOnly(ctx, "/mnt/stage", false); err != nil {
		t.Fatal(err)
	}
	if err := f.Unmount(ctx, "/mnt/stage", unix.MNT_DETACH, mount.AnyID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Statfs(ctx, "/mnt/nfs"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Mounts(ctx); err != nil {
		t.Fatal(err)
	}

	want := []Call{
		{Op: OpMount, Source: "198.51.100.10:/export/data", Target: "/mnt/nfs", FSType: "nfs", Flags: unix.MS_NOATIME, Data: "vers=3"},
		{Op: OpBind, Source: "/mnt/nfs/a", Target: "/mnt/stage", ReadOnly: true},
		{Op: OpRemountReadOnly, Target: "/mnt/stage"},
		{Op: OpUnmount, Target: "/mnt/stage", Flags: unix.MNT_DETACH},
		{Op: OpStatfs, Target: "/mnt/nfs"},
		{Op: OpMounts, Target: observe.MountinfoPath},
	}
	got := f.Calls()
	if len(got) != len(want) {
		t.Fatalf("recorded %d calls, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("call %d = %+v, want %+v", i, got[i], want[i])
		}
	}

	f.Reset()
	if n := len(f.Calls()); n != 0 {
		t.Errorf("%d calls after Reset, want 0", n)
	}
}

// The fake refuses what the real Mounter refuses, before anything is recorded.
func TestValidatesLikeTheRealOne(t *testing.T) {
	t.Parallel()
	var f Mounter
	ctx := context.Background()
	bad := []string{"", "rel", "/a/../b", "/a//b", "/a/"}
	for _, p := range bad {
		if err := f.Mount(ctx, "tmpfs", p, "tmpfs", 0, ""); err == nil {
			t.Errorf("Mount(%q): want an error", p)
		}
		if err := f.Bind(ctx, "", p, "/ok", false); err == nil {
			t.Errorf("Bind source %q: want an error", p)
		}
		if err := f.Bind(ctx, "", "/ok", p, false); err == nil {
			t.Errorf("Bind target %q: want an error", p)
		}
		if err := f.RemountReadOnly(ctx, p, true); err == nil {
			t.Errorf("RemountReadOnly(%q): want an error", p)
		}
		if err := f.Unmount(ctx, p, 0, mount.AnyID); err == nil {
			t.Errorf("Unmount(%q): want an error", p)
		}
		if _, err := f.Statfs(ctx, p); err == nil {
			t.Errorf("Statfs(%q): want an error", p)
		}
	}
	if err := f.Unmount(ctx, "/", 0, mount.AnyID); err == nil || !strings.Contains(err.Error(), "refusing") {
		t.Errorf("Unmount(/): got %v, want a refusal", err)
	}
	if err := f.Unmount(ctx, "/mnt/t", unix.MNT_FORCE, mount.AnyID); err == nil {
		t.Error("Unmount with MNT_FORCE: want an error")
	}
	if n := len(f.Calls()); n != 0 {
		t.Errorf("%d calls recorded, want 0: refused calls never started", n)
	}
}

func TestConfiguredErrors(t *testing.T) {
	t.Parallel()
	var f Mounter
	ctx := context.Background()
	errAny, errBind := errors.New("any"), errors.New("bind only")

	f.FailOn("/mnt/a", errAny)
	f.FailOp(OpBind, "/mnt/a", errBind)

	if err := f.Unmount(ctx, "/mnt/a", 0, mount.AnyID); err != errAny {
		t.Errorf("Unmount: got %v, want the per-target error", err)
	}
	if err := f.Bind(ctx, "", "/x", "/mnt/a", false); err != errBind {
		t.Errorf("Bind: got %v, want the per-operation error, which takes precedence", err)
	}
	if err := f.Unmount(ctx, "/mnt/b", 0, mount.AnyID); err != nil {
		t.Errorf("another target: got %v, want nil", err)
	}
	if _, err := f.Statfs(ctx, "/mnt/a"); err != errAny {
		t.Errorf("Statfs: got %v, want the per-target error", err)
	}
	if n := len(f.Calls()); n != 4 {
		t.Errorf("%d calls recorded, want 4: failed calls are recorded too", n)
	}

	f.FailOn("/mnt/a", nil)
	f.FailOp(OpBind, "/mnt/a", nil)
	if err := f.Bind(ctx, "", "/x", "/mnt/a", false); err != nil {
		t.Errorf("after clearing: got %v, want nil", err)
	}
}

func TestStatfs(t *testing.T) {
	t.Parallel()
	var f Mounter
	f.SetStatfs("/mnt/a", unix.Statfs_t{Blocks: 42})
	st, err := f.Statfs(context.Background(), "/mnt/a")
	if err != nil || st.Blocks != 42 {
		t.Errorf("configured path: %+v, %v", st, err)
	}
	st, err = f.Statfs(context.Background(), "/mnt/b")
	if err != nil || st.Blocks != 0 {
		t.Errorf("other path: %+v, %v", st, err)
	}
}

func TestMounts(t *testing.T) {
	t.Parallel()
	var f Mounter
	ctx := context.Background()

	if tbl, err := f.Mounts(ctx); err != nil || len(tbl) != 0 {
		t.Errorf("unconfigured: %v, %v; want an empty table", tbl, err)
	}

	cfg := observe.Table{{ID: 1, ParentID: 1, MountPoint: "/", Root: "/"}, {ID: 2, ParentID: 1, MountPoint: "/mnt/a", Root: "/"}}
	f.SetTable(cfg)
	tbl, err := f.Mounts(ctx)
	if err != nil || len(tbl) != 2 {
		t.Fatalf("Mounts = %v, %v", tbl, err)
	}
	if _, ok := tbl.At("/mnt/a"); !ok {
		t.Error("the configured mount is not found")
	}
	// The caller's slices are copies: changing them changes nothing in the fake.
	tbl[0].MountPoint = "/changed"
	cfg[1].MountPoint = "/changed"
	if again, _ := f.Mounts(ctx); again[0].MountPoint != "/" || again[1].MountPoint != "/mnt/a" {
		t.Errorf("the fake's table was modified through a copy: %v", again)
	}

	wantErr := errors.New("unreadable")
	f.SetMountsError(wantErr)
	if _, err := f.Mounts(ctx); err != wantErr {
		t.Errorf("got %v, want the configured error", err)
	}
	f.SetMountsError(nil)
	if _, err := f.Mounts(ctx); err != nil {
		t.Errorf("after clearing: %v", err)
	}
}

func TestBlockHangsUntilTheContextIsDone(t *testing.T) {
	t.Parallel()
	var f Mounter
	f.Block("/mnt/dead")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := f.Unmount(ctx, "/mnt/dead", 0, mount.AnyID)
	if !errors.Is(err, mount.ErrTimeout) || !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("got %v, want mount.ErrTimeout wrapping DeadlineExceeded", err)
	}
	if n := len(f.Calls()); n != 1 {
		t.Errorf("the blocked call must be recorded; got %d calls", n)
	}

	// Other targets are unaffected.
	if err := f.Unmount(context.Background(), "/mnt/alive", 0, mount.AnyID); err != nil {
		t.Errorf("unblocked target: %v", err)
	}
}

func TestBlockCoversEverythingBelowAndBindsFromIt(t *testing.T) {
	t.Parallel()
	var f Mounter
	f.DefaultTimeout = 20 * time.Millisecond
	f.Block("/mnt/dead")

	tests := []struct {
		name string
		run  func() error
		hung bool
	}{
		{"statfs below the dead transport", func() error { _, err := f.Statfs(context.Background(), "/mnt/dead/vols/a"); return err }, true},
		{"unmount of the transport itself", func() error { return f.Unmount(context.Background(), "/mnt/dead", 0, mount.AnyID) }, true},
		{"bind from the dead transport", func() error { return f.Bind(context.Background(), "", "/mnt/dead/vols/a", "/mnt/stage", false) }, true},
		{"mount below it", func() error { return f.Mount(context.Background(), "x", "/mnt/dead/sub", "tmpfs", 0, "") }, true},
		{"a name that only shares a string prefix", func() error { return f.Unmount(context.Background(), "/mnt/deadly", 0, mount.AnyID) }, false},
		{"the parent directory", func() error { return f.Unmount(context.Background(), "/mnt", 0, mount.AnyID) }, false},
		{"a bind to elsewhere from a live path", func() error { return f.Bind(context.Background(), "", "/mnt/live/a", "/mnt/stage2", false) }, false},
	}
	for _, tt := range tests {
		err := tt.run()
		if tt.hung && !errors.Is(err, mount.ErrTimeout) {
			t.Errorf("%s: got %v, want ErrTimeout", tt.name, err)
		}
		if !tt.hung && err != nil {
			t.Errorf("%s: got %v, want nil", tt.name, err)
		}
	}

	f.Block("/")
	if err := f.Unmount(context.Background(), "/anywhere", 0, mount.AnyID); !errors.Is(err, mount.ErrTimeout) && !errors.Is(err, mount.ErrBusy) {
		t.Errorf("blocking / blocks everything: got %v", err)
	}
	// Reading the table is not a call on a mount, so it is never blocked.
	if _, err := f.Mounts(context.Background()); err != nil {
		t.Errorf("Mounts while blocked: %v", err)
	}
}

func TestDefaultDeadlineApplies(t *testing.T) {
	t.Parallel()
	f := Mounter{DefaultTimeout: 20 * time.Millisecond}
	f.Block("/mnt/dead")
	// No deadline on the context: the call must still end, like the real one.
	err := f.Unmount(context.Background(), "/mnt/dead", 0, mount.AnyID)
	if !errors.Is(err, mount.ErrTimeout) {
		t.Errorf("got %v, want ErrTimeout from the default deadline", err)
	}
}

func TestUnsetDefaultIsTheRealDefault(t *testing.T) {
	t.Parallel()
	var f Mounter
	f.Block("/mnt/dead")
	// An expired context fails before anything starts, whatever the default.
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	err := f.Unmount(ctx, "/mnt/dead", 0, mount.AnyID)
	if !errors.Is(err, mount.ErrTimeout) || strings.Contains(err.Error(), "abandoned") {
		t.Errorf("got %v, want ErrTimeout and no abandoned call", err)
	}
	if n := len(f.Calls()); n != 0 {
		t.Errorf("%d calls recorded, want 0", n)
	}
}

func TestCancellationIsNotATimeout(t *testing.T) {
	t.Parallel()
	var f Mounter
	f.Block("/mnt/dead")

	pre, precancel := context.WithCancel(context.Background())
	precancel()
	if err := f.Unmount(pre, "/mnt/dead", 0, mount.AnyID); !errors.Is(err, context.Canceled) || errors.Is(err, mount.ErrTimeout) {
		t.Errorf("already cancelled: got %v, want context.Canceled and not ErrTimeout", err)
	}

	started := make(chan Call, 1)
	f.OnCall = func(c Call) { started <- c }
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-started
		cancel()
	}()
	if err := f.Unmount(ctx, "/mnt/dead", 0, mount.AnyID); !errors.Is(err, context.Canceled) || errors.Is(err, mount.ErrTimeout) {
		t.Errorf("cancelled while hung: got %v, want context.Canceled and not ErrTimeout", err)
	}
}

func TestTimedOutCallLeavesThePathBusyUntilUnblock(t *testing.T) {
	t.Parallel()
	f := Mounter{DefaultTimeout: 20 * time.Millisecond}
	f.Block("/mnt/dead")
	ctx := context.Background()

	if err := f.Mount(ctx, "x", "/mnt/dead/sub", "tmpfs", 0, ""); !errors.Is(err, mount.ErrTimeout) {
		t.Fatalf("first call: got %v, want ErrTimeout", err)
	}
	if n := len(f.Calls()); n != 1 {
		t.Fatalf("%d calls recorded, want 1", n)
	}
	// The same path is busy; the real Mounter would not have started another.
	err := f.Unmount(ctx, "/mnt/dead/sub", 0, mount.AnyID)
	if !errors.Is(err, mount.ErrBusy) || errors.Is(err, mount.ErrTimeout) {
		t.Errorf("second call: got %v, want ErrBusy", err)
	}
	if n := len(f.Calls()); n != 1 {
		t.Errorf("%d calls recorded, want the refused call not to count", n)
	}
	// Statfs is tracked apart from the calls that change mounts.
	if _, err := f.Statfs(ctx, "/mnt/dead/sub"); !errors.Is(err, mount.ErrTimeout) {
		t.Errorf("statfs of the same path: got %v, want its own ErrTimeout", err)
	}
	if _, err := f.Statfs(ctx, "/mnt/dead/sub"); !errors.Is(err, mount.ErrBusy) {
		t.Errorf("second statfs: got %v, want ErrBusy", err)
	}

	f.Unblock("/mnt/dead")
	if err := f.Unmount(ctx, "/mnt/dead/sub", 0, mount.AnyID); err != nil {
		t.Errorf("after Unblock: %v", err)
	}
	if _, err := f.Statfs(ctx, "/mnt/dead/sub"); err != nil {
		t.Errorf("statfs after Unblock: %v", err)
	}
}

func TestUnblockReleasesTheCall(t *testing.T) {
	t.Parallel()
	started := make(chan Call, 1)
	f := Mounter{OnCall: func(c Call) { started <- c }}
	f.Block("/mnt/dead")
	f.Block("/mnt/dead") // blocking twice is the same as once

	done := make(chan error, 1)
	go func() { done <- f.Unmount(context.Background(), "/mnt/dead", 0, mount.AnyID) }()

	if c := <-started; c.Op != OpUnmount || c.Target != "/mnt/dead" {
		t.Fatalf("OnCall saw %+v", c)
	}
	f.Unblock("/mnt/dead")
	if err := <-done; err != nil {
		t.Errorf("released call: got %v, want nil", err)
	}
	// Unblocking a path that is not blocked is harmless, and it no longer hangs.
	f.Unblock("/mnt/dead")
	if err := f.Unmount(context.Background(), "/mnt/dead", 0, mount.AnyID); err != nil {
		t.Errorf("after Unblock the path no longer hangs: %v", err)
	}
	<-started
}

// A bind from a dead transport hangs while its source is resolved, which holds
// the source busy and not the target; Unblock of the transport frees it.
func TestUnblockFreesABindFromADeadSource(t *testing.T) {
	t.Parallel()
	f := Mounter{DefaultTimeout: 20 * time.Millisecond}
	ctx := context.Background()
	f.Block("/nfs")

	if err := f.Bind(ctx, "", "/nfs/vol", "/pub/x", false); !errors.Is(err, mount.ErrTimeout) {
		t.Fatalf("first bind: got %v, want ErrTimeout", err)
	}
	err := f.Bind(ctx, "", "/nfs/vol", "/pub/y", false)
	if !errors.Is(err, mount.ErrBusy) || !strings.Contains(err.Error(), "/nfs/vol") {
		t.Errorf("a second bind of the dead source: got %v, want ErrBusy naming the source", err)
	}
	if err := f.Unmount(ctx, "/pub/x", 0, mount.AnyID); err != nil {
		t.Errorf("unmount of the target while the source hangs: %v", err)
	}

	f.Unblock("/nfs")
	if err := f.Bind(ctx, "", "/nfs/vol", "/pub/x", false); err != nil {
		t.Errorf("bind after Unblock: %v", err)
	}
	if err := f.Bind(ctx, "", "/nfs/vol", "/pub/y", false); err != nil {
		t.Errorf("second bind after Unblock: %v", err)
	}
}

// The cause of a busy key is the block that produced it, not only the paths
// below the one unblocked.
func TestUnblockClearsWhatItCaused(t *testing.T) {
	t.Parallel()
	f := Mounter{DefaultTimeout: 20 * time.Millisecond}
	ctx := context.Background()
	f.Block("/nfs")
	f.Block("/other")

	if err := f.Unmount(ctx, "/nfs/a", 0, mount.AnyID); !errors.Is(err, mount.ErrTimeout) {
		t.Fatalf("got %v, want ErrTimeout", err)
	}
	if err := f.Unmount(ctx, "/other/b", 0, mount.AnyID); !errors.Is(err, mount.ErrTimeout) {
		t.Fatalf("got %v, want ErrTimeout", err)
	}
	f.Unblock("/nfs")
	if err := f.Unmount(ctx, "/nfs/a", 0, mount.AnyID); err != nil {
		t.Errorf("/nfs/a after Unblock(/nfs): %v", err)
	}
	if err := f.Unmount(ctx, "/other/b", 0, mount.AnyID); !errors.Is(err, mount.ErrBusy) {
		t.Errorf("/other/b is still held by its own block: got %v, want ErrBusy", err)
	}
}

func TestUnmountHonoursTheExpectedMountID(t *testing.T) {
	t.Parallel()
	f := &Mounter{}
	f.SetTable(observe.Table{{ID: 7, MountPoint: "/mnt/a", Root: "/", FSType: "tmpfs", Source: "t"}})
	ctx := context.Background()
	if err := f.Unmount(ctx, "/mnt/a", 0, 8); !errors.Is(err, mount.ErrConflict) {
		t.Errorf("a different ID: got %v, want ErrConflict", err)
	}
	if err := f.Unmount(ctx, "/mnt/a", 0, 7); err != nil {
		t.Errorf("the visible ID: %v", err)
	}
	if err := f.Unmount(ctx, "/mnt/a", 0, mount.AnyID); err != nil {
		t.Errorf("AnyID: %v", err)
	}
	if err := f.Unmount(ctx, "/mnt/none", 0, 8); err != nil {
		t.Errorf("nothing mounted: %v", err)
	}
}
