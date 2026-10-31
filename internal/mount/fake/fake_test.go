package fake

import (
	"context"
	"errors"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/mount"
)

func TestRecordsCalls(t *testing.T) {
	t.Parallel()
	var f Mounter
	ctx := context.Background()

	if err := f.Mount(ctx, "198.51.100.10:/export/data", "/mnt/nfs", "nfs", unix.MS_NOATIME, "vers=3"); err != nil {
		t.Fatal(err)
	}
	if err := f.Bind(ctx, "/mnt/nfs/a", "/mnt/stage", true); err != nil {
		t.Fatal(err)
	}
	if err := f.RemountReadOnly(ctx, "/mnt/stage", false); err != nil {
		t.Fatal(err)
	}
	if err := f.Unmount(ctx, "/mnt/stage", unix.MNT_DETACH); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Statfs(ctx, "/mnt/nfs"); err != nil {
		t.Fatal(err)
	}

	want := []Call{
		{Op: OpMount, Source: "198.51.100.10:/export/data", Target: "/mnt/nfs", FSType: "nfs", Flags: unix.MS_NOATIME, Data: "vers=3"},
		{Op: OpBind, Source: "/mnt/nfs/a", Target: "/mnt/stage", ReadOnly: true},
		{Op: OpRemountReadOnly, Target: "/mnt/stage"},
		{Op: OpUnmount, Target: "/mnt/stage", Flags: unix.MNT_DETACH},
		{Op: OpStatfs, Target: "/mnt/nfs"},
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

func TestConfiguredErrors(t *testing.T) {
	t.Parallel()
	var f Mounter
	ctx := context.Background()
	errAny, errBind := errors.New("any"), errors.New("bind only")

	f.FailOn("/mnt/a", errAny)
	f.FailOp(OpBind, "/mnt/a", errBind)

	if err := f.Unmount(ctx, "/mnt/a", 0); err != errAny {
		t.Errorf("Unmount: got %v, want the per-target error", err)
	}
	if err := f.Bind(ctx, "/x", "/mnt/a", false); err != errBind {
		t.Errorf("Bind: got %v, want the per-operation error, which takes precedence", err)
	}
	if err := f.Unmount(ctx, "/mnt/b", 0); err != nil {
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
	if err := f.Bind(ctx, "/x", "/mnt/a", false); err != nil {
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

func TestBlockHangsUntilTheContextIsDone(t *testing.T) {
	t.Parallel()
	var f Mounter
	f.Block("/mnt/dead")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := f.Unmount(ctx, "/mnt/dead", 0)
	if !errors.Is(err, mount.ErrTimeout) || !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("got %v, want mount.ErrTimeout wrapping DeadlineExceeded", err)
	}
	if n := len(f.Calls()); n != 1 {
		t.Errorf("the blocked call must be recorded; got %d calls", n)
	}

	cctx, ccancel := context.WithCancel(context.Background())
	ccancel()
	_, err = f.Statfs(cctx, "/mnt/dead")
	if !errors.Is(err, context.Canceled) || errors.Is(err, mount.ErrTimeout) {
		t.Errorf("cancelled: got %v, want context.Canceled and not ErrTimeout", err)
	}

	// Other targets are unaffected.
	if err := f.Unmount(context.Background(), "/mnt/alive", 0); err != nil {
		t.Errorf("unblocked target: %v", err)
	}
}

func TestUnblockReleasesTheCall(t *testing.T) {
	t.Parallel()
	started := make(chan Call, 1)
	f := Mounter{OnCall: func(c Call) { started <- c }}
	f.Block("/mnt/dead")
	f.Block("/mnt/dead") // blocking twice is the same as once

	done := make(chan error, 1)
	go func() { done <- f.Unmount(context.Background(), "/mnt/dead", 0) }()

	if c := <-started; c.Op != OpUnmount || c.Target != "/mnt/dead" {
		t.Fatalf("OnCall saw %+v", c)
	}
	f.Unblock("/mnt/dead")
	if err := <-done; err != nil {
		t.Errorf("released call: got %v, want nil", err)
	}
	// Unblocking a target that is not blocked is harmless, and it no longer hangs.
	f.Unblock("/mnt/dead")
	if err := f.Unmount(context.Background(), "/mnt/dead", 0); err != nil {
		t.Errorf("after Unblock the target no longer hangs: %v", err)
	}
	<-started
}
