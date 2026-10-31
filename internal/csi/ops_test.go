package csi

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
)

// fillGroup takes every slot of a transport, as operations hung on it would.
func fillGroup(t *testing.T, l *opLimiter, group string) (release func()) {
	t.Helper()
	var releases []func()
	for i := 0; i < maxStuckPerTransport; i++ {
		r, ok := l.acquire(group, fmt.Sprintf("%s/stuck-%d", group, i))
		if !ok {
			t.Fatalf("could not take slot %d", i)
		}
		releases = append(releases, r)
	}
	return func() {
		for _, r := range releases {
			r()
		}
	}
}

func TestBlockingStartsNothingWhileAKeyIsBusy(t *testing.T) {
	t.Parallel()
	var l opLimiter
	var started atomic.Int32
	hang := make(chan struct{})
	exited := make(chan struct{})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := blocking(ctx, &l, "g", "k", func() (int, error) {
		started.Add(1)
		<-hang
		close(exited)
		return 1, nil
	})
	if err == nil || ctx.Err() == nil {
		t.Fatalf("the first call returned %v before its context ended", err)
	}

	// The first operation is still running: the same key is refused, at once,
	// without a goroutine.
	_, err = blocking(context.Background(), &l, "g", "k", func() (int, error) {
		started.Add(1)
		return 2, nil
	})
	if codeOf(err) != codes.Unavailable || !strings.Contains(err.Error(), "has not returned") {
		t.Fatalf("error = %v, want Unavailable saying a previous operation has not returned", err)
	}
	if n := started.Load(); n != 1 {
		t.Fatalf("%d operations were started, want 1", n)
	}

	// Another key on the same transport is not held up.
	got, err := blocking(context.Background(), &l, "g", "other", func() (int, error) { return 3, nil })
	if err != nil || got != 3 {
		t.Fatalf("another key: %d, %v", got, err)
	}

	// Once the hung operation exits, its own goroutine frees the slot.
	close(hang)
	<-exited
	for {
		if _, err := blocking(context.Background(), &l, "g", "k", func() (int, error) { return 4, nil }); err == nil {
			break
		}
		runtime.Gosched() // the goroutine releases its slot just after it sends its result
	}
}

func TestBlockingBoundsTheOperationsOnOneTransport(t *testing.T) {
	t.Parallel()
	var l opLimiter
	release := fillGroup(t, &l, "g")

	var started atomic.Int32
	_, err := blocking(context.Background(), &l, "g", "new-key", func() (int, error) { started.Add(1); return 0, nil })
	if codeOf(err) != codes.Unavailable || started.Load() != 0 {
		t.Fatalf("error = %v, started = %d; want Unavailable and nothing started", err, started.Load())
	}
	// Another transport has slots of its own.
	if _, err := blocking(context.Background(), &l, "h", "new-key", func() (int, error) { return 0, nil }); err != nil {
		t.Errorf("another transport: %v", err)
	}
	release()
	if _, err := blocking(context.Background(), &l, "g", "new-key", func() (int, error) { return 0, nil }); err != nil {
		t.Errorf("after the slots are free: %v", err)
	}
	// The last call's goroutine releases its slot just after it sends its
	// result; once it has, nothing is left behind.
	for {
		l.mu.Lock()
		n := len(l.keys) + len(l.count)
		l.mu.Unlock()
		if n == 0 {
			break
		}
		runtime.Gosched()
	}
}

func TestBlockingReturnsTheResultOfAFinishedOperation(t *testing.T) {
	t.Parallel()
	var l opLimiter
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := blocking(context.Background(), &l, "g", fmt.Sprint(i), func() (int, error) { return i * 2, nil })
			// The limit may refuse some of 20 at once; a refusal is Unavailable.
			if err != nil && codeOf(err) != codes.Unavailable {
				t.Errorf("error %v", err)
			}
			if err == nil && got != i*2 {
				t.Errorf("got %d, want %d", got, i*2)
			}
		}()
	}
	wg.Wait()
}

// With a transport's slots all taken by operations that have not returned, a
// create, a delete and a list start nothing and answer Unavailable, and touch
// nothing.
func TestControllerStartsNothingOnAStuckTransport(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id, dir, _ := volumeWithContent(t, e)
	release := fillGroup(t, &e.ctrl.ops, e.mountDir())
	defer release()

	_, err := e.ctrl.CreateVolume(context.Background(), e.createReq(nameC))
	if codeOf(err) != codes.Unavailable || !strings.Contains(err.Error(), "has not returned") {
		t.Errorf("create: %v, want Unavailable (stuck)", err)
	}
	mustNotExist(t, filepath.Join(e.mountDir(), nameC))

	_, err = e.ctrl.DeleteVolume(context.Background(), deleteReq(id))
	if codeOf(err) != codes.Unavailable || !strings.Contains(err.Error(), "has not returned") {
		t.Errorf("delete: %v, want Unavailable (stuck)", err)
	}
	mustExist(t, filepath.Join(dir, "a.txt"))
}

func TestListVolumesLeavesOutATransportThatIsStuckAndListsTheRest(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.create(nameA)
	good := e.mountDir()
	stuck := filepath.Join(e.root, "transports", "0123456789ab")
	writeFile(t, filepath.Join(stuck, nameB, "x"), "x")
	e.mounter.SetTable(tableWithTransports(good, stuck))
	release := fillGroup(t, &e.ctrl.ops, stuck)
	defer release()

	got := listIDs(t, e)
	if !slices.Equal(got, []string{"directory:" + nameA}) {
		t.Errorf("listed %v, want only the transport that answered", got)
	}
	logs := e.logs.String()
	if !strings.Contains(logs, "level=WARN") || !strings.Contains(logs, "transport=0123456789ab") {
		t.Errorf("the transport that did not answer was not logged by its ID:\n%s", logs)
	}
	if strings.Contains(logs, e.root) {
		t.Errorf("the log has a path:\n%s", logs)
	}
}

// A transport that hangs makes the listing wait for the handler's own
// deadline, not for ever, and the transports that answered are listed.
func TestListVolumesHasADeadlineOfItsOwn(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.create(nameA)
	hung := filepath.Join(e.root, "transports", "0123456789ab")
	writeFile(t, filepath.Join(hung, nameB, "x"), "x")
	e.mounter.SetTable(tableWithTransports(e.mountDir(), hung))

	ctrl, err := NewController(ControllerOptions{Root: e.root, Mounter: e.mounter, PVs: e.pvs, Deadline: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	hang := make(chan struct{})
	defer close(hang)
	ctrl.scan = func(dir string) []string {
		if dir == hung {
			<-hang
		}
		return volumeNames(dir)
	}

	// No deadline on the caller's context.
	resp, err := ctrl.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, entry := range resp.GetEntries() {
		ids = append(ids, entry.GetVolume().GetVolumeId())
	}
	if !slices.Equal(ids, []string{"directory:" + nameA}) {
		t.Errorf("listed %v, want only the transport that answered", ids)
	}
}

func TestListVolumesFailsWhenTheCallerGivesUp(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.create(nameA)
	e.mounter.SetTable(tableWithTransports(e.mountDir()))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := e.ctrl.ListVolumes(ctx, &csi.ListVolumesRequest{}); codeOf(err) != codes.Canceled {
		t.Errorf("code = %v, want Canceled", codeOf(err))
	}
}

// An operation that has returned its result has released its slot: the next one
// on the same key, started at once, is never refused.
func TestBlockingReleasesTheSlotBeforeTheCallerSeesTheResult(t *testing.T) {
	t.Parallel()
	var l opLimiter
	for i := 0; i < 2000; i++ {
		got, err := blocking(context.Background(), &l, "g", "k", func() (int, error) { return i, nil })
		if err != nil || got != i {
			t.Fatalf("call %d: %d, %v", i, got, err)
		}
	}
}
