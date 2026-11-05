package volume

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/blockd"
	"github.com/greghaskins/csi-miami/internal/holder"
	"github.com/greghaskins/csi-miami/internal/item"
	"github.com/greghaskins/csi-miami/internal/systemd"
)

// ---- decision 52: teardown is a verified clean release ----

// dirtyEnv is a running stack with unpublished writes in its cache.
func dirtyEnv(t *testing.T) *env {
	t.Helper()
	e := newEnv(t)
	e.seedRunning()
	e.makeCache(1, "", 3, 4)
	e.ctl.dirty = 2 << 20
	e.it.cfg.PublishTimeout = 150 * time.Millisecond
	return e
}

func TestTeardownIsAVerifiedCleanRelease(t *testing.T) {
	e := dirtyEnv(t)
	if err := e.it.Teardown(bg, e.in, item.Observed{}); err != nil {
		t.Fatal(err)
	}
	e.rec.inOrder(t,
		"adopt-thaw", "unmount", "tool cryptsetup close",
		"ctl.Boundary", "ctl.Publish unstage", "current",
		"ctl.Stop", "systemd.Stop")
	// The cache and the holder file are still there when the unit stops, and
	// gone after: nothing is removed before the data is verified in the store.
	if e.atStop != [2]bool{true, true} {
		t.Errorf("cache/holder present at systemd.Stop = %v, want both", e.atStop)
	}
	if e.cacheExists() || e.holderInfo().Present {
		t.Error("the cache or the holder file survived the unstage")
	}
	if len(e.events) != 0 {
		t.Errorf("events = %v", e.events)
	}
}

func TestTeardownWaitsOutAnInFlightPublishThenPublishes(t *testing.T) {
	e := dirtyEnv(t)
	e.ctl.inFlightPolls = 3
	if err := e.it.Teardown(bg, e.in, item.Observed{}); err != nil {
		t.Fatal(err)
	}
	e.rec.inOrder(t, "ctl.Publish unstage", "ctl.Stop", "systemd.Stop")
	if e.cacheExists() {
		t.Error("cache left")
	}
}

// Reviewers' probe: an unstage that finds a publish in flight used to discard
// the unpublished chunks with no final generation and no event.
func TestTeardownDuringInFlightPublishDoesNotLoseWrites(t *testing.T) {
	e := dirtyEnv(t)
	e.ctl.inFlight = true
	err := e.it.Teardown(bg, e.in, item.Observed{})
	if !errors.Is(err, item.ErrUnstageIncomplete) {
		t.Fatalf("err = %v, want ErrUnstageIncomplete", err)
	}
	e.rec.none(t, "ctl.Stop", "systemd.Stop")
	if !e.cacheExists() || !e.holderInfo().Present {
		t.Error("the cache or holder was removed while a publish was in flight")
	}
	// Once it clears, the retry finishes and skips what is already done.
	e.ctl.inFlight = false
	if err := e.it.Teardown(bg, e.in, item.Observed{}); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if n := e.rec.count("unmount"); n != 1 {
		t.Errorf("unmounts = %d: a retry must skip finished steps", n)
	}
	if e.cacheExists() {
		t.Error("cache left after the retry")
	}
}

func TestTeardownPublishErrorIsRetryableAndTearsDownNothingBelowTheMapping(t *testing.T) {
	e := dirtyEnv(t)
	e.ctl.publishErr = syscall.ESTALE
	err := e.it.Teardown(bg, e.in, item.Observed{})
	if !errors.Is(err, item.ErrUnstageIncomplete) {
		t.Fatalf("err = %v, want ErrUnstageIncomplete", err)
	}
	e.rec.none(t, "ctl.Stop", "systemd.Stop")
	if !e.cacheExists() || !e.holderInfo().Present {
		t.Error("cache or holder removed")
	}
}

func TestTeardownVerificationMismatchIsRetryable(t *testing.T) {
	e := dirtyEnv(t)
	e.ctl.wrongName = true // the publish says it made 1.1; the store's current does not move
	if err := e.it.Teardown(bg, e.in, item.Observed{}); !errors.Is(err, item.ErrUnstageIncomplete) {
		t.Fatalf("err = %v, want ErrUnstageIncomplete", err)
	}
	e.rec.none(t, "ctl.Stop", "systemd.Stop")
}

// A previous attempt's publish may still be running inside the server when
// its unstage times out. The retry waits for it, publishes nothing new, and
// passes because current is the cache's base.
func TestTeardownRetryAcceptsThePreviousAttemptsPublish(t *testing.T) {
	e := dirtyEnv(t)
	e.ctl.inFlight = true // attempt 1: the publish it started is still uploading
	if err := e.it.Teardown(bg, e.in, item.Observed{}); !errors.Is(err, item.ErrUnstageIncomplete) {
		t.Fatalf("attempt 1: err = %v", err)
	}
	// The upload finishes inside the server: current and the cache's base move.
	e.ctl.inFlight, e.ctl.dirty, e.ctl.base, e.curName = false, 0, "1.1", "1.1"
	e.rec.reset()
	if err := e.it.Teardown(bg, e.in, item.Observed{}); err != nil {
		t.Fatalf("attempt 2: %v", err)
	}
	// Boundary and Publish still ran (the dirty count hides trimmed chunks),
	// and the empty publish was verified against the base, not a new name.
	e.rec.inOrder(t, "ctl.Boundary", "ctl.Publish unstage", "current", "ctl.Stop", "systemd.Stop")
}

// Status.Dirty is bytes and leaves out trimmed chunks, so it never decides
// whether to publish.
func TestTeardownPublishesEvenWhenNothingLooksDirty(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	e.ctl.dirty = 0
	if err := e.it.Teardown(bg, e.in, item.Observed{}); err != nil {
		t.Fatal(err)
	}
	e.rec.inOrder(t, "ctl.Boundary", "ctl.Publish unstage", "ctl.Stop")
}

func TestTeardownNothingDirtyVerifiesTheBase(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	e.ctl.base, e.curName = "1.4", "1.4"
	if err := e.it.Teardown(bg, e.in, item.Observed{}); err != nil {
		t.Fatal(err)
	}
	e.curName = "1.4"
	g := newEnv(t)
	g.seedRunning()
	g.ctl.base, g.curName = "1.4", "1.9" // current moved on from the cache's base
	if err := g.it.Teardown(bg, g.in, item.Observed{}); !errors.Is(err, item.ErrUnstageIncomplete) {
		t.Fatalf("a current that is not the base: err = %v", err)
	}
}

func TestTeardownRefusedPublishDiscardsReportsAndSucceeds(t *testing.T) {
	e := dirtyEnv(t)
	e.ctl.publishRefused = true
	if err := e.it.Teardown(bg, e.in, item.Observed{}); err != nil {
		t.Fatalf("a fenced unstage discards and succeeds: %v", err)
	}
	if !slices.Equal(e.events, []string{ReasonGenerationLost}) {
		t.Errorf("events = %v", e.events)
	}
	e.rec.inOrder(t, "ctl.Stop", "systemd.Stop")
	if e.cacheExists() {
		t.Error("cache left")
	}
	if !strings.Contains(e.logs.String(), "2097152") {
		t.Errorf("the discarded byte count is not in the log:\n%s", e.logs.String())
	}
}

func TestTeardownRestartsAGoneServerToSyncIt(t *testing.T) {
	e := dirtyEnv(t)
	e.sd.SetState(e.v.unit, systemd.State{})
	e.ctl.down = true
	if err := e.it.Teardown(bg, e.in, item.Observed{}); err != nil {
		t.Fatal(err)
	}
	e.rec.inOrder(t, "systemd.Start", "ctl.Publish unstage", "ctl.Stop", "systemd.Stop")
	if e.cacheExists() {
		t.Error("cache left")
	}
}

func TestTeardownOfAnAbandonedHolderWithAGoneServerDiscards(t *testing.T) {
	e := dirtyEnv(t)
	e.sd.SetState(e.v.unit, systemd.State{})
	e.ctl.down = true
	e.holderAt(otherNode, 3) // the operator abandoned us; another node holds the volume
	if err := e.it.Teardown(bg, e.in, item.Observed{}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(e.events, []string{ReasonGenerationLost}) {
		t.Errorf("events = %v", e.events)
	}
	if e.cacheExists() {
		t.Error("cache left")
	}
	if got := e.holderInfo(); got.Node != otherNode {
		t.Errorf("holder = %+v: another node's file must not be released", got)
	}
}

func TestTeardownStopRPCFailureIsLoggedWithTheDevice(t *testing.T) {
	e := dirtyEnv(t)
	e.ctl.stopErr = errors.New("rpc broke")
	if err := e.it.Teardown(bg, e.in, item.Observed{}); err != nil {
		t.Fatal(err)
	}
	e.rec.inOrder(t, "ctl.Stop", "systemd.Stop")
	out := e.logs.String()
	if !strings.Contains(out, "level=WARN") || !strings.Contains(out, "rpc broke") || !strings.Contains(out, "device=0") {
		t.Errorf("the failed Stop RPC is not a Warn naming the device:\n%s", out)
	}
}

func TestTeardownThawsBeforeEveryUnmount(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	if err := e.it.Teardown(bg, e.in, item.Observed{}); err != nil {
		t.Fatal(err)
	}
	e.rec.inOrder(t, "adopt-thaw", "unmount")

	// The fence path unmounts too.
	f := newEnv(t)
	f.seedRunning()
	f.holderAt(otherNode, 3)
	f.regressHolder(testNode, 1)
	f.curName = "3.1"
	f.mustConverge()
	f.rec.inOrder(t, "adopt-thaw", "unmount", "ctl.Stop")
}

func TestGenerationAfterTeardownIsSkippedAndRecreatesNothing(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	if err := e.it.Teardown(bg, e.in, item.Observed{}); err != nil {
		t.Fatal(err)
	}
	e.rec.reset()
	if err := e.it.Generation(bg, e.in); !errors.Is(err, ErrGenerationSkipped) {
		t.Fatalf("err = %v, want ErrGenerationSkipped", err)
	}
	e.rec.none(t, "freeze", "ctl.")
	// A restage lifts it.
	e.ctl.down = true
	e.mustConverge()
	e.rec.reset()
	if err := e.it.Generation(bg, e.in); err != nil {
		t.Fatalf("after a restage: %v", err)
	}
}

// ---- generations ----

type blockingCtl struct {
	Control
	entered, release chan struct{}
}

func (b *blockingCtl) Publish(ctx context.Context, trigger string) (blockd.PublishReply, error) {
	close(b.entered)
	<-b.release
	return b.Control.Publish(ctx, trigger)
}

// The upload of a generation must not hold anything Converge or Teardown
// needs: the loop serves every volume on the node.
func TestConvergeDoesNotBlockBehindAnUploadingPublish(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	inner := e.it.cfg.Dial
	bc := &blockingCtl{entered: make(chan struct{}), release: make(chan struct{})}
	e.it.cfg.Dial = func(ctx context.Context, s string) (Control, error) {
		c, err := inner(ctx, s)
		if err != nil {
			return nil, err
		}
		bc.Control = c
		return bc, nil
	}
	genDone := make(chan error, 1)
	go func() { genDone <- e.it.Generation(bg, e.in) }()
	<-bc.entered
	e.it.cfg.Dial = inner
	convDone := make(chan error, 1)
	go func() { convDone <- e.converge() }()
	select {
	case err := <-convDone:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(700 * time.Millisecond):
		close(bc.release)
		_, _ = <-genDone, <-convDone
		t.Fatal("Converge blocked behind an in-flight Publish")
	}
	close(bc.release)
	<-genDone //nolint:errcheck // only draining
}

type hangPublish struct {
	*fakeCtl
	release chan struct{}
}

func (h hangPublish) Publish(ctx context.Context, trigger string) (blockd.PublishReply, error) {
	if trigger == "interval" {
		select {
		case <-h.release:
		case <-ctx.Done():
			return blockd.PublishReply{}, ctx.Err()
		}
	}
	return h.fakeCtl.Publish(ctx, trigger)
}

func TestTeardownDoesNotBlockBehindALongIntervalPublish(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	rel := make(chan struct{})
	defer close(rel)
	e.it.cfg.Dial = func(context.Context, string) (Control, error) { return hangPublish{e.ctl, rel}, nil }
	e.it.cfg.PublishTimeout = 100 * time.Millisecond
	go func() { _ = e.it.Generation(bg, e.in) }()
	time.Sleep(50 * time.Millisecond)
	done := make(chan error, 1)
	ctx, cancel := context.WithTimeout(bg, 300*time.Millisecond)
	defer cancel()
	go func() { done <- e.it.Teardown(ctx, e.in, item.Observed{}) }()
	select {
	case err := <-done:
		_ = err
	case <-time.After(2 * time.Second):
		t.Errorf("Teardown still blocked 2s later; calls=%v", e.rec.all())
	}
}

func TestScheduledPublishHasItsOwnDeadline(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	rel := make(chan struct{})
	defer close(rel)
	e.it.cfg.Dial = func(context.Context, string) (Control, error) { return hangPublish{e.ctl, rel}, nil }
	e.it.cfg.PublishTimeout = 100 * time.Millisecond
	done := make(chan error, 1)
	go func() { done <- e.it.Generation(bg, e.in) }()
	select {
	case err := <-done:
		if err == nil {
			t.Error("a publish that outlasts PublishTimeout must fail")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the publish has no deadline of its own")
	}
}

func TestGenerationFailsWhenTheWatchdogThawedBeforeTheBoundary(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	e.ctl.boundaryDelay = 400 * time.Millisecond // longer than FreezeBound, ignores its context
	err := e.it.Generation(bg, e.in)
	if err == nil {
		t.Fatal("a boundary taken after the watchdog thawed was published")
	}
	e.rec.none(t, "ctl.Publish")
}

// ---- grow ----

// Reviewers' probes: a half-done grow under 25%, or a mapping short by less
// than a header's worth, used to be left unfinished for good.
func TestSmallHalfDoneGrowIsFinished(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	e.mnt.SetStatfs(e.v.stage, unix.Statfs_t{Blocks: uint64(testSize) * 85 / 100 / 4096, Bsize: 4096})
	e.mustConverge()
	if e.rec.count("tool resize2fs") == 0 || e.rec.count("tool cryptsetup resize") == 0 {
		t.Fatalf("not resized: %v", e.rec.all())
	}
}

func TestGrowStepsRunOncePerSizePerProcess(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	e.mustConverge()
	e.mustConverge()
	if e.rec.count("tool resize2fs") != 1 || e.rec.count("tool cryptsetup resize") != 1 {
		t.Errorf("calls = %v", e.rec.all())
	}
	e.in.Volume.Size = 2 * testSize
	e.ctl.size = 2 * testSize
	e.mustConverge()
	if e.rec.count("tool resize2fs") != 2 {
		t.Errorf("a new size must resize again: %v", e.rec.all())
	}
}

// ---- cache reuse ----

func TestRebootBeforeFirstPublishKeepsTheDirtyCache(t *testing.T) {
	e := newEnv(t)
	e.makeCache(2, "1.3", 5)
	if err := e.it.decideCache(e.v, current{name: "1.3", ok: true, epoch: 1}); err != nil {
		t.Fatal(err)
	}
	if !e.cacheExists() {
		t.Fatalf("discarded although current is still the base it descends from; events=%v", e.events)
	}
}

// ---- the epoch a running server publishes under ----

func TestServerAtAStaleEpochIsStoppedAndRestartedAtTheHeldOne(t *testing.T) {
	e := newEnv(t)
	e.seedRunning() // file us@1, server and cache at 1
	e.holderAt(otherNode, 3)
	if err := os.Remove(holder.Path(e.v.holdDir, e.v.name)); err != nil { // it died or released
		t.Fatal(err)
	}
	e.curName = "3.2"
	e.newItem()
	e.mustConverge() // acquires at 4; the unit still runs -epoch 1

	e.rec.inOrder(t, "holder.Acquire", "unmount", "ctl.Stop", "systemd.Stop", "systemd.Start")
	if got := e.epochOfLastStart(); got != 4 {
		t.Errorf("restarted at epoch %d, want 4", got)
	}
	if e.ctl.epoch != 4 {
		t.Errorf("server epoch = %d", e.ctl.epoch)
	}
}

func TestAdoptingAUnitWithADifferentExecStartStillChecksTheEpoch(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	e.sd.SetState(e.v.unit, systemd.State{})
	e.sd.startErr = wrapDiffers()
	e.sd.activeOnErr = true
	e.ctl.epoch = 9 // the running server publishes under another epoch
	err := e.converge()
	if !errors.Is(err, item.ErrServerDown) {
		t.Fatalf("err = %v, want ErrServerDown (stopped; restarted next pass)", err)
	}
	e.rec.inOrder(t, "systemd.Start", "ctl.Stop", "systemd.Stop")
}

func wrapDiffers() error { return errors.Join(errors.New("unit exists"), systemd.ErrExecStartDiffers) }

// ---- fencing with the stage still in use ----

func TestFencedWhileTheStageIsBoundIntoPodsLeavesItAlone(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	e.holderAt(otherNode, 3)
	e.regressHolder(testNode, 1)
	e.curName = "3.1"
	// A pod's bind of the stage: the same device again.
	e.mnt.addDev("/var/lib/kubelet/pods/p/volumes/mount", "ext4", "/dev/mapper/miami-"+e.v.name, 252, 0)
	e.mnt.setDev(e.v.stage, 252, 0)
	err := e.converge()
	if !errors.Is(err, item.ErrPublishRefused) {
		t.Fatalf("err = %v, want ErrPublishRefused", err)
	}
	e.rec.none(t, "unmount", "ctl.Stop", "systemd.Stop", "holder.Acquire")
	if !e.cacheExists() {
		t.Error("cache removed under a bound stage")
	}
}

// ---- smaller findings ----

func TestHostBinaryIsReusedByContentNotSize(t *testing.T) {
	e := newEnv(t)
	if err := e.it.ensureBinary(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(e.exe, []byte("#!fake_binary"), 0o755); err != nil { //nolint:gosec // a fake executable
		t.Fatal(err)
	}
	// A new item (a restarted plugin) must not trust the name and the size.
	e.newItem()
	if err := e.it.ensureBinary(); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(e.it.binPath())
	if err != nil || string(got) != "#!fake_binary" {
		t.Fatalf("host copy = %q, %v: a copy of the same size and a different content was kept", got, err)
	}
}

func TestStageOwnershipIsThisVolumesDeviceOnly(t *testing.T) {
	e := newEnv(t)
	e.seedRunning() // this volume's device is nbd0
	e.mnt.entries = nil
	e.mnt.add(e.v.stage, "ext4", "/dev/nbd7")
	err := e.it.Teardown(bg, e.in, item.Observed{})
	if !errors.Is(err, item.ErrStageConflict) {
		t.Fatalf("err = %v, want ErrStageConflict for another volume's device", err)
	}
	e.rec.none(t, "unmount")
}

func TestDeviceWithNoCacheIndexIsServerDownNotACreatedCache(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	_ = os.RemoveAll(e.v.cacheDir)
	e.sd.SetState(e.v.unit, systemd.State{})
	err := e.converge()
	if !errors.Is(err, item.ErrServerDown) {
		t.Fatalf("err = %v, want ErrServerDown", err)
	}
	e.rec.none(t, "systemd.Start")
	if e.cacheExists() {
		t.Error("a cache was created for a device that has none")
	}
}

func TestReadingCurrentCreatesNoDirectories(t *testing.T) {
	dir := t.TempDir()
	name, ok, err := storeCurrent(bg, dir)
	if err != nil || ok || name != "" {
		t.Fatalf("empty store: %q %v %v", name, ok, err)
	}
	for _, d := range []string{"chunks", "manifests"} {
		if _, err := os.Stat(filepath.Join(dir, d)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s was created by a read", d)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "current"), []byte("3.4\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if name, ok, err := storeCurrent(bg, dir); err != nil || !ok || name != "3.4" {
		t.Fatalf("current: %q %v %v", name, ok, err)
	}
	if _, _, err := storeCurrent(bg, filepath.Join(dir, "missing")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a missing store directory: %v", err)
	}
}

// The key must not reach a log line or an error, whichever path a key takes.
func TestKeysNeverReachLogsOrErrors(t *testing.T) {
	const key, prev = "SECRET-KEY-MATERIAL", "SECRET-PREVIOUS-KEY"
	var errs []string
	note := func(err error) {
		if err != nil {
			errs = append(errs, err.Error())
		}
	}
	e := newEnv(t)
	e.keys = item.Keys{Key: []byte(key), PreviousKey: []byte(prev)}
	note(e.converge()) // first stage: format and mount
	e.newItem()
	note(e.converge()) // adopt

	f := newEnv(t) // a wrong key, with and without a previous key
	f.tools.luks, f.tools.luksKey = true, []byte("stored")
	f.keys = item.Keys{Key: []byte(key), PreviousKey: []byte(prev)}
	note(f.converge())
	f.keys = item.Keys{Key: []byte(key)}
	note(f.converge())

	g := newEnv(t) // rotation
	g.tools.luks, g.tools.luksKey = true, []byte(prev)
	g.keys = item.Keys{Key: []byte(key), PreviousKey: []byte(prev)}
	note(g.converge())

	logs := e.logs.String() + f.logs.String() + g.logs.String()
	all := logs + strings.Join(errs, "\n")
	for _, secret := range []string{key, prev} {
		if strings.Contains(all, secret) {
			t.Errorf("%q reached a log or an error:\n%s", secret, all)
		}
	}
	for _, env := range []*env{e, f, g} {
		for _, u := range env.sd.units {
			if bytes.Contains([]byte(strings.Join(u.ExecStart, " ")), []byte(key)) {
				t.Error("the key is on a unit's command line")
			}
		}
	}
}
