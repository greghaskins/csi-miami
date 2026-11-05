package volume

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/blockd"
	"github.com/greghaskins/csi-miami/internal/holder"
	"github.com/greghaskins/csi-miami/internal/item"
	mountfake "github.com/greghaskins/csi-miami/internal/mount/fake"
	"github.com/greghaskins/csi-miami/internal/prereq"
	"github.com/greghaskins/csi-miami/internal/systemd"
)

func TestFirstStageFormatsAndMounts(t *testing.T) {
	e := newEnv(t)
	e.mustConverge()

	mapper := "/dev/mapper/miami-" + e.v.name
	e.rec.inOrder(t,
		"holder.Read", "holder.Acquire", "current",
		"systemd.Start",
		"tool cryptsetup isLuks", "tool cryptsetup luksFormat", "tool cryptsetup open",
		"tool blkid", "tool mkfs.ext4", "mount ext4 "+mapper)
	e.rec.none(t, "tool e2fsck", "tool resize2fs", "ctl.Grow")

	// The unit: the host copy of the binary, restarted a second after a
	// failure, with the epoch and no key anywhere.
	if len(e.sd.units) != 1 {
		t.Fatalf("units started: %d", len(e.sd.units))
	}
	u := e.sd.units[0]
	if u.Name != "miami-blockd-"+e.v.name+".service" || u.Restart != "on-failure" || u.RestartDelay != time.Second {
		t.Errorf("unit = %+v", u)
	}
	if want := filepath.Join(e.binDir, "miami-test"); u.ExecStart[0] != want || u.ExecStart[1] != "blockd" {
		t.Errorf("ExecStart = %v", u.ExecStart)
	}
	if !slices.Contains(u.ExecStart, "-epoch") || u.ExecStart[slices.Index(u.ExecStart, "-epoch")+1] != "1" {
		t.Errorf("no epoch in %v", u.ExecStart)
	}
	fi, err := os.Stat(filepath.Join(e.binDir, "miami-test"))
	if err != nil || fi.Mode().Perm() != 0o755 {
		t.Errorf("host binary: %v %v", fi, err)
	}
	var all []string
	all = append(all, u.ExecStart...)
	all = append(all, u.Environment...)
	for _, c := range e.tools.calls {
		all = append(all, c...)
	}
	for _, a := range all {
		if strings.Contains(a, "the-key") {
			t.Errorf("the key is in an argument: %q", a)
		}
	}

	// A second pass changes nothing.
	e.rec.reset()
	e.mustConverge()
	if got := e.rec.all(); !slices.Equal(got, []string{"current"}) {
		t.Errorf("a converged volume made calls: %v", got)
	}
}

func TestPlainVolumeHasNoMapping(t *testing.T) {
	e := newEnv(t)
	e.in.Volume.Encryption = false
	e.keys = item.Keys{}
	e.mustConverge()
	e.rec.none(t, "tool cryptsetup")
	e.rec.inOrder(t, "tool blkid", "tool mkfs.ext4", "mount ext4 "+filepath.Join(e.devDir, "nbd0"))
}

func TestRestageAfterPluginRestartAdoptsEverything(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	before := e.mnt.table()
	e.newItem() // the plugin restarted

	e.mustConverge()

	// Observations and the thaw of the adopted mount, nothing else.
	want := []string{"holder.Read", "holder.Adopt", "current", "adopt-thaw"}
	if got := e.rec.all(); !slices.Equal(got, want) {
		t.Errorf("calls = %v, want %v", got, want)
	}
	if after := e.mnt.table(); !reflect.DeepEqual(after, before) {
		t.Errorf("the mount table changed: %v -> %v", before, after)
	}
	if got := e.holderInfo(); got.Node != testNode || got.Epoch != 1 {
		t.Errorf("holder = %+v: adoption must continue the epoch", got)
	}
}

func TestAdoptThawsMount(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	e.mustConverge()
	e.mustConverge()
	if e.thaws != 1 {
		t.Errorf("thaws after two passes = %d, want 1: the adoption thaws once, and never ends a generation's freeze", e.thaws)
	}
	e.newItem()
	e.mustConverge()
	if e.thaws != 2 {
		t.Errorf("thaws after a plugin restart = %d, want 2", e.thaws)
	}
	// A mount made by this process is not thawed.
	f := newEnv(t)
	f.mustConverge()
	if f.thaws != 0 {
		t.Errorf("a fresh mount was thawed %d times", f.thaws)
	}
}

func TestNoKeyIsKeyRequired(t *testing.T) {
	e := newEnv(t)
	e.keys = item.Keys{}
	err := e.converge()
	if !errors.Is(err, item.ErrKeyRequired) {
		t.Fatalf("err = %v, want ErrKeyRequired", err)
	}
	e.rec.none(t, "tool cryptsetup luksFormat", "tool cryptsetup open", "tool mkfs")

	// An open mapping needs no key: a restage after a plugin restart.
	e.keys = item.Keys{Key: []byte("the-key")}
	e.mustConverge()
	e.keys = item.Keys{}
	e.newItem()
	e.mustConverge()
}

func TestBadKey(t *testing.T) {
	e := newEnv(t)
	e.tools.luks, e.tools.luksKey = true, []byte("stored")
	err := e.converge()
	if !errors.Is(err, item.ErrBadKey) {
		t.Fatalf("err = %v, want ErrBadKey", err)
	}
	e.rec.none(t, "tool mkfs", "tool cryptsetup luksChangeKey", "mount")

	e.keys.PreviousKey = []byte("also-wrong")
	if err := e.converge(); !errors.Is(err, item.ErrBadKey) {
		t.Fatalf("a previous key that does not open it: err = %v, want ErrBadKey", err)
	}
	if string(e.tools.luksKey) != "stored" {
		t.Error("the key was rotated by a previous key that did not open the volume")
	}
}

func TestPreviousKeyRotatesThenOpens(t *testing.T) {
	e := newEnv(t)
	e.tools.luks, e.tools.luksKey = true, []byte("old")
	e.keys = item.Keys{Key: []byte("new"), PreviousKey: []byte("old")}
	e.mustConverge()
	e.rec.inOrder(t, "tool cryptsetup open", "tool cryptsetup luksChangeKey", "tool cryptsetup open")
	if string(e.tools.luksKey) != "new" {
		t.Errorf("key after rotation = %q", e.tools.luksKey)
	}
}

func TestServerDownWhenUnitFailed(t *testing.T) {
	e := newEnv(t)
	e.sd.startErr = errors.New("job failed")
	if err := e.converge(); !errors.Is(err, item.ErrServerDown) {
		t.Fatalf("err = %v, want ErrServerDown", err)
	}
	e.rec.none(t, "tool", "mount")

	// A unit that systemd is restarting is waited for, not started over.
	g := newEnv(t)
	g.seedRunning()
	g.sd.SetState(g.v.unit, systemd.State{Active: "activating", Sub: "auto-restart", Present: true})
	if err := g.converge(); !errors.Is(err, item.ErrServerDown) {
		t.Fatalf("restarting: err = %v, want ErrServerDown", err)
	}
	g.rec.none(t, "systemd.Start")

	// An active unit whose control socket names another device is not live.
	h := newEnv(t)
	h.seedRunning()
	h.ctl.index = 7
	if err := h.converge(); !errors.Is(err, item.ErrServerDown) {
		t.Fatalf("wrong device: err = %v, want ErrServerDown", err)
	}
	h.rec.none(t, "systemd.Start")
}

func TestUnitDeadDeviceAliveRestartsUnitKeepsMount(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	e.makeCache(1, "", 0, 1)
	e.sd.SetState(e.v.unit, systemd.State{}) // the unit is gone; the device, mapping and mount are not
	table := e.mnt.table()

	e.mustConverge()

	if n := e.rec.count("systemd.Start"); n != 1 {
		t.Errorf("starts = %d, want 1", n)
	}
	e.rec.none(t, "unmount", "mount", "tool", "ctl.Stop", "systemd.Stop")
	if !reflect.DeepEqual(e.mnt.table(), table) {
		t.Error("the mount table changed")
	}
	if !e.cacheExists() {
		t.Error("the node-local cache was removed: the server resumes from it")
	}
	if len(e.events) != 0 {
		t.Errorf("events = %v", e.events)
	}
}

func TestStartExecStartMismatchAdoptsNotServerDown(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	e.sd.SetState(e.v.unit, systemd.State{})
	e.sd.startErr = fmt.Errorf("unit exists: %w", systemd.ErrExecStartDiffers)
	if err := e.converge(); err != nil {
		t.Fatalf("a different ExecStart is adopted, not server-down: %v", err)
	}
}

func TestPrerequisiteMissingRefusesBeforeAnything(t *testing.T) {
	e := newEnv(t)
	obs := e.observed()
	obs.Prereq = prereq.Report{Features: []prereq.Feature{
		{Name: prereq.FeatureNBD, Err: errors.New("no nbd family")},
		{Name: prereq.FeatureCacheDir},
	}}
	err := e.it.Converge(bg, e.in, obs)
	if !errors.Is(err, item.ErrPrerequisiteMissing) || !strings.Contains(err.Error(), "nbd") {
		t.Fatalf("err = %v, want ErrPrerequisiteMissing naming nbd", err)
	}
	if got := e.rec.all(); len(got) != 0 {
		t.Errorf("calls before refusing: %v", got)
	}
	for _, p := range []string{e.v.cacheDir, e.binDir, holder.Path(e.v.holdDir, e.v.name)} {
		if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s was created", p)
		}
	}

	// The cipher and the mapper matter only to an encrypted volume.
	plain := newEnv(t)
	plain.in.Volume.Encryption = false
	plain.keys = item.Keys{}
	obs = plain.observed()
	obs.Prereq = prereq.Report{Features: []prereq.Feature{
		{Name: prereq.FeatureAdiantum, Err: errors.New("no adiantum")},
		{Name: prereq.FeatureDeviceMapper, Err: errors.New("no dm")},
	}}
	if err := plain.it.Converge(bg, plain.in, obs); err != nil {
		t.Fatalf("a plain volume needs neither: %v", err)
	}
}

func TestHolderContended(t *testing.T) {
	e := newEnv(t)
	e.holderAt(otherNode, 1)
	err := e.converge()
	if !errors.Is(err, item.ErrHolderContended) {
		t.Fatalf("err = %v, want ErrHolderContended", err)
	}
	e.rec.none(t, "systemd.Start", "tool", "mount")
	if got := e.holderInfo(); got.Node != otherNode {
		t.Errorf("holder = %+v: a contended file must be left alone", got)
	}

	// With the reconciler's verdict to take over, the volume is ours at a
	// higher epoch.
	e.judge = func(string) holder.Judgement {
		return holder.Judgement{Takeover: true, Reason: "node is out of service"}
	}
	e.mustConverge()
	if got := e.holderInfo(); got.Node != testNode || got.Epoch != 2 {
		t.Errorf("holder after takeover = %+v", got)
	}
}

func TestGrowRunsThreeSteps(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	e.ctl.size = e.v.size / 2
	e.mustConverge()
	e.rec.inOrder(t, "ctl.Grow", "tool cryptsetup resize", "tool resize2fs")
}

func TestGrowFinishesAHalfDoneGrow(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	// The device and the mapping are big enough; the file system is not.
	e.mnt.SetStatfs(e.v.stage, unix.Statfs_t{Blocks: 100, Bsize: 4096})
	e.mustConverge()
	e.rec.none(t, "ctl.Grow", "tool cryptsetup resize")
	if e.rec.count("tool resize2fs") != 1 {
		t.Errorf("calls = %v", e.rec.all())
	}
	// A file system that merely looks short is resized once, not every pass.
	e.mustConverge()
	if e.rec.count("tool resize2fs") != 1 {
		t.Errorf("resize2fs ran again: %v", e.rec.all())
	}
}

func TestTeardownOrder(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	e.makeCache(1, "", 0)
	if err := e.it.Teardown(bg, e.in, item.Observed{}); err != nil {
		t.Fatal(err)
	}
	e.rec.inOrder(t,
		"freeze", "ctl.Boundary", "thaw", "ctl.Publish unstage",
		"unmount", "tool cryptsetup close", "ctl.Stop", "systemd.Stop")
	if e.cacheExists() {
		t.Error("the cache survived the unstage")
	}
	if e.holderInfo().Present {
		t.Error("the holder file survived the unstage")
	}
	if len(e.events) != 0 {
		t.Errorf("events = %v", e.events)
	}
	// Nothing left to undo: a second teardown is quiet.
	if err := e.it.Teardown(bg, e.in, item.Observed{}); err != nil {
		t.Fatalf("second teardown: %v", err)
	}
}

func TestTeardownSucceedsWhenPublishRefused(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	e.makeCache(1, "", 0)
	e.ctl.publishRefused = true
	if err := e.it.Teardown(bg, e.in, item.Observed{}); err != nil {
		t.Fatalf("a refused final generation must not fail the unstage: %v", err)
	}
	if !slices.Equal(e.events, []string{ReasonPublishRefused}) {
		t.Errorf("events = %v", e.events)
	}
	e.rec.inOrder(t, "unmount", "tool cryptsetup close", "systemd.Stop")
	if e.cacheExists() || e.holderInfo().Present {
		t.Error("cache or holder left behind")
	}
}

func TestTeardownSucceedsWhenTransportDown(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	e.makeCache(1, "", 0)
	e.ctl.publishErr = syscall.ESTALE
	e.hold.adoptErr = errors.New("i/o timeout")
	if err := e.it.Teardown(bg, e.in, item.Observed{}); err != nil {
		t.Fatalf("a down transport must not fail the unstage: %v", err)
	}
	if !slices.Equal(e.events, []string{ReasonGenerationLost}) {
		t.Errorf("events = %v", e.events)
	}
	e.rec.inOrder(t, "unmount", "tool cryptsetup close", "systemd.Stop")
	if e.cacheExists() {
		t.Error("the cache survived")
	}
}

func TestTeardownWithTheServerGoneReportsLostWrites(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	e.makeCache(1, "", 3)
	e.sd.SetState(e.v.unit, systemd.State{})
	e.ctl.down = true
	if err := e.it.Teardown(bg, e.in, item.Observed{}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(e.events, []string{ReasonGenerationLost}) {
		t.Errorf("events = %v", e.events)
	}
	e.rec.none(t, "freeze", "ctl.Publish")
}

func TestTeardownBusyMountIsRetryable(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	e.mnt.FailOp(mountfake.OpUnmount, e.v.stage, syscall.EBUSY)
	err := e.it.Teardown(bg, e.in, item.Observed{})
	if !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("err = %v, want EBUSY", err)
	}
	e.rec.none(t, "tool cryptsetup close", "systemd.Stop", "ctl.Stop")
}

func TestTeardownLeavesAForeignMountAlone(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	e.mnt.entries = nil
	e.mnt.add(e.v.stage, "tmpfs", "tmpfs")
	err := e.it.Teardown(bg, e.in, item.Observed{})
	if !errors.Is(err, item.ErrStageConflict) {
		t.Fatalf("err = %v, want ErrStageConflict", err)
	}
	e.rec.none(t, "unmount", "systemd.Stop")
}

func TestPublishSourceIsStagePath(t *testing.T) {
	e := newEnv(t)
	got, err := e.it.PublishSource(e.in)
	if err != nil || got != e.in.StagePath {
		t.Fatalf("PublishSource = %q, %v", got, err)
	}
	if e.it.Kind() != "volume" {
		t.Errorf("Kind = %q", e.it.Kind())
	}
}

func TestStageConflictWhenSomethingElseIsMounted(t *testing.T) {
	e := newEnv(t)
	e.mnt.add(e.v.stage, "ext4", "/dev/sda1")
	if err := e.converge(); !errors.Is(err, item.ErrStageConflict) {
		t.Fatalf("err = %v, want ErrStageConflict", err)
	}
	e.rec.none(t, "unmount", "mount", "tool mkfs")
}

// ---- generations ----

func TestGenerationSequence(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	if err := e.it.Generation(bg, e.in); err != nil {
		t.Fatal(err)
	}
	want := []string{"freeze", "ctl.Boundary", "thaw", "ctl.Publish interval"}
	if got := e.rec.all(); !slices.Equal(got, want) {
		t.Errorf("calls = %v, want %v", got, want)
	}
}

func TestGenerationSkipsWhenPublishInFlight(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	e.ctl.inFlight = true
	err := e.it.Generation(bg, e.in)
	if !errors.Is(err, ErrGenerationSkipped) {
		t.Fatalf("err = %v, want ErrGenerationSkipped", err)
	}
	if got := e.rec.all(); len(got) != 0 {
		t.Errorf("a skipped generation froze or called: %v", got)
	}

	// A publish that starts between the status and the boundary is skipped too,
	// and the file system is thawed.
	e.ctl.inFlight = false
	e.ctl.boundaryErr = blockd.ErrPublishInFlight
	if err := e.it.Generation(bg, e.in); !errors.Is(err, ErrGenerationSkipped) {
		t.Fatalf("err = %v, want ErrGenerationSkipped", err)
	}
	e.rec.inOrder(t, "freeze", "ctl.Boundary", "thaw")
	e.rec.none(t, "ctl.Publish")
}

func TestGenerationThawsWhenBoundaryFails(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	e.ctl.boundaryErr = errors.New("boundary exploded")
	if err := e.it.Generation(bg, e.in); err == nil || !strings.Contains(err.Error(), "boundary exploded") {
		t.Fatalf("err = %v", err)
	}
	e.rec.inOrder(t, "freeze", "ctl.Boundary", "thaw")
	e.rec.none(t, "ctl.Publish")
	if n := e.rec.count("thaw"); n != 1 {
		t.Errorf("thaws = %d", n)
	}
}

func TestGenerationWatchdogThawsAHungBoundary(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	e.ctl.boundaryHang = make(chan struct{})
	start := time.Now()
	err := e.it.Generation(bg, e.in)
	if err == nil {
		t.Fatal("a hung boundary must fail the generation")
	}
	if time.Since(start) > 5*time.Second {
		t.Errorf("took %s: the freeze is bounded", time.Since(start))
	}
	if n := e.rec.count("thaw"); n != 1 {
		t.Errorf("thaws = %d, want exactly 1 (the watchdog and the defer share one)", n)
	}
	e.rec.none(t, "ctl.Publish")
}

func TestGenerationFreezeFailureCallsNothing(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	e.freezeE = errors.New("FIFREEZE: EBUSY")
	if err := e.it.Generation(bg, e.in); err == nil {
		t.Fatal("a failed freeze must fail the generation")
	}
	e.rec.none(t, "ctl.Boundary", "ctl.Publish")
}

func TestGenerationRefusedIsPublishRefused(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	e.ctl.publishRefused = true
	err := e.it.Generation(bg, e.in)
	if !errors.Is(err, item.ErrPublishRefused) || !errors.Is(err, blockd.ErrRefused) {
		t.Fatalf("err = %v", err)
	}
}

func TestGenerationWithTheServerDownIsServerDown(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	e.ctl.down = true
	if err := e.it.Generation(bg, e.in); !errors.Is(err, item.ErrServerDown) {
		t.Fatalf("err = %v", err)
	}
	e.rec.none(t, "freeze")
}

func TestNewRequiresItsDependencies(t *testing.T) {
	e := newEnv(t)
	for name, cfg := range map[string]Config{
		"empty":    {},
		"relative": {Root: "x", BinDir: "/b", CacheDir: "/c", Tools: e.tools, Systemd: e.sd, NBD: e.nbd, Holder: e.hold, Mounter: e.mnt, Dial: DialBlockd, Node: "n", BinaryHash: "h"},
		"bad hash": {Root: "/r", BinDir: "/b", CacheDir: "/c", Tools: e.tools, Systemd: e.sd, NBD: e.nbd, Holder: e.hold, Mounter: e.mnt, Dial: DialBlockd, Node: "n", BinaryHash: "../h"},
	} {
		if _, err := New(cfg); err == nil {
			t.Errorf("%s: New succeeded", name)
		}
	}
}
