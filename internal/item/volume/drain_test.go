package volume

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/item"
	"github.com/greghaskins/csi-miami/internal/systemd"
)

// ---- decision 52: a node drains what it holds and no longer stages ----

// orphanEnv is the state after a crash and a reboot whose pod moved: this node
// holds the volume, a cache with unpublished writes survived, nothing is
// running, nothing is mounted, and the intent record is gone. Only the drain
// record the item wrote while it served the volume says where the volume is.
func orphanEnv(t *testing.T) *env {
	t.Helper()
	e := newEnv(t)
	e.holderAt(testNode, 1)
	if err := e.it.ensureBinary(); err != nil {
		t.Fatal(err)
	}
	e.makeCache(1, "", 3, 4)
	e.ctl.dirty = 2 << 20
	e.ctl.down = true
	e.it.cfg.PublishTimeout = 150 * time.Millisecond
	if err := e.it.saveDrainRecord(e.v); err != nil {
		t.Fatal(err)
	}
	e.rec.reset()
	e.events = nil
	return e
}

func (e *env) orphan(t *testing.T) (found bool) {
	t.Helper()
	orphans, err := e.it.Orphans(bg)
	if err != nil {
		t.Fatal(err)
	}
	for _, in := range orphans {
		if in.VolumeID == e.in.VolumeID {
			return true
		}
	}
	return false
}

func (e *env) drainRecordExists() bool {
	_, err := os.Stat(e.it.drainRecordPath(e.v))
	return err == nil
}

func TestDrainPublishesVerifiesAndReleases(t *testing.T) {
	e := orphanEnv(t)
	if !e.orphan(t) {
		t.Fatal("a surviving cache with a drain record is not reported as an orphan")
	}
	orphans, _ := e.it.Orphans(bg)
	if got := orphans[0]; got.VolumeID != e.in.VolumeID || got.Transport != e.in.Transport || got.Kind != e.in.Kind {
		t.Errorf("the orphan's intent = %+v", got)
	}
	if err := e.it.Drain(bg, orphans[0]); err != nil {
		t.Fatal(err)
	}
	// Server first, then the publish, verified against the store, then the
	// stop, and only then the cache and the holder go.
	e.rec.inOrder(t, "systemd.Start", "ctl.Boundary", "ctl.Publish unstage", "current", "ctl.Stop", "systemd.Stop")
	e.rec.none(t, "tool", "mount", "unmount")
	if e.atStop != [2]bool{true, true} {
		t.Errorf("cache/holder present at systemd.Stop = %v, want both", e.atStop)
	}
	if e.curName == "" {
		t.Error("nothing was published")
	}
	if e.cacheExists() || e.holderInfo().Present || e.drainRecordExists() {
		t.Errorf("left behind: cache %t, holder %t, record %t", e.cacheExists(), e.holderInfo().Present, e.drainRecordExists())
	}
	if _, err := os.Stat(e.v.cacheDir + ".lock"); !os.IsNotExist(err) {
		t.Error("the cache lock file is left")
	}
	if len(e.events) != 0 {
		t.Errorf("events = %v", e.events)
	}
	if e.orphan(t) {
		t.Error("a drained volume is still an orphan")
	}
	// A server that needs neither a key nor a mount: the unit carries no key.
	if len(e.sd.units) != 1 {
		t.Fatalf("units started = %d", len(e.sd.units))
	}
	if strings.Contains(strings.Join(e.sd.units[0].ExecStart, " "), "key") {
		t.Errorf("the drain's unit mentions a key: %v", e.sd.units[0].ExecStart)
	}
	if !strings.Contains(e.logs.String(), "crash image") {
		t.Errorf("the log does not say the drained generation is a crash image:\n%s", e.logs.String())
	}
}

func TestDrainOfACleanCacheVerifiesTheBase(t *testing.T) {
	e := orphanEnv(t)
	e.makeCache(1, "1.1")
	e.curName = "1.1"
	e.ctl.dirty, e.ctl.base = 0, "1.1"
	if err := e.it.Drain(bg, e.drainIntent(t)); err != nil {
		t.Fatal(err)
	}
	if e.cacheExists() || e.holderInfo().Present {
		t.Error("the cache or the holder survived a clean drain")
	}
}

// The same decision as a stage: a cache the store has moved past is not served.
func TestDrainDiscardsACacheTheStoreHasMovedPast(t *testing.T) {
	e := orphanEnv(t)
	e.curName = "9.1" // another node's generation at epoch 9; this cache is epoch 1 from nothing
	if err := e.it.Drain(bg, e.drainIntent(t)); err != nil {
		t.Fatal(err)
	}
	e.rec.none(t, "systemd.Start", "ctl.", "systemd.Stop")
	if !slices.Equal(e.events, []string{ReasonGenerationLost}) {
		t.Errorf("events = %v", e.events)
	}
	if !strings.Contains(e.logs.String(), "2097152") {
		t.Errorf("the discarded byte count is not in the log:\n%s", e.logs.String())
	}
	if e.cacheExists() || e.drainRecordExists() {
		t.Error("the cache or its record is left")
	}
	// The holder file names this node and nothing here is worth holding for,
	// so it is released (Release removes the file only if it is still ours).
	if e.holderInfo().Present {
		t.Error("an unneeded holder file is left, stranding the volume")
	}
}

func TestDrainDiscardLeavesAnotherNodesHolderAlone(t *testing.T) {
	e := orphanEnv(t)
	in := e.drainIntent(t)
	e.curName = "9.1"
	e.holderAt(otherNode, 2) // an operator's detach already moved the volume
	if err := e.it.Drain(bg, in); err != nil {
		t.Fatal(err)
	}
	// Not ours: nothing of ours is touched, the cache is left for the node's
	// own stage decision to deal with.
	if got := e.holderInfo(); got.Node != otherNode {
		t.Errorf("holder = %+v", got)
	}
	e.rec.none(t, "systemd.Start", "ctl.")
}

// The fence is what an operator's detach landing mid-drain meets: the publish
// is refused, the data is given up, and the new holder's file is untouched.
func TestDrainRefusedByTheFenceDiscardsAndReleasesNothing(t *testing.T) {
	e := orphanEnv(t)
	in := e.drainIntent(t)
	e.ctl.onPublish = func() {
		e.holderAt(otherNode, 2) // the force-detach lands between the boundary and the publish
		e.ctl.publishRefused = true
	}
	if err := e.it.Drain(bg, in); err != nil {
		t.Fatalf("a drain the fence refused discards and succeeds: %v", err)
	}
	if !slices.Equal(e.events, []string{ReasonGenerationLost}) {
		t.Errorf("events = %v", e.events)
	}
	if got := e.holderInfo(); got.Node != otherNode || got.Epoch != 2 {
		t.Errorf("holder = %+v: the new holder's file must not be released", got)
	}
	e.rec.inOrder(t, "ctl.Publish unstage", "ctl.Stop", "systemd.Stop")
	if e.cacheExists() || e.drainRecordExists() {
		t.Error("the cache or its record is left")
	}
}

// A refusal the store does not explain is not an abandonment.
func TestDrainRefusalWithoutProofLeavesEverything(t *testing.T) {
	e := orphanEnv(t)
	in := e.drainIntent(t)
	e.ctl.publishRefused = true
	err := e.it.Drain(bg, in)
	if err == nil {
		t.Fatal("an unexplained refusal drained")
	}
	if !e.cacheExists() || !e.holderInfo().Present || !e.drainRecordExists() {
		t.Error("something was removed on a refusal with no proof")
	}
	if len(e.events) != 0 {
		t.Errorf("events = %v", e.events)
	}
}

func TestDrainFailureLeavesEverythingAndRetries(t *testing.T) {
	e := orphanEnv(t)
	in := e.drainIntent(t)
	e.ctl.publishErr = errors.New("the transport hung")
	err := e.it.Drain(bg, in)
	if !errors.Is(err, item.ErrUnstageIncomplete) {
		t.Fatalf("err = %v, want a retryable incomplete drain", err)
	}
	if !e.cacheExists() || !e.holderInfo().Present || !e.drainRecordExists() {
		t.Error("something was removed before the data was verified in the store")
	}
	e.rec.none(t, "systemd.Stop")

	// The retry finds the server it started still running, with nothing
	// mounted on it, and finishes.
	e.ctl.publishErr = nil
	e.rec.reset()
	if err := e.it.Drain(bg, in); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if e.cacheExists() || e.holderInfo().Present {
		t.Error("the retry left the cache or the holder")
	}
}

func TestDrainRefusesWhenSomethingUsesTheVolume(t *testing.T) {
	for name, setup := range map[string]func(e *env){
		"the stage path is mounted": func(e *env) { e.mnt.add(e.v.stage, "ext4", "/dev/nbd0") },
		"a mount is made from its device": func(e *env) {
			e.attach()
			e.mnt.add("/var/lib/kubelet/pods/x", "ext4", e.it.devPath(0))
		},
		"its mapping is open": func(e *env) {
			e.attach()
			e.setDM(true)
		},
	} {
		t.Run(name, func(t *testing.T) {
			e := orphanEnv(t)
			in := e.drainIntent(t)
			setup(e)
			e.sd.SetState(e.v.unit, systemd.State{Active: "active", Sub: "running", MainPID: 42, Present: true})
			e.rec.reset()
			if err := e.it.Drain(bg, in); err == nil {
				t.Fatal("drained a volume that is in use")
			}
			e.rec.none(t, "systemd.Start", "systemd.Stop", "ctl.", "nbd.Disconnect", "unmount")
			if !e.cacheExists() || !e.holderInfo().Present {
				t.Error("something was removed")
			}
		})
	}
}

func TestDrainLeavesAVolumeThatIsNotOursAlone(t *testing.T) {
	t.Run("the holder names another node", func(t *testing.T) {
		e := orphanEnv(t)
		in := e.drainIntent(t)
		e.holderAt(otherNode, 2)
		if err := e.it.Drain(bg, in); err != nil {
			t.Fatal(err)
		}
		if !e.cacheExists() || e.holderInfo().Node != otherNode {
			t.Error("the cache or another node's holder was touched")
		}
		e.rec.none(t, "systemd.Start", "ctl.")
	})
	t.Run("there is no holder file", func(t *testing.T) {
		e := orphanEnv(t)
		in := e.drainIntent(t)
		_ = os.Remove(filepath.Join(e.v.holdDir, e.v.name+".holder"))
		if err := e.it.Drain(bg, in); err != nil {
			t.Fatal(err)
		}
		// Absence proves nothing (the transport may not be mounted): the
		// cache stays.
		if !e.cacheExists() {
			t.Error("the cache was removed on an absent holder file")
		}
		e.rec.none(t, "systemd.Start", "ctl.")
	})
}

func TestDrainWithAnUnreadableIndexLeavesTheCache(t *testing.T) {
	e := orphanEnv(t)
	in := e.drainIntent(t)
	if err := os.WriteFile(filepath.Join(e.v.cacheDir, "index.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := e.it.Drain(bg, in); err == nil {
		t.Fatal("drained over an unreadable index")
	}
	if !e.cacheExists() || !e.holderInfo().Present {
		t.Error("something was removed")
	}
	e.rec.none(t, "systemd.Start")
}

// The server is started with the geometry of the cache it serves, not of a
// record that may be older (the volume was grown since).
func TestDrainServesTheCacheWithItsOwnGeometry(t *testing.T) {
	e := orphanEnv(t)
	in := e.drainIntent(t)
	in.Volume.Size *= 2
	if err := e.it.Drain(bg, in); err != nil {
		t.Fatal(err)
	}
	args := e.sd.units[0].ExecStart
	i := slices.Index(args, "-size")
	if i < 0 || args[i+1] != strconv.FormatInt(testSize, 10) {
		t.Errorf("unit args = %v, want -size %d", args, testSize)
	}
}

// ---- the record ----

func TestConvergeWritesTheDrainRecordAndTeardownRemovesIt(t *testing.T) {
	e := newEnv(t)
	e.mustConverge()
	if !e.drainRecordExists() {
		t.Fatal("serving a volume leaves no drain record")
	}
	b, _ := os.ReadFile(e.it.drainRecordPath(e.v))
	if strings.Contains(string(b), "targetPath") || strings.Contains(string(b), "the-key") {
		t.Errorf("the record carries more than it should:\n%s", b)
	}
	e.mustConverge() // idempotent
	if err := e.it.Teardown(bg, e.in, item.Observed{}); err != nil {
		t.Fatal(err)
	}
	if e.drainRecordExists() {
		t.Error("a clean unstage left its drain record")
	}
}

func TestOrphansIgnoresWhatIsNotADrainableCache(t *testing.T) {
	e := orphanEnv(t)
	// A record whose cache is gone is stale and is removed; a corrupt record
	// and a record under the wrong name are skipped and left.
	if err := os.RemoveAll(e.v.cacheDir); err != nil {
		t.Fatal(err)
	}
	stray := filepath.Join(e.cacheDir, "pvc-00000000-0000-4000-8000-0000000000ff.drain.json")
	if err := os.WriteFile(stray, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	wrong := filepath.Join(e.cacheDir, "pvc-00000000-0000-4000-8000-0000000000fe.drain.json")
	b, _ := os.ReadFile(e.it.drainRecordPath(e.v))
	if err := os.WriteFile(wrong, b, 0o600); err != nil { //nolint:gosec // a temporary directory
		t.Fatal(err)
	}
	orphans, err := e.it.Orphans(bg)
	if err != nil {
		t.Fatal(err)
	}
	if len(orphans) != 0 {
		t.Errorf("orphans = %+v", orphans)
	}
	if e.drainRecordExists() {
		t.Error("the stale record was kept")
	}
	for _, p := range []string{stray, wrong} {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("%s was removed: %v", p, err)
		}
	}
}

// drainIntent is what the reconciler hands Drain: the record's intent.
func (e *env) drainIntent(t *testing.T) (in intent.Intent) {
	t.Helper()
	orphans, err := e.it.Orphans(bg)
	if err != nil || len(orphans) != 1 {
		t.Fatalf("Orphans = %+v, %v", orphans, err)
	}
	return orphans[0]
}
