package volume

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/holder"
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

// Decision 52's proof rule, as in an unstage: a holder file that names another
// node proves the volume was given up, so the surviving cache is discarded
// (reported) and nothing of the other node's is released.
func TestDrainDiscardsWhenTheHolderNamesAnotherNode(t *testing.T) {
	e := orphanEnv(t)
	in := e.drainIntent(t)
	e.holderAt(otherNode, 2)
	if err := e.it.Drain(bg, in); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(e.events, []string{ReasonGenerationLost}) {
		t.Errorf("events = %v", e.events)
	}
	if e.cacheExists() || e.drainRecordExists() {
		t.Error("the cache or its record is left")
	}
	if got := e.holderInfo(); got.Node != otherNode || got.Epoch != 2 {
		t.Errorf("holder = %+v: another node's file must not be released", got)
	}
	e.rec.none(t, "systemd.Start", "ctl.Publish")
}

// An absent holder is proof only with the store's current above the cache's
// epoch; with nothing to show it is not, and the cache is kept.
func TestDrainWithNoHolderNeedsProof(t *testing.T) {
	absent := func(e *env) {
		if err := os.Remove(holder.Path(e.v.holdDir, e.v.name)); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("current above the cache's epoch is proof", func(t *testing.T) {
		e := orphanEnv(t)
		in := e.drainIntent(t)
		absent(e)
		e.curName = "9.1"
		if err := e.it.Drain(bg, in); err != nil {
			t.Fatal(err)
		}
		if e.cacheExists() || !slices.Equal(e.events, []string{ReasonGenerationLost}) {
			t.Errorf("cache %t, events %v", e.cacheExists(), e.events)
		}
	})
	t.Run("a held epoch above current is not", func(t *testing.T) {
		e := orphanEnv(t)
		e.makeCache(5, "")
		in := e.drainIntent(t)
		absent(e)
		e.curName = "3.1"
		if err := e.it.Drain(bg, in); err != nil {
			t.Fatal(err)
		}
		if !e.cacheExists() {
			t.Error("the cache was discarded without proof")
		}
	})
	t.Run("no proof keeps the cache and warns once", func(t *testing.T) {
		e := orphanEnv(t)
		in := e.drainIntent(t)
		absent(e)
		for range 3 {
			if err := e.it.Drain(bg, in); err != nil {
				t.Fatal(err)
			}
		}
		if !e.cacheExists() || !e.drainRecordExists() {
			t.Error("the cache or its record was removed")
		}
		e.rec.none(t, "systemd.Start", "ctl.")
		if n := strings.Count(e.logs.String(), "level=WARN"); n != 1 {
			t.Errorf("%d warnings over 3 passes, want 1:\n%s", n, e.logs.String())
		}
		if !strings.Contains(e.logs.String(), "force-detach") {
			t.Errorf("the warning does not name the way out:\n%s", e.logs.String())
		}
	})
}

// A store directory that is missing proves nothing: a mounted but empty export
// (a dataset not yet mounted on the NAS, a recreated share, a rollback) looks
// the same as a deletion. Only the API's NotFound for the PV is proof.
func TestDrainDiscardsOnlyWithTheAPIsWordThatTheVolumeIsDeleted(t *testing.T) {
	setup := func(t *testing.T, exists func(context.Context, string) (bool, error)) (*env, intent.Intent) {
		e := orphanEnv(t)
		in := e.drainIntent(t)
		_ = os.Remove(holder.Path(e.v.holdDir, e.v.name))
		if err := os.RemoveAll(e.v.storeDir); err != nil {
			t.Fatal(err)
		}
		e.it.cfg.Current = storeCurrent // the real reader, which reports a missing store directory
		e.it.cfg.VolumeExists = exists
		mp, err := e.in.Transport.MountPoint(e.root)
		if err != nil {
			t.Fatal(err)
		}
		e.mnt.add(mp, "nfs", "nas.example.com:/export")
		return e, in
	}
	kept := func(t *testing.T, name string, exists func(context.Context, string) (bool, error)) {
		t.Run(name, func(t *testing.T) {
			e, in := setup(t, exists)
			for range 2 {
				if err := e.it.Drain(bg, in); err != nil {
					t.Fatal(err)
				}
			}
			if !e.cacheExists() || !e.drainRecordExists() || len(e.events) != 0 {
				t.Errorf("cache %t, record %t, events %v: dirty data discarded without proof", e.cacheExists(), e.drainRecordExists(), e.events)
			}
			if n := strings.Count(e.logs.String(), "level=WARN"); n != 1 {
				t.Errorf("%d warnings, want 1:\n%s", n, e.logs.String())
			}
		})
	}
	kept(t, "no seam", nil)
	kept(t, "the PV exists", func(context.Context, string) (bool, error) { return true, nil })
	// Whatever the PV is (another driver's, another handle), the seam says it
	// exists: only an explicit NotFound is proof; production's PVExists does
	// not look inside the object.
	kept(t, "the API cannot be asked", func(context.Context, string) (bool, error) { return false, errors.New("503") })

	t.Run("the PV is NotFound", func(t *testing.T) {
		e, in := setup(t, func(context.Context, string) (bool, error) { return false, nil })
		if err := e.it.Drain(bg, in); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(e.events, []string{ReasonGenerationLost}) {
			t.Errorf("events = %v", e.events)
		}
		if e.cacheExists() || e.drainRecordExists() {
			t.Error("the cache or its record is left")
		}
	})
	t.Run("the PV is NotFound but the transport is not mounted", func(t *testing.T) {
		e, in := setup(t, func(context.Context, string) (bool, error) { return false, nil })
		e.mnt.entries = nil
		e.mnt.SetTable(nil)
		if err := e.it.Drain(bg, in); err != nil || !e.cacheExists() {
			t.Fatalf("Drain = %v, cache %t", err, e.cacheExists())
		}
	})
}

// B-M2: the fence is `current` above the larger of the cache's epoch and the
// held one. A holder file regressed to an older claim of ours (held 1) under a
// cache built and published at epoch 3 is not fenced by its own epoch-3
// generation.
func TestDrainFenceUsesTheLargerOfTheIndexAndHeldEpoch(t *testing.T) {
	e := orphanEnv(t) // holder: this node, epoch 1
	e.makeCache(3, "3.1", 3)
	e.curName = "3.1"
	e.ctl.base, e.ctl.dirty = "3.1", 1<<20
	if err := e.it.Drain(bg, e.drainIntent(t)); err != nil {
		t.Fatal(err)
	}
	e.rec.inOrder(t, "systemd.Start", "ctl.Publish unstage")
	if len(e.events) != 0 {
		t.Errorf("events = %v: our own epoch-3 generation fenced us", e.events)
	}
}

// And a current above both is a fence even when the cache descends from it:
// this node built the cache on a generation another acquisition published
// (epoch 5), under an epoch-3 ownership that was then fenced.
func TestDrainFencedByAGenerationAboveBothEpochsDiscards(t *testing.T) {
	e := orphanEnv(t)
	e.makeCache(3, "5.1", 3) // the cache's base is the epoch-5 manifest it was restaged from
	e.curName = "5.1"
	if err := e.it.Drain(bg, e.drainIntent(t)); err != nil {
		t.Fatal(err)
	}
	e.rec.none(t, "systemd.Start")
	if !slices.Equal(e.events, []string{ReasonGenerationLost}) || e.cacheExists() {
		t.Errorf("events %v, cache %t", e.events, e.cacheExists())
	}
}

// B-I1: a fresh plugin process has copied no binary yet.
func TestDrainCopiesTheHostBinaryFirst(t *testing.T) {
	e := orphanEnv(t)
	if err := e.it.Drain(bg, e.drainIntent(t)); err != nil {
		t.Fatal(err)
	}
	if len(e.sd.units) == 0 {
		t.Fatal("no unit started")
	}
	if _, err := os.Stat(e.sd.units[0].ExecStart[0]); err != nil {
		t.Errorf("the drain started a unit whose binary does not exist: %v", err)
	}
}

// B-I2 (and the coordinator's ruling): the record is kept until the holder is
// released. A release that fails strands nothing: the next pass finds the
// record, releases, and only then removes it.
func TestDrainKeepsTheRecordUntilTheReleaseSucceeds(t *testing.T) {
	skipIfRoot(t)
	e := orphanEnv(t)
	in := e.drainIntent(t)
	e.it.cfg.Systemd = chmodAfterStop{e.sd, e.v.holdDir}
	t.Cleanup(func() { _ = os.Chmod(e.v.holdDir, 0o750) }) //nolint:gosec // a directory in the test's temporary tree
	err := e.it.Drain(bg, in)
	_ = os.Chmod(e.v.holdDir, 0o750) //nolint:gosec // a directory in the test's temporary tree
	if !errors.Is(err, item.ErrReleasePending) || errors.Is(err, item.ErrUnstageIncomplete) {
		t.Fatalf("Drain = %v, want a retryable release failure", err)
	}
	if !e.holderInfo().Present || !e.drainRecordExists() {
		t.Fatalf("holder %t, record %t: the volume is stranded", e.holderInfo().Present, e.drainRecordExists())
	}
	if !e.orphan(t) {
		t.Fatal("the record of a volume with no cache is not found by the next pass")
	}
	// The next pass: no cache, so release if ours, then drop the record.
	e.it.cfg.Systemd = e.sd
	if err := e.it.Drain(bg, e.drainIntent(t)); err != nil {
		t.Fatal(err)
	}
	if e.holderInfo().Present || e.drainRecordExists() {
		t.Errorf("holder %t, record %t after the retry", e.holderInfo().Present, e.drainRecordExists())
	}
}

// A volume with no cache whose holder is another node's: only the record goes.
func TestDrainWithNoCacheLeavesAnotherNodesHolder(t *testing.T) {
	e := orphanEnv(t)
	in := e.drainIntent(t)
	e.holderAt(otherNode, 2)
	if err := os.RemoveAll(e.v.cacheDir); err != nil {
		t.Fatal(err)
	}
	if err := e.it.Drain(bg, in); err != nil {
		t.Fatal(err)
	}
	if e.drainRecordExists() || e.holderInfo().Node != otherNode {
		t.Errorf("record %t, holder %+v", e.drainRecordExists(), e.holderInfo())
	}
}

// B-I3: a failed attempt adopts the holder again; the earlier descriptor is
// closed, not leaked.
func TestDrainRetryDoesNotLeakHolderDescriptors(t *testing.T) {
	e := orphanEnv(t)
	in := e.drainIntent(t)
	e.ctl.publishErr = errors.New("the transport hung")
	count := func() int { ents, _ := os.ReadDir("/proc/self/fd"); return len(ents) }
	_ = e.it.Drain(bg, in)
	before := count()
	for range 20 {
		_ = e.it.Drain(bg, in)
	}
	if after := count(); after-before >= 15 {
		t.Errorf("fds %d -> %d over 20 failed drains", before, after)
	}
}

type chmodAfterStop struct {
	*fakeSystemd
	dir string
}

func (c chmodAfterStop) Stop(ctx context.Context, name string) error {
	err := c.fakeSystemd.Stop(ctx, name)
	_ = os.Chmod(c.dir, 0o555) //nolint:gosec // a directory in the test's temporary tree
	return err
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
	// A record whose cache is gone is still an orphan (the holder may remain
	// to be released); a corrupt record and a record under the wrong name are
	// skipped and left.
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
	if len(orphans) != 1 || orphans[0].VolumeID != e.in.VolumeID {
		t.Errorf("orphans = %+v, want the volume whose cache is gone", orphans)
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

// B-I2 for an unstage: a release that fails is retryable and keeps the record.
func TestTeardownReleaseFailureIsRetryableAndKeepsTheRecord(t *testing.T) {
	skipIfRoot(t)
	e := dirtyEnv(t)
	e.mustConverge() // writes the drain record
	e.it.cfg.Systemd = chmodAfterStop{e.sd, e.v.holdDir}
	t.Cleanup(func() { _ = os.Chmod(e.v.holdDir, 0o750) }) //nolint:gosec // a directory in the test's temporary tree
	err := e.it.Teardown(bg, e.in, item.Observed{})
	_ = os.Chmod(e.v.holdDir, 0o750) //nolint:gosec // a directory in the test's temporary tree
	if !errors.Is(err, item.ErrReleasePending) {
		t.Fatalf("Teardown = %v, want a retryable release failure", err)
	}
	if !e.holderInfo().Present || !e.drainRecordExists() {
		t.Fatalf("holder %t, record %t", e.holderInfo().Present, e.drainRecordExists())
	}
	e.it.cfg.Systemd = e.sd
	if err := e.it.Teardown(bg, e.in, item.Observed{}); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if e.holderInfo().Present || e.drainRecordExists() {
		t.Error("holder or record left after the retry")
	}
}

// The release is made to fail with a read-only directory, which root ignores.
func skipIfRoot(t *testing.T) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("a read-only directory does not stop root")
	}
}

// R2: a failed release after a verified publish says so, and does not suggest
// that anything is unproven or that the force-detach is the way out.
func TestReleaseFailureMessageSaysTheDataIsInTheStore(t *testing.T) {
	skipIfRoot(t)
	e := orphanEnv(t)
	in := e.drainIntent(t)
	e.it.cfg.Systemd = chmodAfterStop{e.sd, e.v.holdDir}
	t.Cleanup(func() { _ = os.Chmod(e.v.holdDir, 0o750) }) //nolint:gosec // a directory in the test's temporary tree
	err := e.it.Drain(bg, in)
	_ = os.Chmod(e.v.holdDir, 0o750) //nolint:gosec // a directory in the test's temporary tree
	if !errors.Is(err, item.ErrReleasePending) || errors.Is(err, item.ErrUnstageIncomplete) {
		t.Fatalf("Drain = %v", err)
	}
	msg := err.Error()
	// The whole text: no claim that anything is unproven, nothing about a way out.
	prefix := "volume item " + e.in.VolumeID + ": the holder file could not be released yet: the data is in the store; it will be retried: "
	if !strings.HasPrefix(msg, prefix) {
		t.Errorf("message = %q, want prefix %q", msg, prefix)
	}
	for _, bad := range []string{"cannot yet prove", "cannot prove", "force-detach", "annotation", "unstage"} {
		if strings.Contains(msg, bad) {
			t.Errorf("message %q contains %q", msg, bad)
		}
	}
}

// F2: the holder file is there and the store directory is not. With the PV
// present nothing is proven and the volume waits; with the PV NotFound the
// volume was deleted and the cache is discarded.
func TestDrainWithAHolderAndNoStoreDirectoryNeedsTheAPIsWord(t *testing.T) {
	for _, exists := range []bool{true, false} {
		e := orphanEnv(t)
		in := e.drainIntent(t)
		if err := os.RemoveAll(e.v.storeDir); err != nil {
			t.Fatal(err)
		}
		e.it.cfg.Current = storeCurrent
		e.it.cfg.VolumeExists = func(context.Context, string) (bool, error) { return exists, nil }
		mp, err := e.in.Transport.MountPoint(e.root)
		if err != nil {
			t.Fatal(err)
		}
		e.mnt.add(mp, "nfs", "nas.example.com:/export")
		err = e.it.Drain(bg, in)
		if exists {
			if !errors.Is(err, item.ErrUnstageIncomplete) || !e.cacheExists() || len(e.events) != 0 {
				t.Errorf("PV present: err %v, cache %t, events %v", err, e.cacheExists(), e.events)
			}
			continue
		}
		if err != nil || e.cacheExists() || !slices.Equal(e.events, []string{ReasonGenerationLost}) {
			t.Errorf("PV NotFound: err %v, cache %t, events %v", err, e.cacheExists(), e.events)
		}
	}
}
