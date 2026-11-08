package volume

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/greghaskins/csi-miami/internal/item"
	"github.com/greghaskins/csi-miami/internal/sizerec"
	"github.com/greghaskins/csi-miami/internal/systemd"
	kinds "github.com/greghaskins/csi-miami/internal/volume"
)

const mi = 1 << 20

// testFootprint is the footprint of the test volume at testSize: 8 MiB of
// cache, a 4 MiB dirty watermark and 16 MiB of index slack.
const testFootprint = 28 * mi

// budgetOf makes the node's cache budget n bytes.
func (e *env) budgetOf(n int64) {
	e.it.cfg.CacheBudget = func(context.Context) (int64, bool, error) { return n, true, nil }
}

// noWork asserts a refused or untouched stage acquired and wrote nothing.
func (e *env) noWork(t *testing.T) {
	t.Helper()
	e.rec.none(t, "systemd.Start", "holder.Acquire", "tool", "mount")
	if len(e.sd.units) != 0 {
		t.Errorf("units started = %d", len(e.sd.units))
	}
	if e.cacheExists() {
		t.Error("a cache directory was made")
	}
	if e.holderInfo().Present {
		t.Error("a holder file was written")
	}
	if e.drainRecordExists() {
		t.Error("a drain record was written")
	}
}

// otherRecord writes the drain record of another volume that this node
// serves, with the test volume's parameters.
func (e *env) otherRecord(t *testing.T, n int, mutate func(*kinds.VolumeParams)) string {
	t.Helper()
	name := fmt.Sprintf("pvc-00000000-0000-4000-8000-%012d", 100+n)
	in := e.in
	in.VolumeID = "volume:" + name
	p := *e.in.Volume
	if mutate != nil {
		mutate(&p)
	}
	in.Volume = &p
	b, err := drainBytes(in)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(e.cacheDir, name+drainSuffix)
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestFreshStageUnderBudgetProceeds(t *testing.T) {
	e := newEnv(t)
	e.budgetOf(testFootprint)
	e.mustConverge()
	if len(e.events) != 0 {
		t.Errorf("events = %v", e.events)
	}
}

func TestFreshStageOverBudgetRefused(t *testing.T) {
	e := newEnv(t)
	e.otherRecord(t, 1, nil)
	e.otherRecord(t, 2, nil)
	e.budgetOf(2*testFootprint + testFootprint - mi)
	err := e.converge()
	if !errors.Is(err, item.ErrCacheBudgetExceeded) {
		t.Fatalf("Converge = %v, want ErrCacheBudgetExceeded", err)
	}
	for _, want := range []string{"cache budget is 83Mi", "56Mi is committed to 2 volumes", "needs 28Mi"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q lacks %q", err, want)
		}
	}
	e.noWork(t)
	if len(e.events) != 1 || e.events[0] != ReasonCacheBudgetExceeded {
		t.Errorf("events = %v", e.events)
	}
}

func TestAdoptNeverRefused(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	e.budgetOf(1)
	e.mustConverge()
}

func TestServerRestartNeverRefused(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	e.sd.SetState(e.v.unit, systemdGone())
	e.budgetOf(1)
	e.mustConverge()
}

func TestRebootWithSurvivingCacheNeverRefused(t *testing.T) {
	e := newEnv(t)
	e.holderAt(testNode, 1)
	e.makeCache(1, "")
	e.budgetOf(1)
	e.mustConverge()
	if !e.cacheExists() {
		t.Error("the surviving cache is gone")
	}
}

func TestDrainNeverRefused(t *testing.T) {
	e := orphanEnv(t)
	e.budgetOf(1)
	if err := e.it.Drain(bg, e.drainIntent(t)); errors.Is(err, item.ErrCacheBudgetExceeded) {
		t.Fatal(err)
	}
}

func TestTeardownNeverRefused(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	e.budgetOf(1)
	if err := e.it.Teardown(bg, e.in, item.Observed{Mounts: e.mnt.table()}); errors.Is(err, item.ErrCacheBudgetExceeded) {
		t.Fatal(err)
	}
}

func TestExpandedVolumeIsCheckedAtItsRaisedSize(t *testing.T) {
	e := newEnv(t)
	if _, err := sizerec.Grow(e.v.holdDir, e.v.name, 3*testSize); err != nil {
		t.Fatal(err)
	}
	// 3x the cache: 24 + 12 + 16 = 52 MiB.
	e.budgetOf(testFootprint)
	err := e.converge()
	if !errors.Is(err, item.ErrCacheBudgetExceeded) {
		t.Fatalf("Converge = %v, want ErrCacheBudgetExceeded", err)
	}
	if !strings.Contains(err.Error(), "needs 52Mi") {
		t.Errorf("error %q does not name the raised need", err)
	}
	e.noWork(t)
}

func TestExpandedByManifestIsCheckedAtItsRaisedSize(t *testing.T) {
	e := newEnv(t)
	e.writeManifest("1.1", 3*testSize)
	e.curName = "1.1"
	e.budgetOf(testFootprint)
	err := e.converge()
	if !errors.Is(err, item.ErrCacheBudgetExceeded) || !strings.Contains(err.Error(), "needs 52Mi") {
		t.Fatalf("Converge = %v", err)
	}
}

func TestStoreReadErrorIsNotARefusal(t *testing.T) {
	e := newEnv(t)
	if err := os.WriteFile(sizerec.Path(e.v.holdDir, e.v.name), []byte("garbage\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	e.budgetOf(1)
	err := e.converge()
	if !errors.Is(err, sizerec.ErrMalformed) || errors.Is(err, item.ErrCacheBudgetExceeded) {
		t.Fatalf("Converge = %v, want the size record's own error", err)
	}

	e = newEnv(t)
	e.writeManifest("1.1", testSize)
	if err := os.WriteFile(filepath.Join(e.v.storeDir, "manifests", "1.1"), []byte("not a manifest"), 0o600); err != nil {
		t.Fatal(err)
	}
	e.curName = "1.1"
	e.budgetOf(1)
	err = e.converge()
	if err == nil || errors.Is(err, item.ErrCacheBudgetExceeded) {
		t.Fatalf("Converge = %v, want the manifest's own error", err)
	}
}

func TestCommittedListingErrorFailsOpen(t *testing.T) {
	skipIfRoot(t)
	e := newEnv(t)
	e.budgetOf(1)
	// The cache directory can be entered but not listed.
	if err := os.Chmod(e.cacheDir, 0o300); err != nil { //nolint:gosec // a directory in the test's temporary tree
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(e.cacheDir, 0o750) }) //nolint:gosec // as above
	if _, _, err := e.it.Committed(bg); err == nil {
		t.Fatal("the listing did not fail")
	}
	if err := e.it.checkBudget(bg, e.v); err != nil {
		t.Fatalf("checkBudget = %v, want it to fail open", err)
	}
	if !strings.Contains(e.logs.String(), "committed cache could not be read") {
		t.Errorf("the log does not say why:\n%s", e.logs.String())
	}
}

func TestCommittedKeepsRecordOfDiscardedCache(t *testing.T) {
	e := newEnv(t)
	e.otherRecord(t, 1, nil)
	// A discarded cache keeps its record: the cache directory is gone.
	n, count, err := e.it.Committed(bg)
	if err != nil || n != testFootprint || count != 1 {
		t.Fatalf("Committed = %d, %d, %v", n, count, err)
	}
}

func TestCommittedCountsDrainRecordsOfUnstagedCaches(t *testing.T) {
	e := orphanEnv(t) // a record, a cache, no intent
	n, count, err := e.it.Committed(bg)
	if err != nil || n != testFootprint || count != 1 {
		t.Fatalf("Committed = %d, %d, %v", n, count, err)
	}
}

func TestCommittedSkipsUnusableRecordWithWarning(t *testing.T) {
	e := newEnv(t)
	e.otherRecord(t, 1, nil)
	if err := os.WriteFile(filepath.Join(e.cacheDir, "pvc-00000000-0000-4000-8000-000000000999"+drainSuffix), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		n, count, err := e.it.Committed(bg)
		if err != nil || n != testFootprint || count != 1 {
			t.Fatalf("Committed = %d, %d, %v", n, count, err)
		}
	}
	if got := strings.Count(e.logs.String(), "not usable"); got != 1 {
		t.Errorf("warned %d times, want once:\n%s", got, e.logs.String())
	}
}

// A valid volume is at most 16 TiB (65536 chunks of 256 MiB), so the sum
// cannot overflow with real records; AddFootprint's saturation is pinned by its
// own tests. What is checked here is the largest valid volumes, summed exactly
// and refused against a budget one MiB short.
func TestCommittedSumsTheLargestVolumesAndRefusesAtTheEdge(t *testing.T) {
	e := newEnv(t)
	huge := func(p *kinds.VolumeParams) { p.Size, p.CacheSize, p.ChunkSize = 65536<<28, 65536<<28, 1<<28 }
	e.otherRecord(t, 1, huge)
	e.otherRecord(t, 2, huge)
	want := 2 * kinds.CacheFootprint(kinds.VolumeParams{Size: 65536 << 28, CacheSize: 65536 << 28})
	n, count, err := e.it.Committed(bg)
	if err != nil || n != want || count != 2 {
		t.Fatalf("Committed = %d, %d, %v, want %d\n%s", n, count, err, want, e.logs.String())
	}
	e.budgetOf(want + testFootprint - mi)
	if err := e.converge(); !errors.Is(err, item.ErrCacheBudgetExceeded) {
		t.Fatalf("Converge = %v, want a refusal", err)
	}
	e.budgetOf(want + testFootprint)
	e.mustConverge()
}

func TestRefusedThenAdmittedAfterUnstage(t *testing.T) {
	e := newEnv(t)
	a := e.otherRecord(t, 1, nil)
	e.otherRecord(t, 2, nil)
	e.budgetOf(2*testFootprint + mi)
	if err := e.converge(); !errors.Is(err, item.ErrCacheBudgetExceeded) {
		t.Fatalf("Converge = %v, want a refusal", err)
	}
	// One of the others is torn down: its release removes its record.
	if err := os.Remove(a); err != nil {
		t.Fatal(err)
	}
	e.mustConverge()
}

func TestBudgetZeroRefusesEveryFreshStage(t *testing.T) {
	e := newEnv(t)
	e.budgetOf(0)
	if err := e.converge(); !errors.Is(err, item.ErrCacheBudgetExceeded) {
		t.Fatalf("Converge = %v, want a refusal", err)
	}
	e.noWork(t)
}

func TestAccountingOffSkipsCheck(t *testing.T) {
	e := newEnv(t)
	e.it.cfg.CacheBudget = nil
	e.mustConverge()

	e = newEnv(t)
	e.it.cfg.CacheBudget = func(context.Context) (int64, bool, error) { return 0, false, nil }
	e.mustConverge()
}

func TestBudgetErrorFailsOpenWithLog(t *testing.T) {
	e := newEnv(t)
	e.it.cfg.CacheBudget = func(context.Context) (int64, bool, error) { return 0, true, errors.New("statfs: boom") }
	e.mustConverge()
	if !strings.Contains(e.logs.String(), "statfs: boom") {
		t.Errorf("the log does not say why the check was skipped:\n%s", e.logs.String())
	}
}

func systemdGone() systemd.State { return systemd.State{} }
