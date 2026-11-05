package volume

import (
	"errors"
	"os"
	"slices"
	"strconv"
	"testing"

	"github.com/greghaskins/csi-miami/internal/holder"
	"github.com/greghaskins/csi-miami/internal/item"
)

// epochOfLastStart is the -epoch of the newest unit started.
func (e *env) epochOfLastStart() uint64 {
	e.t.Helper()
	if len(e.sd.units) == 0 {
		e.t.Fatal("no unit was started")
	}
	args := e.sd.units[len(e.sd.units)-1].ExecStart
	n, err := strconv.ParseUint(args[slices.Index(args, "-epoch")+1], 10, 64)
	if err != nil {
		e.t.Fatal(err)
	}
	return n
}

// The rows of the sanity table of the cache-reuse design: each is a sequence
// of events and the one outcome the node must reach.
func TestRebootSurvivingCacheReusedOnlyIfCurrentMatches(t *testing.T) {
	t.Run("plugin restart with the unit alive makes no cache decision", func(t *testing.T) {
		e := newEnv(t)
		e.seedRunning()
		// An index that the predicate would discard (a base, and no current):
		// it survives because nothing is started.
		e.makeCache(1, "1.1", 2)
		e.newItem()
		e.mustConverge()
		if !e.cacheExists() {
			t.Error("the cache was removed under a live server")
		}
		e.rec.none(t, "systemd.Start")
	})

	t.Run("reboot, nobody else touched it: reuse", func(t *testing.T) {
		e := newEnv(t)
		e.holderAt(testNode, 3)
		e.makeCache(3, "3.2", 4)
		e.curName = "3.2"
		e.mustConverge()
		if !e.cacheExists() {
			t.Fatal("the cache was discarded though current is its lineage")
		}
		if got := e.epochOfLastStart(); got != 3 {
			t.Errorf("started at epoch %d, want 3", got)
		}
		if len(e.events) != 0 {
			t.Errorf("events = %v", e.events)
		}
	})

	t.Run("partition, another node took over and published: held, then discard", func(t *testing.T) {
		e := newEnv(t)
		e.holderAt(testNode, 3)
		e.holderAt(otherNode, 5)
		e.makeCache(3, "3.2", 4)
		e.curName = "5.1"
		if err := e.converge(); !errors.Is(err, item.ErrHolderContended) {
			t.Fatalf("err = %v, want ErrHolderContended", err)
		}
		e.rec.none(t, "systemd.Start", "tool", "mount")
		if !e.cacheExists() {
			t.Fatal("a held volume's cache must not be touched")
		}

		// The other node releases; we acquire above its epoch, and current is
		// not our cache's lineage.
		if err := os.Remove(holder.Path(e.v.holdDir, e.v.name)); err != nil {
			t.Fatal(err)
		}
		e.mustConverge()
		if e.cacheExists() {
			t.Error("a cache that current does not descend from was reused")
		}
		if got := e.epochOfLastStart(); got <= 5 {
			t.Errorf("started at epoch %d, want above 5", got)
		}
		if !slices.Contains(e.events, ReasonGenerationLost) {
			t.Errorf("events = %v: discarding unpublished chunks must be reported", e.events)
		}
	})

	t.Run("the holder file regressed to our older epoch: re-acquire above current, discard", func(t *testing.T) {
		e := newEnv(t)
		e.holderAt(testNode, 3)
		e.holderAt(otherNode, 5)
		e.regressHolder(testNode, 3) // our late renewal landed over theirs
		e.makeCache(3, "3.2", 4)     // equal to the held epoch: the unsafe test would reuse it
		e.curName = "5.1"
		e.mustConverge()

		e.rec.inOrder(t, "holder.Read", "holder.Adopt", "current", "holder.Read", "holder.Acquire", "systemd.Start")
		if e.cacheExists() {
			t.Error("a stale lineage was reused because the held epoch equalled the cache's")
		}
		info := e.holderInfo()
		if info.Node != testNode || info.Epoch <= 5 {
			t.Errorf("holder = %+v: must be re-acquired above current's epoch", info)
		}
		if got := e.epochOfLastStart(); got != info.Epoch {
			t.Errorf("started at epoch %d, holder says %d", got, info.Epoch)
		}
	})

	t.Run("a live server at the fenced epoch is stopped and restarted at the new one", func(t *testing.T) {
		e := newEnv(t)
		e.seedRunning() // file: us@1, server running at 1
		e.holderAt(otherNode, 3)
		e.regressHolder(testNode, 1)
		e.curName = "3.1"
		e.mustConverge()

		e.rec.inOrder(t,
			"holder.Adopt", "current",
			"unmount", "tool cryptsetup close", "ctl.Stop", "systemd.Stop",
			"holder.Acquire", "systemd.Start",
			"tool cryptsetup open", "mount ext4 /dev/mapper/miami-"+e.v.name)
		if e.cacheExists() {
			t.Error("the fenced cache was kept")
		}
		if got := e.epochOfLastStart(); got <= 3 {
			t.Errorf("restarted at epoch %d, want above 3", got)
		}
	})

	t.Run("another node took over and never published: re-acquire, reuse", func(t *testing.T) {
		e := newEnv(t)
		e.holderAt(testNode, 3)
		e.holderAt(otherNode, 4)
		if err := os.Remove(holder.Path(e.v.holdDir, e.v.name)); err != nil { // it was drained
			t.Fatal(err)
		}
		e.makeCache(3, "3.2", 4)
		e.curName = "3.2"
		e.mustConverge()
		if !e.cacheExists() {
			t.Error("our cache is still the newest data anywhere and was discarded")
		}
		if got := e.epochOfLastStart(); got <= 4 {
			t.Errorf("started at epoch %d, want above 4", got)
		}
	})

	t.Run("decision-32 fork: we hold nothing, so nothing is staged", func(t *testing.T) {
		e := newEnv(t)
		e.holderAt(testNode, 3)
		e.holderAt(otherNode, 4)
		e.makeCache(3, "3.2")
		e.curName = "3.2" // the other node's 4.1 is orphaned
		if err := e.converge(); !errors.Is(err, item.ErrHolderContended) {
			t.Fatalf("err = %v, want ErrHolderContended", err)
		}
		e.rec.none(t, "systemd.Start", "mount")
		if !e.cacheExists() {
			t.Error("the cache was touched")
		}
	})

	t.Run("no current and a cache that descends from nothing: reuse", func(t *testing.T) {
		e := newEnv(t)
		e.holderAt(testNode, 2)
		e.makeCache(2, "", 1)
		e.mustConverge()
		if !e.cacheExists() {
			t.Error("a never-published volume's cache was discarded")
		}
	})

	t.Run("no current but a cache with a base: discard", func(t *testing.T) {
		e := newEnv(t)
		e.holderAt(testNode, 2)
		e.makeCache(2, "2.1")
		e.mustConverge()
		if e.cacheExists() {
			t.Error("a cache whose base vanished from the store was reused")
		}
	})

	t.Run("a directory with no index is cleared", func(t *testing.T) {
		e := newEnv(t)
		if err := os.MkdirAll(e.v.cacheDir, 0o750); err != nil {
			t.Fatal(err)
		}
		e.mustConverge()
		if e.cacheExists() {
			t.Error("a half-made cache directory was left")
		}
	})
}

// The comparison with current is only sound if current is read after the
// holder is adopted or acquired: a takeover landing between the two reads is
// what that order defends against.
func TestCurrentIsReadAfterTheHolderCall(t *testing.T) {
	check := func(t *testing.T, e *env, holderCall string) {
		t.Helper()
		calls := e.rec.all()
		h, c := slices.Index(calls, holderCall), slices.Index(calls, "current")
		if h < 0 || c < 0 || h > c {
			t.Fatalf("%s at %d, current at %d in %v", holderCall, h, c, calls)
		}
		if n := e.rec.count("current"); n != 1 {
			t.Errorf("current read %d times in a pass, want 1 (and fresh each pass)", n)
		}
	}
	t.Run("acquire", func(t *testing.T) {
		e := newEnv(t)
		e.mustConverge()
		check(t, e, "holder.Acquire")
	})
	t.Run("adopt", func(t *testing.T) {
		e := newEnv(t)
		e.seedRunning()
		e.newItem()
		e.mustConverge()
		check(t, e, "holder.Adopt")
	})
	t.Run("every pass reads it again", func(t *testing.T) {
		e := newEnv(t)
		e.mustConverge()
		e.rec.reset()
		e.mustConverge()
		if n := e.rec.count("current"); n != 1 {
			t.Errorf("second pass read current %d times", n)
		}
	})
}

func TestFenceRuleIsTheItemsNotTheReconcilers(t *testing.T) {
	e := newEnv(t)
	e.judge = func(string) holder.Judgement {
		t.Error("the reconciler's judge was consulted for our own fenced file")
		return holder.Judgement{}
	}
	info := holder.Info{Node: testNode, Epoch: 3, Present: true}
	j := e.it.judge(e.v, info, current{name: "5.1", ok: true, epoch: 5})
	if !j.Takeover || j.Reason != "own ownership fenced by a newer generation" || j.Info != info {
		t.Errorf("judgement = %+v", j)
	}
	// Not fenced, or someone else's: the reconciler decides.
	e.judge = func(string) holder.Judgement { return holder.Judgement{Reason: "reconciler"} }
	for name, tc := range map[string]struct {
		info holder.Info
		cur  current
	}{
		"current at our epoch": {info, current{ok: true, epoch: 3}},
		"no current":           {info, current{}},
		"another node's file":  {holder.Info{Node: otherNode, Epoch: 3, Present: true}, current{ok: true, epoch: 5}},
	} {
		if j := e.it.judge(e.v, tc.info, tc.cur); j.Takeover || j.Reason != "reconciler" {
			t.Errorf("%s: judgement = %+v", name, j)
		}
	}
}

func TestReuseTable(t *testing.T) {
	for _, tc := range []struct {
		name  string
		epoch uint64
		base  string
		cur   current
		want  bool
	}{
		{"current at the cache's epoch", 3, "3.2", current{ok: true, epoch: 3}, true},
		{"current at another epoch", 3, "3.2", current{ok: true, epoch: 5}, false},
		{"current at an older epoch", 3, "3.2", current{ok: true, epoch: 2}, false},
		{"no current, no base", 3, "", current{}, true},
		{"no current, a base", 3, "3.2", current{}, false},
		{"current is still the base the cache was built from", 2, "1.3", current{name: "1.3", ok: true, epoch: 1}, true},
	} {
		idx := cacheIndex(tc.epoch, tc.base)
		if got := reuse(idx, tc.cur); got != tc.want {
			t.Errorf("%s: reuse = %t, want %t", tc.name, got, tc.want)
		}
	}
}
