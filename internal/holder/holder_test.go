package holder

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
)

// env gives a test a directory and a server clock it sets.
type env struct {
	t   *testing.T
	dir string
	now time.Time
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{t: t, dir: t.TempDir(), now: time.Now()}
	// The package seams are process-wide: these tests do not run in parallel.
	oldProbe, oldHook := probe, hook
	probe = func(string) (time.Time, error) { return e.now, nil }
	t.Cleanup(func() { probe, hook = oldProbe, oldHook })
	return e
}

func (e *env) age(name string, age time.Duration) {
	e.t.Helper()
	mt := e.now.Add(-age)
	if err := os.Chtimes(Path(e.dir, name), mt, mt); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) names() []string {
	ents, err := os.ReadDir(e.dir)
	if err != nil {
		e.t.Fatal(err)
	}
	var out []string
	for _, d := range ents {
		out = append(out, d.Name())
	}
	return out
}

// holderFiles is the directory listing with the durable epoch-claim files
// filtered out, so a test can assert that the holder file stands alone (its
// temporary file removed) without listing the claims beside it.
func (e *env) holderFiles() []string {
	var out []string
	for _, n := range e.names() {
		if !strings.Contains(n, ".holder.epoch-") {
			out = append(out, n)
		}
	}
	return out
}

// claimedEpochs is the sorted set of epochs that have a durable claim file.
func (e *env) claimedEpochs(name string) []uint64 {
	prefix := name + ".holder.epoch-"
	var out []uint64
	for _, n := range e.names() {
		if !strings.HasPrefix(n, prefix) {
			continue
		}
		v, err := strconv.ParseUint(n[len(prefix):], 10, 64)
		if err != nil {
			e.t.Fatalf("unparsable claim file %q", n)
		}
		out = append(out, v)
	}
	slices.Sort(out)
	return out
}

// judgeTakeover reads name afresh and builds a takeover judgement bound to the
// holder as it stands now, the way the reconciler does via Judge.
func (e *env) judgeTakeover(name string) Judgement {
	e.t.Helper()
	info, err := Read(ctx, e.dir, name)
	if err != nil {
		e.t.Fatal(err)
	}
	return Judgement{Takeover: true, Reason: "test", Info: info}
}

func (e *env) raw(name string) string {
	b, err := os.ReadFile(Path(e.dir, name))
	if err != nil {
		e.t.Fatal(err)
	}
	return string(b)
}

var (
	ctx  = context.Background()
	keep = Judgement{Reason: "test"}
)

func TestAcquireCreatesWithEpochOne(t *testing.T) {
	e := newEnv(t)
	h, err := Acquire(ctx, e.dir, "vol", "kind-worker", keep)
	if err != nil {
		t.Fatal(err)
	}
	if h.Epoch() != 1 {
		t.Errorf("Epoch = %d, want 1", h.Epoch())
	}
	if got, want := e.raw("vol"), `{"node":"kind-worker","epoch":1,"renewals":0}`+"\n"; got != want {
		t.Errorf("content = %q, want %q", got, want)
	}
	if got := e.holderFiles(); !slices.Equal(got, []string{"vol.holder"}) {
		t.Errorf("directory holds %v, want only the holder file (the temporary file is removed)", got)
	}
	if got := e.claimedEpochs("vol"); !slices.Equal(got, []uint64{1}) {
		t.Errorf("claimed epochs = %v, want [1]", got)
	}
	if ok, err := h.Mine(ctx); err != nil || !ok {
		t.Errorf("Mine = %v, %v", ok, err)
	}
}

func TestAcquireLosesTheLinkRace(t *testing.T) {
	e := newEnv(t)
	hook = func(point string) {
		if point == "created" {
			hook = func(string) {}
			if _, err := Acquire(ctx, e.dir, "vol", "kind-worker2", keep); err != nil {
				t.Errorf("the other node: %v", err)
			}
		}
	}
	_, err := Acquire(ctx, e.dir, "vol", "kind-worker", keep)
	if !errors.Is(err, ErrHeld) || !strings.Contains(err.Error(), "kind-worker2") {
		t.Errorf("err = %v, want ErrHeld naming kind-worker2", err)
	}
	if got := e.holderFiles(); !slices.Equal(got, []string{"vol.holder"}) {
		t.Errorf("directory holds %v", got)
	}
}

func TestAcquireRefusesLiveHolder(t *testing.T) {
	e := newEnv(t)
	if _, err := Acquire(ctx, e.dir, "vol", "kind-worker", keep); err != nil {
		t.Fatal(err)
	}
	_, err := Acquire(ctx, e.dir, "vol", "kind-worker2", Judgement{Reason: "fresh"})
	if !errors.Is(err, ErrHeld) {
		t.Fatalf("err = %v, want ErrHeld", err)
	}
	if !strings.Contains(err.Error(), "kind-worker at epoch 1") {
		t.Errorf("err = %q, want the holder's node and epoch", err)
	}
	if !strings.Contains(e.raw("vol"), `"kind-worker"`) {
		t.Errorf("the holder file changed: %q", e.raw("vol"))
	}
}

func TestTakeoverIncrementsEpoch(t *testing.T) {
	e := newEnv(t)
	first, err := Acquire(ctx, e.dir, "vol", "kind-worker", keep)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Renew(ctx); err != nil {
		t.Fatal(err)
	}
	second, err := Acquire(ctx, e.dir, "vol", "kind-worker2", e.judgeTakeover("vol"))
	if err != nil {
		t.Fatal(err)
	}
	if second.Epoch() != 2 {
		t.Errorf("Epoch = %d, want 2", second.Epoch())
	}
	if got, want := e.raw("vol"), `{"node":"kind-worker2","epoch":2,"renewals":0}`+"\n"; got != want {
		t.Errorf("content = %q, want %q", got, want)
	}
	if ok, _ := first.Mine(ctx); ok {
		t.Error("the first holder still reports the file as its own")
	}
	if got := e.holderFiles(); !slices.Equal(got, []string{"vol.holder"}) {
		t.Errorf("directory holds %v", got)
	}
	if got := e.claimedEpochs("vol"); !slices.Equal(got, []uint64{1, 2}) {
		t.Errorf("claimed epochs = %v, want [1 2] (the previous epoch is kept to bound the window)", got)
	}
}

func TestRenewRewritesAtomically(t *testing.T) {
	e := newEnv(t)
	h, err := Acquire(ctx, e.dir, "vol", "kind-worker", keep)
	if err != nil {
		t.Fatal(err)
	}
	e.age("vol", time.Minute)
	seen := ""
	hook = func(point string) {
		if point == "renew-written" {
			// Between the write and the rename, a reader sees the old whole file.
			seen = e.raw("vol")
		}
	}
	if err := h.Renew(ctx); err != nil {
		t.Fatal(err)
	}
	if want := `{"node":"kind-worker","epoch":1,"renewals":0}` + "\n"; seen != want {
		t.Errorf("a reader mid-renewal saw %q, want the old content %q", seen, want)
	}
	if got, want := e.raw("vol"), `{"node":"kind-worker","epoch":1,"renewals":1}`+"\n"; got != want {
		t.Errorf("content = %q, want %q", got, want)
	}
	if got := e.holderFiles(); !slices.Equal(got, []string{"vol.holder"}) {
		t.Errorf("directory holds %v", got)
	}
	st, err := os.Stat(Path(e.dir, "vol"))
	if err != nil || time.Since(st.ModTime()) > time.Hour*24 || st.ModTime().Equal(e.now.Add(-time.Minute)) {
		t.Errorf("the renewal did not refresh the modification time: %v, %v", st, err)
	}
}

func TestRenewDetectsLoss(t *testing.T) {
	e := newEnv(t)
	h, err := Acquire(ctx, e.dir, "vol", "kind-worker", keep)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Acquire(ctx, e.dir, "vol", "kind-worker2", e.judgeTakeover("vol")); err != nil {
		t.Fatal(err)
	}
	if err := h.Renew(ctx); !errors.Is(err, ErrLost) {
		t.Fatalf("Renew after a takeover = %v, want ErrLost", err)
	}
	if !strings.Contains(e.raw("vol"), `"kind-worker2"`) {
		t.Errorf("the loser overwrote the winner: %q", e.raw("vol"))
	}
	// Lost stays lost, even if the file would look ours again.
	if err := os.WriteFile(Path(e.dir, "vol"), []byte(`{"node":"kind-worker","epoch":1,"renewals":0}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := h.Renew(ctx); !errors.Is(err, ErrLost) {
		t.Errorf("second Renew = %v, want ErrLost", err)
	}
	if ok, _ := h.Mine(ctx); ok {
		t.Error("Mine is true on a lost holder")
	}
}

func TestRenewDetectsATakeoverRacingTheRename(t *testing.T) {
	e := newEnv(t)
	h, err := Acquire(ctx, e.dir, "vol", "kind-worker", keep)
	if err != nil {
		t.Fatal(err)
	}
	hook = func(point string) {
		if point == "renew-renamed" {
			hook = func(string) {}
			// A taker's rename lands right after ours.
			if err := os.WriteFile(Path(e.dir, "vol"), []byte(`{"node":"kind-worker2","epoch":2,"renewals":0}`+"\n"), 0o600); err != nil {
				t.Error(err)
			}
		}
	}
	if err := h.Renew(ctx); !errors.Is(err, ErrLost) {
		t.Fatalf("Renew = %v, want ErrLost", err)
	}
	if err := h.Renew(ctx); !errors.Is(err, ErrLost) {
		t.Errorf("second Renew = %v, want ErrLost", err)
	}
}

// I2: a takeover that lands between writing the new content and the rename
// must stop the losing holder before it renames, so it never overwrites the
// new owner's file.
func TestRenewDetectsLossBeforeTheRename(t *testing.T) {
	e := newEnv(t)
	h, err := Acquire(ctx, e.dir, "vol", "kind-worker", keep)
	if err != nil {
		t.Fatal(err)
	}
	hook = func(point string) {
		if point == "renew-written" {
			hook = func(string) {}
			// A taker's rename lands after we wrote our temp but before our own
			// rename.
			if err := os.WriteFile(Path(e.dir, "vol"), []byte(`{"node":"kind-worker2","epoch":2,"renewals":0}`+"\n"), 0o600); err != nil {
				t.Error(err)
			}
		}
	}
	if err := h.Renew(ctx); !errors.Is(err, ErrLost) {
		t.Fatalf("Renew = %v, want ErrLost", err)
	}
	// The loser did not overwrite the new owner: the file still names kind-worker2.
	if got := e.raw("vol"); !strings.Contains(got, `"kind-worker2"`) || strings.Contains(got, `"renewals":1`) {
		t.Errorf("the loser overwrote the new owner at the rename: %q", got)
	}
}

// I1: the judgement is bound to the holder it judged. A renewal landing
// between the judge read and the break means the holder is no longer the one
// judged stale, so Acquire must refuse rather than break a now-live holder.
func TestAcquireRefusesWhenTheJudgedHolderChanged(t *testing.T) {
	e := newEnv(t)
	h, err := Acquire(ctx, e.dir, "vol", "kind-worker", keep)
	if err != nil {
		t.Fatal(err)
	}
	// The reconciler judged the holder as it was before this renewal.
	stale := e.judgeTakeover("vol")
	if err := h.Renew(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = Acquire(ctx, e.dir, "vol", "kind-worker2", stale)
	if !errors.Is(err, ErrHeld) {
		t.Fatalf("Acquire against a stale judgement = %v, want ErrHeld", err)
	}
	if got := e.raw("vol"); !strings.Contains(got, `"kind-worker"`) || !strings.Contains(got, `"renewals":1`) {
		t.Errorf("the live holder was altered: %q", got)
	}
	if ok, err := h.Mine(ctx); err != nil || !ok {
		t.Errorf("the live holder lost its volume: Mine = %v, %v", ok, err)
	}
	// A takeover judgement with no holder recorded is also refused.
	if _, err := Acquire(ctx, e.dir, "vol", "kind-worker2", Judgement{Takeover: true, Reason: "no info"}); !errors.Is(err, ErrHeld) {
		t.Errorf("Acquire with an empty judged holder = %v, want ErrHeld", err)
	}
}

// I3: Release keeps the durable claims, so the next acquire continues above
// the highest epoch ever claimed rather than resetting to 1; the epoch never
// goes backwards across a release and reacquire cycle.
func TestEpochSurvivesReleaseAndNeverResets(t *testing.T) {
	e := newEnv(t)
	h1, err := Acquire(ctx, e.dir, "vol", "kind-worker", keep)
	if err != nil || h1.Epoch() != 1 {
		t.Fatalf("first acquire: epoch %d, %v", h1.Epoch(), err)
	}
	if err := h1.Release(ctx); err != nil {
		t.Fatal(err)
	}
	h2, err := Acquire(ctx, e.dir, "vol", "kind-worker", keep)
	if err != nil {
		t.Fatal(err)
	}
	if h2.Epoch() != 2 {
		t.Errorf("acquire after release: epoch %d, want 2 (never reset to 1)", h2.Epoch())
	}
	if err := h2.Release(ctx); err != nil {
		t.Fatal(err)
	}
	h3, err := Acquire(ctx, e.dir, "vol", "kind-worker", keep)
	if err != nil {
		t.Fatal(err)
	}
	if h3.Epoch() != 3 {
		t.Errorf("third acquire: epoch %d, want 3", h3.Epoch())
	}
	// After acquiring epoch 3, claims older than epoch 2 are pruned.
	if got := e.claimedEpochs("vol"); !slices.Equal(got, []uint64{2, 3}) {
		t.Errorf("claimed epochs = %v, want [2 3] (older claims pruned)", got)
	}
}

// I3: two takers racing for the same holder get distinct epochs via the
// durable claim, and the one whose rename lands first wins; the epoch never
// goes backwards.
func TestConcurrentTakeoversGetDistinctEpochs(t *testing.T) {
	e := newEnv(t)
	first, err := Acquire(ctx, e.dir, "vol", "kind-worker", keep)
	if err != nil {
		t.Fatal(err)
	}
	judged := e.judgeTakeover("vol")
	var third *Holder
	var thirdErr error
	hook = func(point string) {
		if point == "claimed" {
			hook = func(string) {}
			// A third node takes over after we claimed our epoch but before our
			// rename. It claims the next epoch above ours and installs first.
			third, thirdErr = Acquire(ctx, e.dir, "vol", "kind-worker3", judged)
		}
	}
	_, err = Acquire(ctx, e.dir, "vol", "kind-worker2", judged)
	if !errors.Is(err, ErrHeld) || !strings.Contains(err.Error(), "never decreases") {
		t.Fatalf("the slower taker = %v, want ErrHeld naming the epoch rule", err)
	}
	if thirdErr != nil {
		t.Fatalf("the faster taker: %v", thirdErr)
	}
	if third.Epoch() != 3 {
		t.Errorf("the faster taker's epoch = %d, want 3 (above the loser's claim of 2)", third.Epoch())
	}
	if got := e.raw("vol"); !strings.Contains(got, `"kind-worker3"`) || !strings.Contains(got, `"epoch":3`) {
		t.Errorf("the winner is not installed: %q", got)
	}
	if ok, _ := first.Mine(ctx); ok {
		t.Error("the original holder still owns the file after a takeover")
	}
}

func TestReleaseOnlyIfMine(t *testing.T) {
	e := newEnv(t)
	first, _ := Acquire(ctx, e.dir, "vol", "kind-worker", keep)
	second, err := Acquire(ctx, e.dir, "vol", "kind-worker2", e.judgeTakeover("vol"))
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(e.raw("vol"), `"kind-worker2"`) {
		t.Fatal("Release removed or changed a file that was not ours")
	}
	if err := second.Release(ctx); err != nil {
		t.Fatal(err)
	}
	// The holder file is gone, but the durable epoch claims stay so a later
	// acquire cannot reissue an epoch.
	if got := e.holderFiles(); len(got) != 0 {
		t.Errorf("holder file still present after Release: %v", got)
	}
	if got := e.claimedEpochs("vol"); !slices.Equal(got, []uint64{1, 2}) {
		t.Errorf("claimed epochs = %v after Release, want [1 2] kept", got)
	}
	if err := second.Release(ctx); err != nil {
		t.Errorf("Release of an absent file = %v, want nil", err)
	}
	if err := second.Renew(ctx); !errors.Is(err, ErrLost) {
		t.Errorf("Renew after Release = %v, want ErrLost", err)
	}
}

func TestReadAgeUsesProbeClock(t *testing.T) {
	e := newEnv(t)
	if info, err := Read(ctx, e.dir, "vol"); err != nil || info.Present {
		t.Fatalf("Read of an absent file = %+v, %v", info, err)
	}
	if _, err := Acquire(ctx, e.dir, "vol", "kind-worker", keep); err != nil {
		t.Fatal(err)
	}
	e.age("vol", 95*time.Second)
	info, err := Read(ctx, e.dir, "vol")
	if err != nil {
		t.Fatal(err)
	}
	if want := (Info{Node: "kind-worker", Epoch: 1, Age: 95 * time.Second, Present: true}); info != want {
		t.Errorf("Read = %+v, want %+v", info, want)
	}
	// The local clock is not consulted: move the server's.
	e.now = e.now.Add(time.Hour)
	if info, _ = Read(ctx, e.dir, "vol"); info.Age != time.Hour+95*time.Second {
		t.Errorf("Age = %v, want the server's clock to drive it", info.Age)
	}
	// A holder newer than the probe cannot have a negative age.
	e.now = e.now.Add(-2 * time.Hour)
	if info, _ = Read(ctx, e.dir, "vol"); info.Age != 0 {
		t.Errorf("Age = %v, want 0", info.Age)
	}
}

func TestEpochNeverDecreases(t *testing.T) {
	e := newEnv(t)
	if _, err := Acquire(ctx, e.dir, "vol", "kind-worker", keep); err != nil {
		t.Fatal(err)
	}
	// Judged at epoch 1; before our rename another node takes over and moves
	// the file to epoch 3.
	judged := e.judgeTakeover("vol")
	hook = func(point string) {
		if point == "written" {
			hook = func(string) {}
			if err := os.WriteFile(Path(e.dir, "vol"), []byte(`{"node":"kind-worker3","epoch":3,"renewals":0}`+"\n"), 0o600); err != nil {
				t.Error(err)
			}
		}
	}
	_, err := Acquire(ctx, e.dir, "vol", "kind-worker2", judged)
	if !errors.Is(err, ErrHeld) || !strings.Contains(err.Error(), "never decreases") {
		t.Fatalf("err = %v, want a refusal naming the epoch rule", err)
	}
	if got := e.raw("vol"); !strings.Contains(got, `"epoch":3`) {
		t.Errorf("the file went backwards: %q", got)
	}
	if got := e.holderFiles(); !slices.Equal(got, []string{"vol.holder"}) {
		t.Errorf("directory holds %v", got)
	}
}

func TestMineRereadsEveryCall(t *testing.T) {
	e := newEnv(t)
	h, _ := Acquire(ctx, e.dir, "vol", "kind-worker", keep)
	if ok, _ := h.Mine(ctx); !ok {
		t.Fatal("not mine at first")
	}
	if err := os.Remove(Path(e.dir, "vol")); err != nil {
		t.Fatal(err)
	}
	if ok, err := h.Mine(ctx); ok || err != nil {
		t.Errorf("Mine with the file gone = %v, %v; want false, nil", ok, err)
	}
}

func TestCorruptFileIsAnErrorNotAnAbsence(t *testing.T) {
	// Unparsable content, an empty node and a zero epoch are each corruption:
	// Read and Acquire must error, never treat the holder as absent (which
	// would let a second node create a fresh epoch-1 holder beside a live one).
	corrupt := map[string]string{
		"junk":       "junk",
		"empty node": `{"node":"","epoch":2,"renewals":0}` + "\n",
		"zero epoch": `{"node":"kind-worker","epoch":0,"renewals":0}` + "\n",
	}
	for name, content := range corrupt {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			if err := os.WriteFile(Path(e.dir, "vol"), []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := Read(ctx, e.dir, "vol"); err == nil {
				t.Errorf("Read of %s succeeded, want an error", name)
			}
			j := Judgement{Takeover: true, Reason: "x"}
			if _, err := Acquire(ctx, e.dir, "vol", "kind-worker2", j); err == nil {
				t.Errorf("Acquire over %s succeeded, want an error", name)
			}
		})
	}
}

func TestAcquireRejectsBadArguments(t *testing.T) {
	e := newEnv(t)
	for _, c := range [][2]string{{"", "kind-worker"}, {"a/b", "kind-worker"}, {"vol", ""}} {
		if _, err := Acquire(ctx, e.dir, c[0], c[1], keep); err == nil {
			t.Errorf("Acquire(%q, %q) succeeded", c[0], c[1])
		}
	}
}

func TestPathIsBesideTheVolume(t *testing.T) {
	if got, want := Path("/x", "vol"), filepath.Join("/x", "vol.holder"); got != want {
		t.Errorf("Path = %q, want %q", got, want)
	}
}

func TestRealAcquirer(t *testing.T) {
	e := newEnv(t)
	var a Acquirer = Real{}
	h, err := a.Acquire(ctx, e.dir, "vol", "kind-worker", keep)
	if err != nil {
		t.Fatal(err)
	}
	info, err := a.Read(ctx, e.dir, "vol")
	if err != nil || info.Epoch != h.Epoch() || !info.Present {
		t.Errorf("Read = %+v, %v", info, err)
	}
}
