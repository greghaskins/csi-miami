package holder

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
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
	if got, want := e.raw("vol"), `{"node":"kind-worker","epoch":1}`+"\n"; got != want {
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
	if got, want := e.raw("vol"), `{"node":"kind-worker2","epoch":2}`+"\n"; got != want {
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

func TestRenewTouchesInPlace(t *testing.T) {
	e := newEnv(t)
	h, err := Acquire(ctx, e.dir, "vol", "kind-worker", keep)
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(Path(e.dir, "vol"))
	if err != nil {
		t.Fatal(err)
	}
	e.age("vol", time.Minute)
	if err := h.Renew(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(Path(e.dir, "vol"))
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Error("a renewal replaced the file; it must touch it in place")
	}
	if got, want := e.raw("vol"), `{"node":"kind-worker","epoch":1}`+"\n"; got != want {
		t.Errorf("a renewal changed the content: %q, want %q", got, want)
	}
	if got := e.holderFiles(); !slices.Equal(got, []string{"vol.holder"}) {
		t.Errorf("directory holds %v", got)
	}
	if time.Since(after.ModTime()) > time.Hour*24 || after.ModTime().Equal(e.now.Add(-time.Minute)) {
		t.Errorf("the renewal did not refresh the modification time: %v", after.ModTime())
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
	if err := os.WriteFile(Path(e.dir, "vol"), []byte(`{"node":"kind-worker","epoch":1}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := h.Renew(ctx); !errors.Is(err, ErrLost) {
		t.Errorf("second Renew = %v, want ErrLost", err)
	}
	if ok, _ := h.Mine(ctx); ok {
		t.Error("Mine is true on a lost holder")
	}
}

// A takeover is a new inode renamed over the name. A renewal that has checked
// the file and is about to touch it must leave the taker's file as it is, and
// is told it lost.
func TestRenewRacingATakeoverLeavesTheTakersFileIntact(t *testing.T) {
	e := newEnv(t)
	h, err := Acquire(ctx, e.dir, "vol", "kind-worker", keep)
	if err != nil {
		t.Fatal(err)
	}
	hook = func(point string) {
		if point == "renew-checked" {
			hook = func(string) {}
			if _, err := Acquire(ctx, e.dir, "vol", "kind-worker2", e.judgeTakeover("vol")); err != nil {
				t.Error(err)
			}
			e.age("vol", time.Hour) // the taker's file is old, so a touch of it would show
		}
	}
	if err := h.Renew(ctx); !errors.Is(err, ErrLost) {
		t.Fatalf("Renew = %v, want ErrLost", err)
	}
	if got, want := e.raw("vol"), `{"node":"kind-worker2","epoch":2}`+"\n"; got != want {
		t.Errorf("the taker's file changed: %q, want %q", got, want)
	}
	if st, err := os.Stat(Path(e.dir, "vol")); err != nil || time.Since(st.ModTime()) < 30*time.Minute {
		t.Errorf("the late renewal touched the taker's file: %v, %v", st, err)
	}
	if err := h.Renew(ctx); !errors.Is(err, ErrLost) {
		t.Errorf("second Renew = %v, want ErrLost", err)
	}
	if ok, _ := h.Mine(ctx); ok {
		t.Error("Mine is true on a lost holder")
	}
}

// The name is another inode with our very content: not ours to touch.
func TestRenewFindsTheNameOnAnotherInode(t *testing.T) {
	e := newEnv(t)
	h, err := Acquire(ctx, e.dir, "vol", "kind-worker", keep)
	if err != nil {
		t.Fatal(err)
	}
	f := h.fd
	tmp := filepath.Join(e.dir, "copy")
	if err := os.WriteFile(tmp, []byte(e.raw("vol")), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, Path(e.dir, "vol")); err != nil {
		t.Fatal(err)
	}
	if err := h.Renew(ctx); !errors.Is(err, ErrLost) {
		t.Fatalf("Renew = %v, want ErrLost", err)
	}
	if _, err := f.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Errorf("the descriptor of a lost holder is still open: %v", err)
	}
}

// ESTALE on the touch is what an NFS client says of a replaced inode.
func TestRenewEstaleIsLost(t *testing.T) {
	e := newEnv(t)
	h, err := Acquire(ctx, e.dir, "vol", "kind-worker", keep)
	if err != nil {
		t.Fatal(err)
	}
	old := touch
	t.Cleanup(func() { touch = old })
	touch = func(*os.File) error { return syscall.ESTALE }
	f := h.fd
	if err := h.Renew(ctx); !errors.Is(err, ErrLost) {
		t.Fatalf("Renew = %v, want ErrLost", err)
	}
	if _, err := f.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Errorf("the descriptor of a lost holder is still open: %v", err)
	}
	touch = old
	if err := h.Renew(ctx); !errors.Is(err, ErrLost) {
		t.Errorf("Renew after ESTALE = %v, want ErrLost (lost stays lost)", err)
	}
}

// Any other failure of the touch is the transport's, not a loss.
func TestRenewOtherTouchErrorIsNotLoss(t *testing.T) {
	e := newEnv(t)
	h, err := Acquire(ctx, e.dir, "vol", "kind-worker", keep)
	if err != nil {
		t.Fatal(err)
	}
	old := touch
	t.Cleanup(func() { touch = old })
	touch = func(*os.File) error { return syscall.EIO }
	if err := h.Renew(ctx); !errors.Is(err, syscall.EIO) || errors.Is(err, ErrLost) {
		t.Fatalf("Renew = %v, want EIO", err)
	}
	touch = old
	if err := h.Renew(ctx); err != nil {
		t.Errorf("Renew after a transient error = %v", err)
	}
}

func TestDescriptorClosedOnReleaseAndClose(t *testing.T) {
	e := newEnv(t)
	h, err := Acquire(ctx, e.dir, "vol", "kind-worker", keep)
	if err != nil {
		t.Fatal(err)
	}
	f := h.fd
	if f == nil {
		t.Fatal("an acquired holder keeps no descriptor")
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Errorf("Release left the descriptor open: %v", err)
	}
	if err := h.Renew(ctx); !errors.Is(err, ErrLost) {
		t.Errorf("Renew after Release = %v", err)
	}

	a, err := Acquire(ctx, e.dir, "vol", "kind-worker", keep)
	if err != nil {
		t.Fatal(err)
	}
	ad, err := Adopt(ctx, e.dir, "vol", "kind-worker")
	if err != nil {
		t.Fatal(err)
	}
	af := ad.fd
	if err := ad.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ad.Close(); err != nil {
		t.Errorf("second Close = %v", err)
	}
	if _, err := af.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Errorf("Close left the descriptor open: %v", err)
	}
	// Closing an adopter's descriptor leaves the ownership in the store.
	if ok, _ := a.Mine(ctx); !ok {
		t.Error("Close gave the ownership up")
	}
	// A takeover closes nothing of the loser's until it finds out; a failed
	// acquire holds no descriptor.
	if _, err := Acquire(ctx, e.dir, "vol", "kind-worker2", keep); !errors.Is(err, ErrHeld) {
		t.Fatal(err)
	}
}

// A loser's late renewal can no longer put its own claim back: the whole
// regression the rename-over renewal had.
func TestLateRenewalCannotRegressTheFile(t *testing.T) {
	e := newEnv(t)
	h, err := Acquire(ctx, e.dir, "vol", "kind-worker", keep)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Acquire(ctx, e.dir, "vol", "kind-worker2", e.judgeTakeover("vol")); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := h.Renew(ctx); !errors.Is(err, ErrLost) {
			t.Fatalf("Renew = %v, want ErrLost", err)
		}
	}
	if got, want := e.raw("vol"), `{"node":"kind-worker2","epoch":2}`+"\n"; got != want {
		t.Errorf("content = %q, want %q", got, want)
	}
}

// I1: the judgement is bound to the holder it judged. A takeover by a third
// node between the judge read and the break means the holder is no longer the
// one judged, so Acquire must refuse rather than break the new owner.
func TestAcquireRefusesWhenTheJudgedHolderChanged(t *testing.T) {
	e := newEnv(t)
	h, err := Acquire(ctx, e.dir, "vol", "kind-worker", keep)
	if err != nil {
		t.Fatal(err)
	}
	// Two nodes judged the holder as it was.
	stale := e.judgeTakeover("vol")
	if _, err := Acquire(ctx, e.dir, "vol", "kind-worker2", stale); err != nil {
		t.Fatal(err)
	}
	_, err = Acquire(ctx, e.dir, "vol", "kind-worker3", stale)
	if !errors.Is(err, ErrHeld) {
		t.Fatalf("Acquire against a stale judgement = %v, want ErrHeld", err)
	}
	if got := e.raw("vol"); !strings.Contains(got, `"kind-worker2"`) {
		t.Errorf("the new owner was altered: %q", got)
	}
	if ok, _ := h.Mine(ctx); ok {
		t.Error("the first holder still reports the file as its own")
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

func TestAdoptContinuesTheExistingEpoch(t *testing.T) {
	e := newEnv(t)
	orig, err := Acquire(ctx, e.dir, "vol", "kind-worker", keep)
	if err != nil {
		t.Fatal(err)
	}
	if err := orig.Renew(ctx); err != nil {
		t.Fatal(err)
	}
	before := e.raw("vol")
	claims := e.claimedEpochs("vol")
	h, err := Adopt(ctx, e.dir, "vol", "kind-worker")
	if err != nil {
		t.Fatal(err)
	}
	if h.Epoch() != orig.Epoch() {
		t.Errorf("Epoch = %d, want %d", h.Epoch(), orig.Epoch())
	}
	if e.raw("vol") != before || !slices.Equal(e.claimedEpochs("vol"), claims) {
		t.Error("Adopt changed the holder file or the epoch claims")
	}
	if ok, err := h.Mine(ctx); err != nil || !ok {
		t.Errorf("Mine = %v, %v", ok, err)
	}
	if err := h.Renew(ctx); err != nil {
		t.Fatalf("Renew: %v", err)
	}
	if got, want := e.raw("vol"), before; got != want {
		t.Errorf("content = %q, want %q", got, want)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(Path(e.dir, "vol")); !os.IsNotExist(err) {
		t.Errorf("holder file after Release: %v, want removed", err)
	}
}

func TestAdoptRefusesAnotherNodesHolder(t *testing.T) {
	e := newEnv(t)
	if _, err := Acquire(ctx, e.dir, "vol", "kind-worker2", keep); err != nil {
		t.Fatal(err)
	}
	if _, err := Adopt(ctx, e.dir, "vol", "kind-worker"); !errors.Is(err, ErrHeld) {
		t.Errorf("Adopt = %v, want ErrHeld", err)
	}
}

func TestAdoptAfterATakeoverIsHeld(t *testing.T) {
	e := newEnv(t)
	if _, err := Acquire(ctx, e.dir, "vol", "kind-worker", keep); err != nil {
		t.Fatal(err)
	}
	if _, err := Acquire(ctx, e.dir, "vol", "kind-worker2", e.judgeTakeover("vol")); err != nil {
		t.Fatal(err)
	}
	if _, err := Adopt(ctx, e.dir, "vol", "kind-worker"); !errors.Is(err, ErrHeld) {
		t.Errorf("Adopt = %v, want ErrHeld", err)
	}
}

func TestAdoptAbsentAndCorrupt(t *testing.T) {
	e := newEnv(t)
	if _, err := Adopt(ctx, e.dir, "vol", "kind-worker"); !errors.Is(err, ErrLost) {
		t.Errorf("absent: Adopt = %v, want ErrLost", err)
	}
	if err := os.WriteFile(Path(e.dir, "vol"), []byte("not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	h, err := Adopt(ctx, e.dir, "vol", "kind-worker")
	if err == nil || h != nil || errors.Is(err, ErrHeld) || errors.Is(err, ErrLost) {
		t.Errorf("corrupt: Adopt = %v, %v, want a plain corruption error", h, err)
	}
	if _, err := Adopt(ctx, e.dir, "a/b", "kind-worker"); err == nil {
		t.Error("Adopt accepted a name with a slash")
	}
}

// A call stuck on one volume must not make operations on a sibling volume under
// the same directory fail with ErrBusy.
func TestSiblingVolumesDoNotShareDeadlineKeys(t *testing.T) {
	e := newEnv(t)
	for _, n := range []string{"a", "b"} {
		if _, err := Acquire(ctx, e.dir, n, "kind-worker", keep); err != nil {
			t.Fatal(err)
		}
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	probe = func(string) (time.Time, error) {
		if first.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		return e.now, nil
	}
	done := make(chan error, 1)
	go func() { _, err := Read(ctx, e.dir, "a"); done <- err }()
	<-entered // volume a's probe is now outstanding
	if _, err := Read(ctx, e.dir, "b"); err != nil {
		t.Errorf("Read of the sibling while a's probe is outstanding: %v", err)
	}
	if _, err := maxClaimedEpoch(ctx, e.dir, "b"); err != nil {
		t.Errorf("maxClaimedEpoch of the sibling: %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Errorf("Read of a: %v", err)
	}
}

// A-I1: on NFS, unlinking a file this client still has open renames it to
// .nfsXXXX, which a crash then leaves behind for good. The descriptor is
// closed before the unlink.
func TestReleaseClosesTheDescriptorBeforeTheRemove(t *testing.T) {
	e := newEnv(t)
	h, err := Acquire(ctx, e.dir, "vol", "kind-worker", keep)
	if err != nil {
		t.Fatal(err)
	}
	f := h.fd
	old := removeFile
	t.Cleanup(func() { removeFile = old })
	var closedAtRemove bool
	removeFile = func(path string) error {
		_, err := f.Stat()
		closedAtRemove = errors.Is(err, os.ErrClosed)
		return old(path)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if !closedAtRemove {
		t.Error("the file was removed while the holder's descriptor was still open")
	}
	if _, err := os.Stat(Path(e.dir, "vol")); !os.IsNotExist(err) {
		t.Errorf("holder file after Release: %v", err)
	}
}

// B-M6: the name is another inode (a taker's file) that happens to say what
// ours says: not ours to remove.
func TestReleaseLeavesAnotherInodeAlone(t *testing.T) {
	e := newEnv(t)
	h, err := Acquire(ctx, e.dir, "vol", "kind-worker", keep)
	if err != nil {
		t.Fatal(err)
	}
	tmp := filepath.Join(e.dir, "copy")
	if err := os.WriteFile(tmp, []byte(e.raw("vol")), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, Path(e.dir, "vol")); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); !errors.Is(err, ErrLost) {
		t.Fatalf("Release = %v, want ErrLost", err)
	}
	if _, err := os.Stat(Path(e.dir, "vol")); err != nil {
		t.Errorf("another inode's file was removed: %v", err)
	}
}

// A-m3: ESTALE from the final stat of the kept descriptor is a loss.
func TestRenewEstaleOnTheFinalStatIsLost(t *testing.T) {
	e := newEnv(t)
	h, err := Acquire(ctx, e.dir, "vol", "kind-worker", keep)
	if err != nil {
		t.Fatal(err)
	}
	old := statFile
	t.Cleanup(func() { statFile = old })
	calls := 0
	statFile = func(f *os.File) (os.FileInfo, error) {
		calls++
		if calls == 2 { // the first is the inode comparison, the second the final look
			return nil, syscall.ESTALE
		}
		return old(f)
	}
	if err := h.Renew(ctx); !errors.Is(err, ErrLost) {
		t.Fatalf("Renew = %v, want ErrLost", err)
	}
}
