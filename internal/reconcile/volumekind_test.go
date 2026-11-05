package reconcile

import (
	"context"
	"errors"
	"fmt"
	"github.com/greghaskins/csi-miami/internal/transport"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/item"
	"github.com/greghaskins/csi-miami/internal/prereq"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// keyItem stands in for the directory kind and records what the reconciler
// hands its Converge, copied at call time because the reconciler wipes the
// keys when the request is finished.
type keyItem struct {
	mu      sync.Mutex
	key     string
	prev    string
	missing []string
}

func (k *keyItem) Kind() volume.Kind                              { return volume.KindDirectory }
func (k *keyItem) PublishSource(in intent.Intent) (string, error) { return in.StagePath, nil }
func (k *keyItem) Teardown(context.Context, intent.Intent, item.Observed) error {
	return nil
}
func (k *keyItem) Converge(_ context.Context, _ intent.Intent, obs item.Observed) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.key, k.prev, k.missing = string(obs.Keys.Key), string(obs.Keys.PreviousKey), obs.Prereq.Missing()
	return nil
}

// The keys of a stage request and the node's prerequisite report reach the
// item's Converge, and a pass with no request carries no keys.
func TestKeysAndPrerequisitesReachTheItem(t *testing.T) {
	t.Parallel()
	ki := &keyItem{}
	e := newEnv(t, func(o *Options) {
		o.Items = []item.Item{ki}
		o.Prereq = func(context.Context) prereq.Report {
			return prereq.Report{Features: []prereq.Feature{{Name: prereq.FeatureNBD, Err: errors.New("absent")}}}
		}
	})
	e.start()
	in := e.vol("a", pvc1)

	mustNil(t, "StageWith", e.r.StageWith(bg, in, Keys{Key: []byte("k1"), PreviousKey: []byte("k0")}))
	ki.mu.Lock()
	got := fmt.Sprintf("%s/%s/%v", ki.key, ki.prev, ki.missing)
	ki.mu.Unlock()
	if want := "k1/k0/[nbd]"; got != want {
		t.Errorf("Converge saw %s, want %s", got, want)
	}

	e.pass() // a timer pass: no request, no keys
	ki.mu.Lock()
	defer ki.mu.Unlock()
	if ki.key != "" || ki.prev != "" {
		t.Errorf("a pass with no request carried keys %q/%q", ki.key, ki.prev)
	}
}

func TestBatchKeysIsPerVolumeAndSkipsOtherRequests(t *testing.T) {
	t.Parallel()
	batch := []*request{
		{kind: reqUnstage, volume: "v0", keys: Keys{Key: []byte("no")}},
		{kind: reqStage, volume: "v1", keys: Keys{Key: []byte("a")}},
		{kind: reqPublish, volume: "v1", keys: Keys{Key: []byte("b")}},
		{kind: reqPublish, volume: "v2", keys: Keys{Key: []byte("c"), PreviousKey: []byte("d")}},
		{kind: reqStage, volume: "v3"},
	}
	got := batchKeys(batch)
	if len(got) != 2 || string(got["v1"].Key) != "a" || string(got["v2"].Key) != "c" || string(got["v2"].PreviousKey) != "d" {
		t.Errorf("batchKeys = %+v", got)
	}
}

func TestVolumeConditionsAreRegisteredAndClassified(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		err       error
		condition string
		class     Class
	}{
		{ErrPrerequisiteMissing, ConditionPrerequisiteMissing, ClassFailedPrecondition},
		{ErrHolderContended, ConditionHolderContended, ClassUnavailable},
		{ErrServerDown, ConditionServerDown, ClassUnavailable},
		{ErrKeyRequired, ConditionKeyRequired, ClassFailedPrecondition},
		{ErrBadKey, ConditionBadKey, ClassFailedPrecondition},
		{ErrGenerationOverdue, ConditionGenerationOverdue, ClassInternal},
		{ErrPublishRefused, ConditionPublishRefused, ClassFailedPrecondition},
	} {
		wrapped := fmt.Errorf("volume x: %w", tc.err)
		if got := conditionOf(wrapped); got != tc.condition {
			t.Errorf("conditionOf(%v) = %q, want %q", tc.err, got, tc.condition)
		}
		if got := Classify(wrapped); got != tc.class {
			t.Errorf("Classify(%v) = %v, want %v", tc.err, got, tc.class)
		}
		if !slices.Contains(Conditions, tc.condition) {
			t.Errorf("%q is not in Conditions", tc.condition)
		}
	}
}

// countItem counts Converge calls and fails Teardown on request.
type countItem struct {
	mu       sync.Mutex
	converge int
	teardown int
	failTear error
	src      string // the publish source, when a test publishes
}

func (c *countItem) Kind() volume.Kind { return volume.KindDirectory }
func (c *countItem) PublishSource(in intent.Intent) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.src != "" {
		return c.src, nil
	}
	return in.StagePath, nil
}
func (c *countItem) Converge(context.Context, intent.Intent, item.Observed) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.converge++
	return nil
}
func (c *countItem) Teardown(context.Context, intent.Intent, item.Observed) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.teardown++
	return c.failTear
}
func (c *countItem) counts() (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.converge, c.teardown
}

// An unstage that cannot finish is retried; the passes between the retries
// must not converge the volume (no fsck, no remount, no generations) until a
// Stage or a Publish for it says kubelet wants it back.
func TestIncompleteUnstageIsNotUndoneByLaterPasses(t *testing.T) {
	t.Parallel()
	ci := &countItem{failTear: item.ErrUnstageIncomplete}
	e := newEnv(t, func(o *Options) { o.Items = []item.Item{ci} })
	e.start()
	in := e.vol("a", pvc1)
	mustNil(t, "Stage", e.r.Stage(bg, in))
	staged, _ := ci.counts()

	if err := e.r.Unstage(bg, in.VolumeID); !errors.Is(err, item.ErrUnstageIncomplete) {
		t.Fatalf("Unstage = %v, want ErrUnstageIncomplete", err)
	}
	if got := Classify(fmt.Errorf("x: %w", ErrReleasePending)); got != ClassUnavailable {
		t.Errorf("Classify(ErrReleasePending) = %v, want Unavailable", got)
	}
	if got := Classify(fmt.Errorf("x: %w", ErrUnstageIncomplete)); got != ClassUnavailable {
		t.Errorf("class = %v", got)
	}
	e.pass()
	e.pass()
	if c, _ := ci.counts(); c != staged {
		t.Errorf("Converge ran %d more times for a volume being unstaged", c-staged)
	}
	rec, ok := e.intentOf(in.VolumeID)
	if !ok || !rec.Unstaging {
		t.Fatalf("the record does not carry the mark: %+v", rec)
	}

	// kubelet wants it back: a Stage clears the mark and converges again.
	mustNil(t, "Stage", e.r.Stage(bg, in))
	if c, _ := ci.counts(); c <= staged {
		t.Error("a Stage did not converge the volume")
	}
	if rec, _ := e.intentOf(in.VolumeID); rec.Unstaging {
		t.Error("a Stage left the mark")
	}
}

// The final generation and the holder file live on the transport: an unstage
// of a volume of the volume kind needs it mounted whether or not a stage is.
func TestVolumeKindUnstageNeedsItsTransportMounted(t *testing.T) {
	t.Parallel()
	vi := &volumeKindItem{}
	e := newEnv(t, func(o *Options) { o.Items = []item.Item{vi} })
	in := intent.Intent{
		Version:   intent.Version,
		VolumeID:  "volume:" + pvc1,
		Kind:      volume.KindVolume,
		Transport: transport.Spec{Kind: transport.KindNFS3, Server: "nas.example.com", Export: "/export"},
		StagePath: e.stage(pvc1), AccessMode: "SINGLE_NODE_WRITER",
	}
	in.Volume = &volume.VolumeParams{ChunkSize: 1 << 20, CacheSize: 8 << 20, Size: 8 << 20, Encryption: true, Cipher: "xchacha12,aes-adiantum-plain64", KeySize: 256,
		Generations: volume.GenerationParams{Interval: 5 * time.Minute, MaxAge: time.Hour, Keep: 3}}
	mustNil(t, "save", e.store.Save(in))
	err := e.r.runUnstageForTest(in.VolumeID)
	if !errors.Is(err, ErrTransportUnavailable) && !errors.Is(err, ErrTransportConflict) {
		t.Fatalf("err = %v, want the transport's error", err)
	}
	if vi.teardowns != 0 {
		t.Error("the item was torn down with its transport absent")
	}
}

type volumeKindItem struct{ teardowns int }

func (v *volumeKindItem) Kind() volume.Kind                              { return volume.KindVolume }
func (v *volumeKindItem) PublishSource(in intent.Intent) (string, error) { return in.StagePath, nil }
func (v *volumeKindItem) Converge(context.Context, intent.Intent, item.Observed) error {
	return nil
}
func (v *volumeKindItem) Teardown(context.Context, intent.Intent, item.Observed) error {
	v.teardowns++
	return nil
}

// runUnstageForTest runs one pass carrying an Unstage for the volume.
func (r *Reconciler) runUnstageForTest(volumeID string) error {
	req := &request{kind: reqUnstage, volume: volumeID, done: make(chan error, 1)}
	r.runPass(context.Background(), []*request{req})
	return <-req.done
}

// ---- a marked record is retried by the reconciler itself ----

func newMarked(t *testing.T) (*env, *countItem, intent.Intent) {
	t.Helper()
	ci := &countItem{failTear: item.ErrUnstageIncomplete}
	e := newEnv(t, func(o *Options) { o.Items = []item.Item{ci} })
	e.start()
	in := e.vol("a", pvc1)
	mustNil(t, "Stage", e.r.Stage(bg, in))
	if err := e.r.Unstage(bg, in.VolumeID); !errors.Is(err, item.ErrUnstageIncomplete) {
		t.Fatalf("Unstage = %v", err)
	}
	return e, ci, in
}

// Only a kubelet retry used to re-run Teardown for a marked record, and
// kubelet can stop retrying. The reconciler retries it on timer passes, with a
// backoff, and a success removes the record as an unstage would.
func TestMarkedRecordIsRetriedOnTimerPassesWithBackoff(t *testing.T) {
	t.Parallel()
	e, ci, in := newMarked(t)
	_, base := ci.counts()

	e.pass() // no time has passed: the request's own attempt was just made
	if _, n := ci.counts(); n != base {
		t.Fatalf("retried at once: %d teardowns after %d", n, base)
	}
	var seen []int
	last := base
	for i := 0; i < 6; i++ {
		e.clock.Advance(time.Minute)
		e.pass()
		_, n := ci.counts()
		seen = append(seen, n-last)
		last = n
	}
	// The first minute retries (the initial delay); the delay then doubles, so
	// six minutes hold only a few attempts, not six.
	total := 0
	for _, d := range seen {
		total += d
	}
	if total < 2 || total > 4 {
		t.Errorf("attempts per minute = %v: want a few with a growing gap", seen)
	}

	// Success deletes the record exactly as a kubelet-driven unstage does.
	ci.mu.Lock()
	ci.failTear = nil
	ci.mu.Unlock()
	for range 30 {
		e.clock.Advance(time.Minute)
		e.pass()
		if _, ok := e.intentOf(in.VolumeID); !ok {
			break
		}
	}
	if _, ok := e.intentOf(in.VolumeID); ok {
		t.Fatal("a successful timer retry did not remove the record")
	}
	// A late NodeUnstage from kubelet answers OK and does nothing.
	_, before := ci.counts()
	mustNil(t, "late Unstage", e.r.Unstage(bg, in.VolumeID))
	if _, after := ci.counts(); after != before {
		t.Error("a late Unstage tore the item down again")
	}
	e.noStageMount(in)
}

func TestOldMarkRaisesAWarningEventNamingTheWayOut(t *testing.T) {
	t.Parallel()
	e, _, in := newMarked(t)
	count := func() int {
		n := 0
		for _, ev := range e.events.all() {
			if ev.Reason == ReasonUnstageStuck && ev.Volume == in.VolumeID {
				n++
			}
		}
		return n
	}
	e.clock.Advance(10 * time.Minute)
	e.pass()
	if count() != 0 {
		t.Fatal("warned too early")
	}
	e.clock.Advance(40 * time.Minute)
	e.pass()
	e.clock.Advance(40 * time.Minute)
	e.pass()
	if got := count(); got != 1 {
		t.Errorf("stuck events = %d, want exactly 1", got)
	}
}

// B-M2: a Publish means kubelet wants the volume again, and clears the mark.
func TestPublishClearsTheUnstagingMark(t *testing.T) {
	t.Parallel()
	e, ci, in := newMarked(t)
	ci.mu.Lock()
	ci.src = e.itemDir("a", pvc1)
	ci.mu.Unlock()
	e.mkTargetParent("pod1")
	mustNil(t, "Publish", e.r.Publish(bg, in.VolumeID, intent.Publish{TargetPath: e.target("pod1")}))
	if rec, ok := e.intentOf(in.VolumeID); !ok || rec.Unstaging {
		t.Fatalf("the mark survived a Publish: %+v %v", rec, ok)
	}
}
