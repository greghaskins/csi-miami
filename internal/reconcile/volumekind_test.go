package reconcile

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"

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
