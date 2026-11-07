package reconcile

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"

	"github.com/greghaskins/csi-miami/internal/prereq"
)

// A request's keys are the reconciler's own copy, wiped once the pass has
// answered it, and are not part of the record.
func TestRequestKeysAreCopiedAndWiped(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	in := e.vol("a", pvc1)
	key, prev := []byte("secret-key-material"), []byte("older-key-material")
	req := &request{kind: reqStage, volume: in.VolumeID, keys: Keys{Key: key, PreviousKey: prev}.clone()}
	req.keys.Key[0] = 'X' // the copy is private to the request
	if key[0] != 's' {
		t.Fatal("the request shares the caller's key")
	}
	held := req.keys
	req.keys.Wipe()
	if req.keys.Key != nil || bytes.Contains(held.Key, []byte("ecret")) || bytes.Contains(held.PreviousKey, []byte("older")) {
		t.Errorf("not wiped: %q %q", held.Key, held.PreviousKey)
	}

	// Through the real path: after Stage returns, the record holds no key.
	e.start()
	if err := e.r.StageWith(bg, in, Keys{Key: key, PreviousKey: prev}); err != nil {
		t.Fatal(err)
	}
	rec, _ := e.intentOf(in.VolumeID)
	if bytes.Contains([]byte(rec.StagePath+rec.Subdir), key) {
		t.Error("record holds the key")
	}
}

// The prerequisite probe runs on every pass.
func TestPrereqProbeRunsEveryPass(t *testing.T) {
	t.Parallel()
	var n atomic.Int32
	e := newEnv(t, func(o *Options) {
		o.Prereq = func(context.Context) prereq.Report { n.Add(1); return prereq.Report{} }
	})
	e.pass()
	e.pass()
	if n.Load() != 2 {
		t.Errorf("probe ran %d times in 2 passes", n.Load())
	}
}
