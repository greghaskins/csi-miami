package fake

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/greghaskins/csi-miami/internal/kube"
)

func pv(name string) *kube.PV {
	return &kube.PV{Name: name, VolumeAttributes: map[string]string{"a": "b"}}
}

func TestReader(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var r Reader

	if _, err := r.GetPV(ctx, "x"); !errors.Is(err, kube.ErrNotFound) {
		t.Errorf("empty reader: error = %v, want ErrNotFound", err)
	}

	r.Add(pv("x"))
	got, err := r.GetPV(ctx, "x")
	if err != nil || got.Name != "x" {
		t.Fatalf("GetPV = %v, %v", got, err)
	}
	got.VolumeAttributes["a"] = "changed"
	if again, _ := r.GetPV(ctx, "x"); again.VolumeAttributes["a"] != "b" {
		t.Error("a caller changed the stored PV")
	}

	boom := errors.New("boom")
	r.FailOn("x", boom)
	if _, err := r.GetPV(ctx, "x"); !errors.Is(err, boom) {
		t.Errorf("FailOn: error = %v", err)
	}
	r.FailOn("x", nil)
	if _, err := r.GetPV(ctx, "x"); err != nil {
		t.Errorf("after clearing FailOn: %v", err)
	}

	r.Remove("x")
	if _, err := r.GetPV(ctx, "x"); !errors.Is(err, kube.ErrNotFound) {
		t.Errorf("after Remove: error = %v, want ErrNotFound", err)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := r.GetPV(cancelled, "x"); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled context: error = %v", err)
	}

	if got, want := r.Calls(), []string{"x", "x", "x", "x", "x", "x"}; !slices.Equal(got, want) {
		t.Errorf("Calls = %v, want %v", got, want)
	}
}
