package reconcile

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/item"
	"github.com/greghaskins/csi-miami/internal/transport"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// sizeItem is a volume item that records the intent of its last Converge.
type sizeItem struct {
	mu   sync.Mutex
	last intent.Intent
	n    int
	err  error
}

func (s *sizeItem) Kind() volume.Kind                              { return volume.KindVolume }
func (s *sizeItem) PublishSource(in intent.Intent) (string, error) { return in.StagePath, nil }
func (s *sizeItem) Converge(_ context.Context, in intent.Intent, _ item.Observed) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.last, s.n = in, s.n+1
	return s.err
}
func (s *sizeItem) Teardown(context.Context, intent.Intent, item.Observed) error { return nil }
func (s *sizeItem) seen() (intent.Intent, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last, s.n
}

func (e *env) sizedVolume() intent.Intent {
	return intent.Intent{
		Version:   intent.Version,
		VolumeID:  "volume:" + pvc1,
		Kind:      volume.KindVolume,
		Transport: transport.Spec{Kind: transport.KindNFS3, Server: "192.0.2.10", Export: "/export"},
		StagePath: e.stage(pvc1), AccessMode: "SINGLE_NODE_WRITER",
		Volume: &volume.VolumeParams{ChunkSize: 1 << 20, CacheSize: 8 << 20, Size: 8 << 20, Encryption: true, Cipher: "xchacha12,aes-adiantum-plain64", KeySize: 256,
			Generations: volume.GenerationParams{Interval: 5 * time.Minute, MaxAge: time.Hour, Keep: 3}},
	}
}

func TestExpandRecordsTheSizeAndConverges(t *testing.T) {
	t.Parallel()
	si := &sizeItem{}
	e := newEnv(t, func(o *Options) { o.Items = []item.Item{si} })
	e.start()
	in := e.sizedVolume()
	mustNil(t, "Stage", e.r.Stage(bg, in))
	_, before := si.seen()

	mustNil(t, "Expand", e.r.Expand(bg, in.VolumeID, 16<<20))
	rec, ok := e.intentOf(in.VolumeID)
	if !ok || rec.GrownSize != 16<<20 {
		t.Fatalf("record = %+v, %v; want GrownSize 16 MiB", rec, ok)
	}
	if rec.Volume.Size != 8<<20 {
		t.Errorf("the context's size changed to %d: it mirrors the immutable context", rec.Volume.Size)
	}
	last, n := si.seen()
	if n <= before || last.GrownSize != 16<<20 {
		t.Errorf("the item converged %d more times, last with GrownSize %d; want a pass with 16 MiB", n-before, last.GrownSize)
	}
}

func TestExpandNeverLowersTheRecord(t *testing.T) {
	t.Parallel()
	si := &sizeItem{}
	e := newEnv(t, func(o *Options) { o.Items = []item.Item{si} })
	e.start()
	in := e.sizedVolume()
	mustNil(t, "Stage", e.r.Stage(bg, in))
	mustNil(t, "Expand", e.r.Expand(bg, in.VolumeID, 16<<20))
	mustNil(t, "Expand smaller", e.r.Expand(bg, in.VolumeID, 12<<20))
	mustNil(t, "Expand below the context", e.r.Expand(bg, in.VolumeID, 4<<20))
	if rec, _ := e.intentOf(in.VolumeID); rec.GrownSize != 16<<20 {
		t.Errorf("GrownSize = %d after smaller expands, want 16 MiB", rec.GrownSize)
	}
}

func TestRepeatedStageKeepsTheGrownSize(t *testing.T) {
	t.Parallel()
	si := &sizeItem{}
	e := newEnv(t, func(o *Options) { o.Items = []item.Item{si} })
	e.start()
	in := e.sizedVolume()
	mustNil(t, "Stage", e.r.Stage(bg, in))
	mustNil(t, "Expand", e.r.Expand(bg, in.VolumeID, 16<<20))
	// kubelet repeats the stage with the PV's (old) context.
	mustNil(t, "Stage again", e.r.Stage(bg, in))
	if rec, _ := e.intentOf(in.VolumeID); rec.GrownSize != 16<<20 {
		t.Errorf("GrownSize = %d after a repeated stage, want 16 MiB", rec.GrownSize)
	}
	if last, _ := si.seen(); last.GrownSize != 16<<20 {
		t.Errorf("the restage converged with GrownSize %d, want 16 MiB", last.GrownSize)
	}
}

func TestExpandRefusals(t *testing.T) {
	t.Parallel()
	si := &sizeItem{}
	e := newEnv(t, func(o *Options) { o.Items = []item.Item{si} })
	e.start()
	if err := e.r.Expand(bg, "volume:"+pvc1, 16<<20); !errors.Is(err, ErrUnknownVolume) {
		t.Errorf("an unstaged volume: %v, want ErrUnknownVolume", err)
	}
	in := e.sizedVolume()
	mustNil(t, "Stage", e.r.Stage(bg, in))
	if err := e.r.Expand(bg, in.VolumeID, 0); !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("a zero size: %v, want ErrInvalidRequest", err)
	}
	if err := e.r.Expand(bg, "directory:"+pvc1, 16<<20); !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("a directory: %v, want ErrInvalidRequest", err)
	}
}

func TestExpandReportsTheItemsFailure(t *testing.T) {
	t.Parallel()
	si := &sizeItem{}
	e := newEnv(t, func(o *Options) { o.Items = []item.Item{si} })
	e.start()
	in := e.sizedVolume()
	mustNil(t, "Stage", e.r.Stage(bg, in))
	si.mu.Lock()
	si.err = item.ErrServerDown
	si.mu.Unlock()
	if err := e.r.Expand(bg, in.VolumeID, 16<<20); !errors.Is(err, item.ErrServerDown) {
		t.Errorf("Expand = %v, want the item's ErrServerDown", err)
	}
	// The record keeps the size so that the next pass finishes the grow.
	if rec, _ := e.intentOf(in.VolumeID); rec.GrownSize != 16<<20 {
		t.Errorf("GrownSize = %d after a failed pass, want 16 MiB", rec.GrownSize)
	}
}
