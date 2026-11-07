package reconcile

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/item"
	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/mount/fake"
	"github.com/greghaskins/csi-miami/internal/transport"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// stageRootItem is a volume-kind item whose publish source is a mount at the
// stage path, outside the transport: the ext4 file system of a volume. source
// and root override the stage path for the negative test.
type stageRootItem struct{ source, root string }

func (s *stageRootItem) Kind() volume.Kind { return volume.KindVolume }
func (s *stageRootItem) PublishSource(in intent.Intent) (string, error) {
	if s.source != "" {
		return s.source, nil
	}
	return in.StagePath, nil
}
func (s *stageRootItem) PublishRoot(in intent.Intent) (string, error) {
	if s.root != "" {
		return s.root, nil
	}
	return in.StagePath, nil
}
func (s *stageRootItem) Converge(context.Context, intent.Intent, item.Observed) error { return nil }
func (s *stageRootItem) Teardown(context.Context, intent.Intent, item.Observed) error { return nil }

func stageRootIntent(e *env) intent.Intent {
	in := intent.Intent{
		Version:    intent.Version,
		VolumeID:   "volume:" + pvc1,
		Kind:       volume.KindVolume,
		Transport:  transport.Spec{Kind: transport.KindNFS3, Server: "192.0.2.10", Export: "/export"},
		StagePath:  e.stage(pvc1),
		AccessMode: "SINGLE_NODE_WRITER",
	}
	in.Volume = &volume.VolumeParams{ChunkSize: 1 << 20, CacheSize: 8 << 20, Size: 8 << 20, Encryption: true, Cipher: "xchacha12,aes-adiantum-plain64", KeySize: 256,
		Generations: volume.GenerationParams{Interval: 5 * time.Minute, MaxAge: time.Hour, Keep: 3}}
	return in
}

// A volume's publish source is the ext4 mount at its stage path, which is not
// beneath the transport's mount point: the item names the root the bind is
// resolved beneath, and it is the stage path itself.
func TestPublishBindsTheStageMountBeneathItself(t *testing.T) {
	t.Parallel()
	e := newEnv(t, func(o *Options) { o.Items = []item.Item{&stageRootItem{}} })
	e.start()
	in := stageRootIntent(e)

	mustNil(t, "Stage", e.r.Stage(bg, in))
	e.sim.addMount(in.StagePath, "ext4", "/dev/mapper/vol")
	e.mkTargetParent("pod1")
	mustNil(t, "Publish", e.r.Publish(bg, in.VolumeID, intent.Publish{TargetPath: e.target("pod1")}))

	var bind *fake.Call
	for _, c := range e.sim.Calls() {
		if c.Op == fake.OpBind && c.Target == e.target("pod1") {
			bind = &c
		}
	}
	if bind == nil {
		t.Fatal("no bind at the publish target")
	}
	if bind.Root != in.StagePath || bind.Source != in.StagePath {
		t.Errorf("Bind(root %q, source %q), want the stage path %q as both", bind.Root, bind.Source, in.StagePath)
	}
	if !e.isBind(e.target("pod1"), in.StagePath) {
		t.Error("the target is not a bind of the stage mount")
	}
}

// The root is never skipped: a source that is not beneath the item's root is
// refused, and nothing is mounted at the target.
func TestPublishRefusesASourceOutsideTheItemsRoot(t *testing.T) {
	t.Parallel()
	outside := t.TempDir()
	e := newEnv(t, func(o *Options) { o.Items = []item.Item{&stageRootItem{source: outside}} })
	e.start()
	in := stageRootIntent(e)

	mustNil(t, "Stage", e.r.Stage(bg, in))
	e.mkTargetParent("pod1")
	err := e.r.Publish(bg, in.VolumeID, intent.Publish{TargetPath: e.target("pod1")})
	if !errors.Is(err, mount.ErrNotResolved) {
		t.Fatalf("Publish = %v, want ErrNotResolved", err)
	}
	if _, ok := e.sim.table().At(e.target("pod1")); ok {
		t.Error("something is mounted at the target")
	}
}
