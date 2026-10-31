package reconcile

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/mount/fake"
)

// A record that cannot be read leaves its volume's intent unknown. Nothing
// that depends on knowing it may act: not a rewrite of the record, not an
// unpublish, not an unstage, not garbage collection anywhere on the node.

func (e *env) recordPath(volumeID string) string {
	return filepath.Join(e.store.Dir(), intent.Escape(volumeID)+".json")
}

// breakRecord replaces a volume's record with junk and returns the good bytes,
// so a test can put them back, as a transient read error passing would.
func (e *env) breakRecord(volumeID string) (good []byte) {
	e.t.Helper()
	good, err := os.ReadFile(e.recordPath(volumeID))
	if err != nil {
		e.t.Fatal(err)
	}
	mustNil(e.t, "break", os.WriteFile(e.recordPath(volumeID), []byte("{not json"), 0o600))
	return good
}

func (e *env) restoreRecord(volumeID string, good []byte) {
	e.t.Helper()
	mustNil(e.t, "restore", os.WriteFile(e.recordPath(volumeID), good, 0o600))
}

func (e *env) unmountCalls() []string {
	var out []string
	for _, c := range e.sim.mutating() {
		if c.Op == fake.OpUnmount {
			out = append(out, c.Target)
		}
	}
	return out
}

// Kubelet repeats NodeStage after its own restart, and the record may be
// unreadable just then. Stage must neither rewrite it without the pod's
// publish nor let the pass after unmount that publish.
func TestStageDoesNotRewriteAnUnreadableRecord(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", "pvc-1")
	target := e.stageAndPublish(in, "pod1", false)
	good := e.breakRecord(in.VolumeID)
	junk, _ := os.ReadFile(e.recordPath(in.VolumeID))

	err := e.r.Stage(bg, in)
	if !errors.Is(err, ErrIntentsUnknown) {
		t.Fatalf("Stage = %v, want ErrIntentsUnknown", err)
	}
	if now, _ := os.ReadFile(e.recordPath(in.VolumeID)); !bytes.Equal(now, junk) {
		t.Error("Stage wrote a record over one it could not read")
	}
	e.freshPass(e.r.nudge)
	if _, ok := e.visible(target); !ok {
		t.Fatalf("the pod's publish was unmounted; events %+v", e.events.all())
	}
	if _, ok := e.visible(in.StagePath); !ok {
		t.Error("the stage was unmounted")
	}
	if u := e.unmountCalls(); len(u) != 0 {
		t.Errorf("unmounts = %v", u)
	}

	// The read error passes: the record is whole, with its publish, and
	// nothing was ever taken down.
	e.restoreRecord(in.VolumeID, good)
	mustNil(t, "Stage", e.r.Stage(bg, in))
	got, _ := e.intentOf(in.VolumeID)
	if len(got.Publishes) != 1 || got.Publishes[0].TargetPath != target {
		t.Errorf("publishes = %+v", got.Publishes)
	}
	if u := e.unmountCalls(); len(u) != 0 {
		t.Errorf("unmounts = %v", u)
	}
}

// The same, with the timer having run while the record was unreadable.
func TestStageAfterAPassThatSawAnUnreadableRecord(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", "pvc-1")
	target := e.stageAndPublish(in, "pod1", false)
	e.breakRecord(in.VolumeID)

	e.freshPass(e.r.nudge)
	if _, ok := e.visible(in.StagePath); !ok {
		t.Error("the failure pass unmounted the stage")
	}
	if err := e.r.Stage(bg, in); !errors.Is(err, ErrIntentsUnknown) {
		t.Fatalf("Stage = %v, want ErrIntentsUnknown", err)
	}
	if _, ok := e.visible(target); !ok {
		t.Error("the pod's publish was unmounted")
	}
	if u := e.unmountCalls(); len(u) != 0 {
		t.Errorf("unmounts = %v", u)
	}
}

func TestNothingIsCollectedWhileAnyRecordIsUnreadable(t *testing.T) {
	t.Parallel()
	e := newEnv(t, func(o *Options) { o.TransportIdle = time.Minute })
	e.start()
	a := e.vol("a", "pvc-1")
	targetA := e.stageAndPublish(a, "pod1", false)
	b := e.vol("b", "pvc-2")
	targetB := e.stageAndPublish(b, "pod2", false)
	e.stop()

	// B's record is deleted by hand (a derived drop in a clean pass), A's is
	// unreadable, a stray sits below the staging root, and C's transport is
	// idle.
	mustNil(t, "delete B", e.store.Delete(b.VolumeID))
	good := e.breakRecord(a.VolumeID)
	stray := filepath.Join(e.sRoot, "stray")
	e.sim.addMount(stray, "tmpfs", "tmpfs")
	e.clock.Advance(time.Hour)

	e.pass()
	e.pass()
	for _, p := range []string{targetA, targetB, a.StagePath, b.StagePath, stray, e.tdir("a"), e.tdir("b")} {
		if _, ok := e.visible(p); !ok {
			t.Errorf("%s was unmounted while a record was unreadable", p)
		}
	}
	if u := e.unmountCalls(); len(u) != 0 {
		t.Errorf("unmounts = %v", u)
	}
	if e.metrics.loadFailures != 1 {
		t.Errorf("load failures = %d", e.metrics.loadFailures)
	}

	// An explicit Unpublish is still carried out: kubelet named the target.
	req := &request{kind: reqUnpublish, volume: a.VolumeID, target: targetA, done: make(chan error, 1)}
	e.r.runPass(bg, []*request{req})
	mustNil(t, "explicit drop", <-req.done)
	if _, ok := e.visible(targetA); ok {
		t.Error("an explicit unpublish was held back")
	}

	// Once every record reads, collection resumes.
	e.restoreRecord(a.VolumeID, good)
	e.pass()
	for _, p := range []string{stray, targetB, b.StagePath} {
		if _, ok := e.visible(p); ok {
			t.Errorf("%s survived the first clean pass", p)
		}
	}
}

// A transient read error during Unpublish used to unmount the target and
// return success; the publish then came back when the record read again.
func TestUnpublishWithAnUnreadableRecordUnmountsNothing(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", "pvc-1")
	target := e.stageAndPublish(in, "pod1", false)
	good := e.breakRecord(in.VolumeID)

	err := e.r.Unpublish(bg, in.VolumeID, target)
	if !errors.Is(err, ErrIntentsUnknown) {
		t.Fatalf("Unpublish = %v, want ErrIntentsUnknown", err)
	}
	if _, ok := e.visible(target); !ok {
		t.Error("the target was unmounted although the unpublish could not be recorded")
	}
	e.restoreRecord(in.VolumeID, good)
	mustNil(t, "Unpublish again", e.r.Unpublish(bg, in.VolumeID, target))
	if _, ok := e.visible(target); ok {
		t.Error("the retry did not unmount")
	}
	got, _ := e.intentOf(in.VolumeID)
	if len(got.Publishes) != 0 {
		t.Errorf("publishes = %+v", got.Publishes)
	}
}

func TestUnstageWithAnUnreadableRecordTouchesNothing(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	in := e.vol("a", "pvc-1")
	target := e.stageAndPublish(in, "pod1", false)
	e.breakRecord(in.VolumeID)
	junk, _ := os.ReadFile(e.recordPath(in.VolumeID))

	if err := e.r.Unstage(bg, in.VolumeID); !errors.Is(err, ErrIntentsUnknown) {
		t.Fatalf("Unstage = %v, want ErrIntentsUnknown", err)
	}
	if now, err := os.ReadFile(e.recordPath(in.VolumeID)); err != nil || !bytes.Equal(now, junk) {
		t.Errorf("the record was deleted or changed: %v", err)
	}
	for _, p := range []string{target, in.StagePath} {
		if _, ok := e.visible(p); !ok {
			t.Errorf("%s was unmounted", p)
		}
	}
}

// A record that cannot even be opened as a file (here, a directory in its
// place) is the same case.
func TestARecordThatIsNotAFileIsUnknownNotAbsent(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.start()
	id := "directory:pvc-1"
	mustNil(t, "mkdir", os.MkdirAll(filepath.Join(e.recordPath(id), "x"), 0o700))
	if err := e.r.Unstage(bg, id); !errors.Is(err, ErrIntentsUnknown) {
		t.Errorf("Unstage = %v", err)
	}
	if err := e.r.Publish(bg, id, intent.Publish{TargetPath: e.target("p")}); !errors.Is(err, ErrIntentsUnknown) {
		t.Errorf("Publish = %v", err)
	}
	if err := e.r.Unpublish(bg, id, e.target("p")); !errors.Is(err, ErrIntentsUnknown) {
		t.Errorf("Unpublish = %v", err)
	}
}

// The log line for an unreadable record tells a person what is safe: removing
// the record of a volume that is still in use unmounts it.
func TestTheLogLineForAnUnreadableRecordWarnsAboutRemovingIt(t *testing.T) {
	t.Parallel()
	lg, buf := logger()
	e := newEnv(t, func(o *Options) { o.Logger = lg })
	in := e.vol("a", "pvc-1")
	mustNil(t, "save", e.store.Save(in))
	e.breakRecord(in.VolumeID)
	e.pass()
	e.pass()
	out := buf.String()
	if !strings.Contains(out, "repair the file or, if the volume is gone, remove it") ||
		!strings.Contains(out, "Removing the record of a volume still in use unmounts it") {
		t.Errorf("the log does not say what to do:\n%s", out)
	}
	if strings.Contains(out, "fix or remove") {
		t.Error("the old advice is still in the log")
	}
	if n := strings.Count(out, "could not be loaded"); n != 1 {
		t.Errorf("%d lines for one unreadable record over two passes, want 1", n)
	}
}
