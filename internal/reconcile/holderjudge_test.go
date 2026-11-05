package reconcile

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/holder"
	"github.com/greghaskins/csi-miami/internal/volume"
)

const (
	judgeVolume = "volume:pvc-00000000-0000-4000-8000-000000000001"
	judgePV     = "pvc-00000000-0000-4000-8000-000000000001"
)

// fakeCluster is the API server as the judge sees it: one PV's annotation and
// the nodes' out-of-service taints. It records what was asked and offers no
// way to write, which is the property under test: the driver never removes the
// annotation.
type fakeCluster struct {
	mu         sync.Mutex
	annotation string
	annotated  bool
	annErr     error
	oos        map[string]bool
	nodeErr    error
	asked      []string
}

func (c *fakeCluster) PVAnnotation(_ context.Context, pv, key string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.asked = append(c.asked, "pv:"+pv)
	if key != volume.ForceDetachAnnotation {
		return "", errors.New("the judge asked for another annotation: " + key)
	}
	if !c.annotated {
		return "", c.annErr
	}
	return c.annotation, c.annErr
}

func (c *fakeCluster) NodeOutOfService(_ context.Context, node string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.asked = append(c.asked, "node:"+node)
	return c.oos[node], c.nodeErr
}

func holderInfo(node string, epoch uint64, age time.Duration) holder.Info {
	return holder.Info{Node: node, Epoch: epoch, Age: age, Present: true}
}

func newJudge(c *fakeCluster) *EmergencyJudge {
	return NewEmergencyJudge(JudgeOptions{Cluster: c})
}

func TestJudgeAnnotationMatchingHolder(t *testing.T) {
	c := &fakeCluster{annotation: "node-a@3", annotated: true}
	info := holderInfo("node-a", 3, time.Second) // a live, freshly renewing holder
	j := newJudge(c).Judge(context.Background(), judgeVolume, info)
	if !j.Takeover {
		t.Fatalf("a matching annotation did not take over: %s", j.Reason)
	}
	if j.Info != info {
		t.Errorf("the judgement is not bound to the holder judged: %+v", j.Info)
	}
	if !strings.Contains(j.Reason, TriggerAnnotation) {
		t.Errorf("reason %q does not name the annotation trigger", j.Reason)
	}
	// The annotation is not consumed: judged again against the same holder it
	// says the same, idempotently.
	if again := newJudge(c).Judge(context.Background(), judgeVolume, info); !again.Takeover {
		t.Error("the second judgement differs")
	}
}

func TestJudgeAnnotationStaleEpochIgnored(t *testing.T) {
	for name, ann := range map[string]string{
		"older epoch":   "node-a@2",
		"newer epoch":   "node-a@4",
		"another node":  "node-b@3",
		"empty":         "",
		"no epoch":      "node-a",
		"padded":        " node-a@3",
		"trailing":      "node-a@3 ",
		"leading zeros": "node-a@03",
	} {
		c := &fakeCluster{annotation: ann, annotated: true}
		j := newJudge(c).Judge(context.Background(), judgeVolume, holderInfo("node-a", 3, time.Hour))
		if j.Takeover {
			t.Errorf("%s: %q took over", name, ann)
		}
		if !strings.Contains(j.Reason, `"node-a@3"`) {
			t.Errorf("%s: reason %q does not give the exact value that would match", name, j.Reason)
		}
	}
	// After a takeover the annotation, left in place, names the old holder and
	// does not match the new one: no second takeover.
	c := &fakeCluster{annotation: "node-a@3", annotated: true}
	if j := newJudge(c).Judge(context.Background(), judgeVolume, holderInfo("node-b", 4, 0)); j.Takeover {
		t.Errorf("a leftover annotation took over the new holder: %s", j.Reason)
	}
	// Not set at all: no takeover, and the way out is named.
	j := newJudge(&fakeCluster{}).Judge(context.Background(), judgeVolume, holderInfo("node-a", 3, 0))
	if j.Takeover || !strings.Contains(j.Reason, "miami.csi.greghaskins.com/force-detach") {
		t.Errorf("judgement = %+v", j)
	}
}

func TestJudgeOutOfServiceTaint(t *testing.T) {
	c := &fakeCluster{oos: map[string]bool{"node-a": true}}
	info := holderInfo("node-a", 3, 0)
	j := newJudge(c).Judge(context.Background(), judgeVolume, info)
	if !j.Takeover || !strings.Contains(j.Reason, TriggerOutOfService) {
		t.Fatalf("judgement = %+v", j)
	}
	if j.Info != info {
		t.Errorf("Info = %+v", j.Info)
	}
	// The taint is the holder's node's, not any node's.
	c = &fakeCluster{oos: map[string]bool{"node-b": true}}
	if j := newJudge(c).Judge(context.Background(), judgeVolume, info); j.Takeover {
		t.Errorf("another node's taint took over: %s", j.Reason)
	}
}

func TestJudgeNeverOnAgeOrNotReady(t *testing.T) {
	// Nothing the cluster says but the annotation and the taint matters. The
	// holder is a year old on the server's clock, its node is gone from the
	// cluster's view of readiness, and a stale annotation sits on the PV.
	c := &fakeCluster{annotation: "node-a@1", annotated: true, oos: map[string]bool{}}
	for _, age := range []time.Duration{0, time.Minute, time.Hour, 24 * 365 * time.Hour} {
		if j := newJudge(c).Judge(context.Background(), judgeVolume, holderInfo("node-a", 3, age)); j.Takeover {
			t.Errorf("a holder %s old was taken over: %s", age, j.Reason)
		}
	}
	// The judge asks the cluster for nothing but the two triggers.
	for _, a := range c.asked {
		if a != "pv:"+judgePV && a != "node:node-a" {
			t.Errorf("the judge asked %q", a)
		}
	}
}

func TestJudgeNodeReadErrorIsNoTakeover(t *testing.T) {
	c := &fakeCluster{nodeErr: errors.New("the API server could not be reached")}
	j := newJudge(c).Judge(context.Background(), judgeVolume, holderInfo("node-a", 3, 0))
	if j.Takeover {
		t.Fatalf("a node read error took over: %s", j.Reason)
	}
	for _, want := range []string{"node-a", "could not be reached", "force-detach", `"node-a@3"`} {
		if !strings.Contains(j.Reason, want) {
			t.Errorf("reason %q lacks %q", j.Reason, want)
		}
	}
	// A matching annotation does not need the node: the operator's act stands
	// whatever the node read says.
	c = &fakeCluster{annotation: "node-a@3", annotated: true, nodeErr: errors.New("boom")}
	if j := newJudge(c).Judge(context.Background(), judgeVolume, holderInfo("node-a", 3, 0)); !j.Takeover {
		t.Errorf("the annotation was held up by a node read error: %s", j.Reason)
	}
	// And the taint does not need the PV.
	c = &fakeCluster{annErr: errors.New("boom"), oos: map[string]bool{"node-a": true}}
	if j := newJudge(c).Judge(context.Background(), judgeVolume, holderInfo("node-a", 3, 0)); !j.Takeover {
		t.Errorf("the taint was held up by a PV read error: %s", j.Reason)
	}
	// A PV read error with no taint is no takeover either.
	c = &fakeCluster{annErr: errors.New("boom")}
	if j := newJudge(c).Judge(context.Background(), judgeVolume, holderInfo("node-a", 3, 0)); j.Takeover {
		t.Errorf("a PV read error took over: %s", j.Reason)
	}
}

func TestJudgeNeedsAHolderAndAVolumeID(t *testing.T) {
	c := &fakeCluster{annotation: "node-a@3", annotated: true, oos: map[string]bool{"node-a": true}}
	if j := newJudge(c).Judge(context.Background(), judgeVolume, holder.Info{}); j.Takeover {
		t.Error("no holder was taken over")
	}
	if j := newJudge(c).Judge(context.Background(), "not-a-volume-id", holderInfo("node-a", 3, 0)); j.Takeover {
		t.Error("a volume ID that does not parse was taken over")
	}
}

type detailEvents struct {
	mu   sync.Mutex
	rows [][3]string
}

func (d *detailEvents) Event(v, r string) { d.EventDetail(v, r, "") }
func (d *detailEvents) EventDetail(v, r, detail string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.rows = append(d.rows, [3]string{v, r, detail})
}

func TestTakeoverEventNamesTheAbandonedEpochAndTrigger(t *testing.T) {
	ev := &detailEvents{}
	j := NewEmergencyJudge(JudgeOptions{Cluster: &fakeCluster{}, Events: ev})
	j.TookOver(judgeVolume, holder.Judgement{Takeover: true, Reason: TriggerAnnotation, Info: holderInfo("node-a", 3, 0)})
	j.TookOver(judgeVolume, holder.Judgement{Takeover: true, Reason: TriggerOutOfService, Info: holderInfo("node-b", 7, 0)})
	if len(ev.rows) != 2 {
		t.Fatalf("events = %v", ev.rows)
	}
	for i, want := range [][]string{{"node-a@3", TriggerAnnotation}, {"node-b@7", TriggerOutOfService}} {
		row := ev.rows[i]
		if row[0] != judgeVolume || row[1] != ReasonEmergencyDetach {
			t.Errorf("row %d = %v", i, row)
		}
		for _, w := range want {
			if !strings.Contains(row[2], w) {
				t.Errorf("row %d detail %q lacks %q", i, row[2], w)
			}
		}
	}
}

// An Events that cannot carry a detail still hears the reason.
func TestTakeoverEventFallsBackToThePlainSeam(t *testing.T) {
	ev := &recEvents{}
	j := NewEmergencyJudge(JudgeOptions{Cluster: &fakeCluster{}, Events: ev})
	j.TookOver(judgeVolume, holder.Judgement{Takeover: true, Reason: TriggerAnnotation, Info: holderInfo("node-a", 3, 0)})
	if len(ev.all()) != 1 {
		t.Errorf("events = %v", ev.all())
	}
}
