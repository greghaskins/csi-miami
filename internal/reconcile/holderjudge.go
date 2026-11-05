package reconcile

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/greghaskins/csi-miami/internal/holder"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// ReasonEmergencyDetach is the Warning event on a volume's PV that records an
// emergency detach: a holder's unpublished data was abandoned. Its detail
// names the abandoned node and epoch and the trigger.
const ReasonEmergencyDetach = "EmergencyDetach"

// The two triggers of the emergency detach (decision 52), as the judgement's
// Reason and the event's detail name them.
const (
	TriggerAnnotation   = "the PV's force-detach annotation names the holder"
	TriggerOutOfService = "the holder's node carries the " + volume.OutOfServiceTaint + " taint"
)

// Cluster is what the judge reads from the API server. It has no way to write:
// the driver never removes the annotation or the taint. *kube.Client
// implements it.
type Cluster interface {
	// ForceDetach returns the value of the force-detach annotation on the PV
	// called pv and whether it is set. A PV that does not exist has no
	// annotation and is not an error.
	ForceDetach(ctx context.Context, pv string) (value string, set bool, err error)
	// NodeOutOfService reports whether the node carries the out-of-service
	// taint, with any effect. A node that cannot be read is an error.
	NodeOutOfService(ctx context.Context, node string) (bool, error)
}

// DetailEvents is an Events that can carry a line of detail, which the PV's
// event shows. The detail is the driver's own text (a node, an epoch and a
// trigger), never an error.
type DetailEvents interface {
	Events
	EventDetail(volumeID, reason, detail string)
}

// JudgeOptions configure an EmergencyJudge. Cluster is required.
type JudgeOptions struct {
	Cluster Cluster
	// Events receives the takeover events; if it is a DetailEvents the event
	// carries the abandoned epoch and the trigger. Nil discards.
	Events Events
	Log    *slog.Logger
}

// EmergencyJudge is the reconciler's verdict on a holder of another node
// (decision 52). It allows a takeover in exactly two cases:
//
//   - the PV's annotation equals the holder file's "<node>@<epoch>", as read
//     now (holder.Info.DetachMatches, the rule DeleteVolume uses); or
//   - the holder's node carries the node.kubernetes.io/out-of-service taint.
//
// Never on a holder's age, on how long it has gone without renewing, on its
// node being NotReady, or on an API error: whatever cannot be read is no
// takeover. The two triggers are independent, so one failing to read does not
// hold up the other. The annotation is never removed by the driver; it names
// one holder at one epoch, and once that holder is replaced it names nobody.
type EmergencyJudge struct {
	c      Cluster
	events Events
	log    *slog.Logger
}

// NewEmergencyJudge returns the judge. It panics without a Cluster, as that is
// a programming error.
func NewEmergencyJudge(o JudgeOptions) *EmergencyJudge {
	if o.Cluster == nil {
		panic("reconcile: an EmergencyJudge needs a Cluster")
	}
	j := &EmergencyJudge{c: o.Cluster, events: o.Events, log: o.Log}
	if j.events == nil {
		j.events = noopEvents{}
	}
	if j.log == nil {
		j.log = slog.New(slog.DiscardHandler)
	}
	return j
}

// Judge is volume.Config.Judge. The judgement is bound to info: Acquire
// refuses it if the holder file is no longer that node and epoch.
func (j *EmergencyJudge) Judge(ctx context.Context, volumeID string, info holder.Info) holder.Judgement {
	no := func(reason string) holder.Judgement { return holder.Judgement{Reason: reason} }
	if !info.Present {
		return no("no holder")
	}
	id, err := volume.ParseID(volumeID)
	if err != nil {
		return no(fmt.Sprintf("%q is not a volume ID, so its PV cannot be looked up", volumeID))
	}
	var problems []string

	value, set, err := j.c.ForceDetach(ctx, id.Name)
	switch {
	case err != nil:
		problems = append(problems, fmt.Sprintf("the PV could not be read: %v", err))
	case set && info.DetachMatches(value):
		return holder.Judgement{Takeover: true, Reason: TriggerAnnotation, Info: info}
	case set:
		problems = append(problems, fmt.Sprintf("the annotation is %q, which does not name this holder", value))
	}

	oos, err := j.c.NodeOutOfService(ctx, info.Node)
	switch {
	case err != nil:
		problems = append(problems, fmt.Sprintf("node %s could not be read: %v", info.Node, err))
	case oos:
		return holder.Judgement{Takeover: true, Reason: TriggerOutOfService, Info: info}
	}

	return no(fmt.Sprintf("the volume is held by node %s at epoch %d, and a volume moves only after its holder syncs it; to give up what the holder has not published, set the PV annotation %s to %q or taint the node %s%s",
		info.Node, info.Epoch, volume.ForceDetachAnnotation, info.DetachValue(), volume.OutOfServiceTaint, detail(problems)))
}

func detail(problems []string) string {
	if len(problems) == 0 {
		return ""
	}
	return " (" + strings.Join(problems, "; ") + ")"
}

// TookOver is volume.Config.Takeover: it records a takeover that succeeded as
// a Warning event on the PV, and in the log, naming the abandoned node and
// epoch and the trigger.
func (j *EmergencyJudge) TookOver(volumeID string, jd holder.Judgement) {
	line := fmt.Sprintf("abandoned %s: %s", jd.Info.DetachValue(), jd.Reason)
	j.log.Warn("emergency detach: the holder's unpublished data is abandoned", "volume", volumeID, "abandoned", jd.Info.DetachValue(), "trigger", jd.Reason)
	if d, ok := j.events.(DetailEvents); ok {
		d.EventDetail(volumeID, ReasonEmergencyDetach, line)
		return
	}
	j.events.Event(volumeID, ReasonEmergencyDetach)
}
