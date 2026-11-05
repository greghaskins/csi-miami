package reconcile

import (
	"context"
	"log/slog"
	"time"

	"github.com/greghaskins/csi-miami/internal/item"
	"github.com/greghaskins/csi-miami/internal/prereq"
	"github.com/greghaskins/csi-miami/internal/transport"
)

// Defaults for the zero fields of Options.
const (
	// DefaultInterval is how often the loop runs when nothing asks it to.
	DefaultInterval = 30 * time.Second

	// DefaultStatTimeout bounds the stat of an item directory and the
	// creation of a stage or publish directory.
	DefaultStatTimeout = 10 * time.Second
)

// Event reasons, as passed to Events.Event.
const (
	// ReasonMountFailed is a volume whose item or binds could not be made.
	ReasonMountFailed = "MountFailed"
	// ReasonTransportUnavailable is a volume whose transport could not be
	// observed or mounted.
	ReasonTransportUnavailable = "TransportUnavailable"
	// ReasonUnstageStuck is an unstage that has been retrying for a long time
	// without finishing (an item's retryable teardown error); the log line
	// beside it carries the cause and the way out.
	ReasonUnstageStuck = "UnstageStuck"
	// ReasonGarbageUnmounted is a mount the reconciler removed because no
	// intent claims it.
	ReasonGarbageUnmounted = "GarbageUnmounted"
	// ReasonGenerationOverdue is a volume whose newest published generation
	// is older than its maximum age while it holds unpublished writes.
	ReasonGenerationOverdue = "GenerationOverdue"
)

// Metrics is the seam to the metrics registry (plan section 10). The
// reconciler calls it from its loop goroutine only. Implementations must not
// block. The zero Options use a no-op.
type Metrics interface {
	// SetIntents sets the number of intents of a kind, after every pass that
	// could read the store.
	SetIntents(kind string, n int)
	// SetTransport sets whether a transport (by kind and ID) was found mounted.
	// It is observed in the pass, never remembered between passes.
	SetTransport(kind, id string, mounted bool)
	// SetTransportUp sets whether a transport (by kind and ID) answered a
	// deadline-bounded statfs of its mount directory in the pass: true when it
	// did, false when the call timed out, was held busy by an earlier one that
	// has not returned, or failed, and for a transport that is not mounted.
	SetTransportUp(kind, id string, up bool)
	// SetVolumeConditions sets the number of volumes in each condition after
	// the pass. counts has a key for every name in Conditions, zero included,
	// so a condition that empties reads 0.
	SetVolumeConditions(counts map[string]int)
	// ObserveReconcile records the duration of one pass.
	ObserveReconcile(d time.Duration)
	// IncReconcileError counts one failed step: "load", "transport", "probe",
	// "item", "publish", "teardown", "garbage" or "observe".
	IncReconcileError(step string)
	// IncObservationTimeout counts one step that hit its deadline, by the same
	// step names.
	IncObservationTimeout(op string)
	// IncGarbageUnmount counts one mount removed by garbage collection.
	IncGarbageUnmount()
	// SetIntentLoadFailures sets the number of records that failed to load in
	// the latest pass.
	SetIntentLoadFailures(n int)
}

// Events is the seam to the Kubernetes event recorder. It is called from the
// loop goroutine, when a volume's condition changes (not on every pass).
// volumeID is empty when the reconciler does not know the volume, as for a
// stray mount. Implementations must not block.
//
// There is no message: events are readable by anyone who can view the
// namespace they land in, so their text is fixed per reason and chosen by the
// recorder, and the error behind a condition (which can name an NFS server, an
// export or a host path) goes to the node's log only.
type Events interface {
	Event(volumeID, reason string)
}

type noopMetrics struct{}

func (noopMetrics) SetIntents(string, int)              {}
func (noopMetrics) SetTransport(string, string, bool)   {}
func (noopMetrics) SetTransportUp(string, string, bool) {}
func (noopMetrics) SetVolumeConditions(map[string]int)  {}
func (noopMetrics) ObserveReconcile(time.Duration)      {}
func (noopMetrics) IncReconcileError(string)            {}
func (noopMetrics) IncObservationTimeout(string)        {}
func (noopMetrics) IncGarbageUnmount()                  {}
func (noopMetrics) SetIntentLoadFailures(int)           {}

type noopEvents struct{}

func (noopEvents) Event(string, string) {}

// AnnotationPublish is the PV annotation that asks for a generation now: any
// value the node has not seen for the volume requests exactly one.
const AnnotationPublish = "miami.csi.greghaskins.com/publish"

// GenerationOptions are the generation timers' settings for a volume whose own
// parameters leave them unset (a volume's record carries its StorageClass's
// values, which win).
type GenerationOptions struct {
	// Interval is how often a volume with unpublished writes publishes a
	// generation. Zero means volume.DefaultGenerationEvery; anything under
	// volume.MinGenerationInterval is refused by New.
	Interval time.Duration
	// MaxAge is how old the newest published generation of a volume with
	// unpublished writes may get before the volume is reported overdue. Zero
	// means volume.DefaultGenerationMaxAge; it must not be under Interval.
	MaxAge time.Duration
	// Timeout is the deadline of one generation, freeze to upload. Zero means
	// DefaultGenerationTimeout.
	Timeout time.Duration
}

// DefaultGenerationTimeout bounds one generation goroutine: more than the
// volume item's own publish deadline.
const DefaultGenerationTimeout = 5 * time.Minute

// Options configure a Reconciler. TransportsRoot and StageRoot are required;
// everything else has a usable zero value.
type Options struct {
	// TransportsRoot is the directory under which transports are mounted, at
	// <TransportsRoot>/transports/<id> (transport.Spec.MountPoint). Absolute,
	// cleaned and resolved (no symlink): the Mounter refuses a mount point
	// reached through one.
	TransportsRoot string

	// StageRoot is the driver's own staging directory, which is where garbage
	// collection looks for mounts that no intent claims: kubelet's
	// <kubelet dir>/plugins/kubernetes.io/csi/<driver name>. Everything
	// mounted strictly below it that is not a volume's stage path is removed,
	// so it must not be shared with anything else, and it must not overlap
	// TransportsRoot. Absolute, cleaned and resolved.
	StageRoot string

	// Interval is the period of the timer pass. Zero means DefaultInterval.
	Interval time.Duration

	// StatTimeout bounds each stat and directory creation the reconciler or
	// the directory item makes. Zero means DefaultStatTimeout. Mount calls are
	// bounded by the Mounter's own deadline.
	StatTimeout time.Duration

	// TransportIdle is how long a transport that no intent references stays
	// mounted before garbage collection unmounts it. Zero, the default, means
	// never: an unreferenced transport is left mounted for the next volume.
	TransportIdle time.Duration

	// Transport configures the transports the reconciler makes.
	Transport transport.Options

	// Items are the item kinds the reconciler can converge. Nil means the
	// directory item.
	Items []item.Item

	// Logger receives the reconciler's log lines. Nil discards them. It is
	// never given a mount data string or an option value.
	Logger *slog.Logger

	// Metrics and Events default to no-ops.
	Metrics Metrics
	Events  Events

	// Prereq probes the node's prerequisites for the volume kind (see package
	// prereq). It is called once per pass, must not block, and its report is
	// what layer 1 of the volume item reads. Nil means no probe: nothing is
	// reported missing.
	Prereq func(context.Context) prereq.Report

	// CacheDir is the directory volume caches live under, BinDir the directory
	// the per-hash copies of the binary go in, and SystemdSocket systemd's
	// private socket; all three are read by the volume item.
	CacheDir      string
	BinDir        string
	SystemdSocket string

	// HolderRenew is how often a holder renews. Zero means the volume item's
	// default (15s). There is no takeover timeout: a holder is replaced only by
	// the emergency detach (decision 52).
	HolderRenew time.Duration

	// MarkRetry is the delay before the reconciler first retries the teardown
	// of a volume whose unstage began and has not finished, MarkRetryMax the
	// longest the delay grows to (it doubles), and MarkWarnAfter how long such
	// an unstage may go on before ReasonUnstageStuck is raised. Zero means
	// 1 minute, 10 minutes and 30 minutes.
	MarkRetry, MarkRetryMax, MarkWarnAfter time.Duration

	// Remediate permits the reconciler to repair what it finds wrong beyond
	// converging intent (see the remediation task); false observes and reports
	// only.
	Remediate bool

	// Generations are the defaults of the generation timers.
	Generations GenerationOptions

	// PVAnnotation reads one annotation of the PV named pv ("" when it has
	// none), for AnnotationPublish. Nil means no volume can request a
	// generation. An error is not a request. The node's role needs only get on
	// persistentvolumes for it.
	PVAnnotation func(ctx context.Context, pv, key string) (string, error)

	// Now is the clock for the idle timeout and the generation timers. Nil means time.Now.
	Now func() time.Time
}
