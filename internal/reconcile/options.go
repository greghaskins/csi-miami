package reconcile

import (
	"log/slog"
	"time"

	"github.com/greghaskins/csi-miami/internal/item"
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
	// ReasonGarbageUnmounted is a mount the reconciler removed because no
	// intent claims it.
	ReasonGarbageUnmounted = "GarbageUnmounted"
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
type Events interface {
	Event(volumeID, reason, message string)
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

func (noopEvents) Event(string, string, string) {}

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

	// Now is the clock for the idle timeout. Nil means time.Now.
	Now func() time.Time
}
