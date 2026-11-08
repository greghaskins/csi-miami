package metrics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"github.com/greghaskins/csi-miami/internal/prereq"
	"github.com/greghaskins/csi-miami/internal/reconcile"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// shutdownTimeout bounds the close of the HTTP server.
const shutdownTimeout = 5 * time.Second

// requestBuckets are the bounds, in seconds, of the CSI request histogram:
// from a metadata call to a stage that waits for a slow transport.
var requestBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}

// publishBuckets are the bounds, in seconds, of the generation histogram: a
// freeze, a boundary and an upload, from a quiet volume to a slow transport.
var publishBuckets = []float64{0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600}

// reconcileBuckets are the bounds, in seconds, of the converge-pass histogram.
var reconcileBuckets = []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60}

// The metric family names: the contract of plan section 10, exported so that
// tests of a running driver read the names from here and never retype them.
const (
	NameIntents             = "miami_intents"
	NameTransports          = "miami_transports"
	NameTransportUp         = "miami_transport_up"
	NameVolumeCondition     = "miami_volume_condition"
	NameVolumeProbeOK       = "miami_volume_probe_ok"
	NameReconcileDuration   = "miami_reconcile_duration_seconds"
	NameReconcileErrors     = "miami_reconcile_errors_total"
	NameObservationTimeouts = "miami_observation_timeouts_total"
	NameGarbageUnmounts     = "miami_garbage_unmounts_total"
	NameIntentLoadFailures  = "miami_intent_load_failures"
	NameNodeCapability      = "miami_node_capability"
	NameCSIRequests         = "miami_csi_requests_total"
	NameCSIRequestDuration  = "miami_csi_request_duration_seconds"

	NameVolumesServed             = "miami_volumes_served"
	NameGenerationAge             = "miami_generation_age_seconds"
	NameGenerationDirty           = "miami_generation_dirty_bytes"
	NameGenerationPublishDuration = "miami_generation_publish_duration_seconds"
	NameGenerationPublishes       = "miami_generation_publishes_total"
	NameCacheBytes                = "miami_cache_bytes"
	NameChunkFetchDuration        = "miami_chunk_fetch_duration_seconds"
	NameChunkFetchErrors          = "miami_chunk_fetch_errors_total"

	NameCacheBudget    = "miami_cache_budget_bytes"
	NameCacheCommitted = "miami_cache_committed_bytes"
	NameCacheFSSize    = "miami_cache_fs_size_bytes"
	NameCacheFSFree    = "miami_cache_fs_free_bytes"

	NameWebhookMutations  = "miami_webhook_mutations_total"
	NameWebhookUnresolved = "miami_webhook_unresolved_total"

	// NameWebhookConfigOK is 1 while the MutatingWebhookConfiguration carries
	// the controller's CA in every webhook's caBundle, 0 otherwise. Without it
	// (failurePolicy Ignore) a wiped bundle skips accounting silently.
	NameWebhookConfigOK = "miami_webhook_config_ok"

	// NameCachePatchFailures counts failed patches of the node's advertised
	// capacity, by reason: denied (a 403 or 422: RBAC or the admission policy
	// refused) or error (anything else).
	NameCachePatchFailures = "miami_cache_capacity_patch_failures_total"

	// NameCacheFenceState is the verdict of the last fence probe, one series
	// per state with exactly one at 1: fenced, unfenced or unknown (the probe
	// could not tell, so the node advertises nothing; also the state before
	// the first probe).
	NameCacheFenceState = "miami_cache_fence_state"
)

// WebhookOutcomes are the values of the outcome label of
// miami_webhook_mutations_total: a fixed list, so the label cannot grow.
var WebhookOutcomes = []string{"mutated", "unchanged", "stripped", "error"}

// NodeFamilies are the families a node plugin serves: its reconciler's and the
// two CSI request metrics.
var NodeFamilies = []string{
	NameIntents, NameTransports, NameTransportUp, NameVolumeCondition, NameReconcileDuration, NameReconcileErrors,
	NameObservationTimeouts, NameGarbageUnmounts, NameIntentLoadFailures, NameNodeCapability,
	NameCSIRequests, NameCSIRequestDuration,
	NameVolumesServed, NameGenerationPublishDuration, NameGenerationPublishes,
}

// VolumeFamilies are the families with one series per staged volume of kind
// volume, labelled volume=<PV name>. They are served by the node plugin like
// NodeFamilies, but a node with no such volume has no series and so no family
// in its scrape: a test that expects them checks them only where it has staged
// a volume of that kind.
var VolumeFamilies = []string{
	NameVolumeProbeOK, NameGenerationAge, NameGenerationDirty, NameCacheBytes,
	NameChunkFetchDuration, NameChunkFetchErrors,
}

// ControllerFamilies are the families the controller serves: the CSI request
// metrics only.
var ControllerFamilies = []string{NameCSIRequests, NameCSIRequestDuration}

// ReconcileSteps are the step names the reconciler passes to IncReconcileError
// and IncObservationTimeout (see reconcile.Metrics). Their counters are
// created at zero.
var ReconcileSteps = []string{"load", "transport", "probe", "item", "publish", "teardown", "garbage", "observe"}

// Registry holds the driver's metrics. Build one with New. It implements
// reconcile.Metrics and is safe for concurrent use.
type Registry struct {
	reg *prometheus.Registry

	intents         *prometheus.GaugeVec
	transports      *prometheus.GaugeVec
	transportUp     *prometheus.GaugeVec
	volumeCondition *prometheus.GaugeVec
	volumeProbeOK   *prometheus.GaugeVec
	reconcileDur    prometheus.Histogram
	reconcileErrs   *prometheus.CounterVec
	obsTimeouts     *prometheus.CounterVec
	garbage         prometheus.Counter
	loadFailures    prometheus.Gauge
	capability      *prometheus.GaugeVec
	requests        *prometheus.CounterVec
	requestDuration *prometheus.HistogramVec

	volumesServed *prometheus.GaugeVec
	genAge        *prometheus.GaugeVec
	genDirty      *prometheus.GaugeVec
	publishDur    prometheus.Histogram
	publishes     *prometheus.CounterVec
	cacheBytes    *prometheus.GaugeVec
	fetches       *fetchCollector

	cacheBudget, cacheCommitted, cacheFSSize, cacheFSFree prometheus.Gauge

	webhookMutations  *prometheus.CounterVec
	webhookUnresolved prometheus.Counter
	webhookConfigOK   prometheus.Gauge

	cachePatchFailures *prometheus.CounterVec
	cacheFenceState    *prometheus.GaugeVec
}

var (
	_ reconcile.Metrics           = (*Registry)(nil)
	_ reconcile.ProbeMetrics      = (*Registry)(nil)
	_ reconcile.GenerationMetrics = (*Registry)(nil)
)

// Option configures New.
type Option func(*options)

type options struct{ reconciler, cacheCapacity, webhook, webhookConfig bool }

// WithReconciler registers the reconciler's family (the intents, transports,
// reconcile duration and error, timeout, garbage and load-failure metrics).
// Only the node runs a reconciler; without the option those metrics are not
// exposed, so the controller's endpoint does not carry a family that is always
// zero. The Registry still implements reconcile.Metrics, and its methods work
// either way.
func WithReconciler() Option { return func(o *options) { o.reconciler = true } }

// WithCacheCapacity registers the node's cache budget gauges (budget,
// committed, and the cache filesystem's size and free space). Only a node with
// cache accounting on carries them; SetCacheCapacity works either way.
func WithCacheCapacity() Option { return func(o *options) { o.cacheCapacity = true } }

// WithWebhook registers the admission webhook's counters: the mutations by
// outcome and the volumes it could not resolve. Only the controller runs the
// webhook; Outcome and Unresolved work either way.
func WithWebhook() Option { return func(o *options) { o.webhook = true } }

// WithWebhookConfig registers miami_webhook_config_ok. Only a controller that
// asserts the configuration's caBundle itself carries it; with a Secret and a
// bundle managed by someone else the controller cannot say, and a gauge stuck
// at 0 would be a false alarm.
func WithWebhookConfig() Option { return func(o *options) { o.webhookConfig = true } }

// New returns a Registry with the CSI request metrics, the Go collector and
// the process collector registered on its own prometheus.Registry (the global
// default registry is not used), and the reconciler's family if WithReconciler
// is given.
func New(opts ...Option) *Registry {
	var o options
	for _, f := range opts {
		f(&o)
	}
	r := &Registry{reg: prometheus.NewRegistry()}
	r.intents = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: NameIntents, Help: "Intents (volumes staged on this node) by volume kind.",
	}, []string{"kind"})
	r.transports = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: NameTransports, Help: "Whether a transport was found mounted in the latest pass: 1 mounted, 0 not.",
	}, []string{"kind", "id"})
	r.transportUp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: NameTransportUp, Help: "Whether a mounted transport answered a deadline-bounded statfs in the latest pass: 1 it did, 0 it timed out, was busy or failed (or is not mounted).",
	}, []string{"kind", "id"})
	r.volumeCondition = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: NameVolumeCondition, Help: "Volumes staged on this node in each condition after the latest pass (ok, or what is wrong).",
	}, []string{"condition"})
	r.volumeProbeOK = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: NameVolumeProbeOK, Help: "Whether the volume's device answered its active probe (a direct read of its first sector): 1 it did, 0 it failed twice in a row. Volumes of kind volume only.",
	}, []string{"volume"})
	r.reconcileDur = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: NameReconcileDuration, Help: "Duration of one converge pass.", Buckets: reconcileBuckets,
	})
	r.reconcileErrs = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: NameReconcileErrors, Help: "Failed reconcile steps, by step.",
	}, []string{"step"})
	r.obsTimeouts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: NameObservationTimeouts, Help: "Reconcile steps that hit their deadline, by step.",
	}, []string{"op"})
	r.garbage = prometheus.NewCounter(prometheus.CounterOpts{
		Name: NameGarbageUnmounts, Help: "Mounts removed by garbage collection.",
	})
	r.loadFailures = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: NameIntentLoadFailures, Help: "Intent records that failed to load in the latest pass.",
	})
	r.capability = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: NameNodeCapability, Help: "Whether the node has what the volume kind needs, by feature: 1 present, 0 missing.",
	}, []string{"feature"})
	r.requests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: NameCSIRequests, Help: "Finished CSI calls, by method and gRPC code.",
	}, []string{"method", "code"})
	r.requestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: NameCSIRequestDuration, Help: "Duration of CSI calls, by method.", Buckets: requestBuckets,
	}, []string{"method"})

	r.volumesServed = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: NameVolumesServed, Help: "Staged volumes of kind volume by the state of their block server: serving, outdated-binary (answering, on an older binary) or down.",
	}, []string{"state"})
	r.genAge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: NameGenerationAge, Help: "Seconds since the volume's newest published generation (since the node first saw the volume, if its server has not published yet).",
	}, []string{"volume"})
	r.genDirty = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: NameGenerationDirty, Help: "Bytes the volume has written that no published generation holds.",
	}, []string{"volume"})
	r.publishDur = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: NameGenerationPublishDuration, Help: "Duration of a successful generation, freeze to upload.", Buckets: publishBuckets,
	})
	r.publishes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: NameGenerationPublishes, Help: "Finished generations by result: ok, refused (the fencing check said another node owns the volume) or failed. A generation skipped because another is running is not counted.",
	}, []string{"result"})
	r.cacheBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: NameCacheBytes, Help: "Bytes in the volume's node-local cache: clean (copies of chunks the store holds) or dirty (unpublished writes).",
	}, []string{"volume", "state"})
	r.fetches = newFetchCollector()
	gauge := func(name, help string) prometheus.Gauge {
		return prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
	}
	r.cacheBudget = gauge(NameCacheBudget, "Bytes of node-local cache this node offers the scheduler.")
	r.cacheCommitted = gauge(NameCacheCommitted, "Bytes of cache footprint committed to the volumes staged on this node.")
	r.cacheFSSize = gauge(NameCacheFSSize, "Size in bytes of the filesystem the volume caches live on.")
	r.cacheFSFree = gauge(NameCacheFSFree, "Bytes available on the filesystem the volume caches live on.")
	r.cachePatchFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: NameCachePatchFailures, Help: "Failed patches or clears of this node's advertised cache capacity, by reason: denied (the API server answered 403 or 422, so RBAC or the admission policy refused), unknown (a fence probe that could not tell) or error. A budget the node computes but cannot advertise leaves pods pending.",
	}, []string{"reason"})
	r.cacheFenceState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: NameCacheFenceState, Help: "The verdict of the node's last fence probe, one series per state with exactly one at 1: fenced, unfenced (the admission policy does not fence this node plugin) or unknown (the probe could not tell, e.g. the policy was renamed or the API server failed). The node advertises only while fenced.",
	}, []string{"state"})
	r.SetCacheFenceState("unknown")
	for _, reason := range []string{"denied", "error", "unknown"} {
		r.cachePatchFailures.WithLabelValues(reason)
	}

	r.webhookMutations = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: NameWebhookMutations, Help: "Pods the admission webhook handled, by outcome: mutated (a cache request set), stripped (a stray request removed), unchanged or error (allowed unchanged because of a lookup failure).",
	}, []string{"outcome"})
	r.webhookUnresolved = prometheus.NewCounter(prometheus.CounterOpts{
		Name: NameWebhookUnresolved, Help: "Pod volumes the admission webhook could not account for (a claim that does not exist yet, or one it could not make sense of).",
	})
	r.webhookConfigOK = gauge(NameWebhookConfigOK, "Whether the webhook configuration carries this controller's CA in the caBundle of every webhook: 1 it does, 0 it does not (or could not be read). With failurePolicy Ignore a 0 means every pod skips cache accounting.")
	for _, o := range WebhookOutcomes {
		r.webhookMutations.WithLabelValues(o)
	}

	// The steps the reconciler reports are a fixed list (reconcile.Metrics), so
	// their counters exist at zero from the start: a series that appears only
	// at the first failure cannot be rated or alerted on before it.
	// The same goes for every condition a volume can be in.
	for _, c := range reconcile.Conditions {
		r.volumeCondition.WithLabelValues(c)
	}
	for _, state := range reconcile.ServedStates {
		r.volumesServed.WithLabelValues(state)
	}
	for _, res := range reconcile.PublishResults {
		r.publishes.WithLabelValues(res)
	}
	for _, step := range ReconcileSteps {
		r.reconcileErrs.WithLabelValues(step)
		r.obsTimeouts.WithLabelValues(step)
	}

	r.reg.MustRegister(
		r.requests, r.requestDuration,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	if o.reconciler {
		r.reg.MustRegister(
			r.intents, r.transports, r.transportUp, r.volumeCondition, r.volumeProbeOK, r.reconcileDur, r.reconcileErrs, r.obsTimeouts,
			r.garbage, r.loadFailures, r.capability,
			r.volumesServed, r.genAge, r.genDirty, r.publishDur, r.publishes, r.cacheBytes, r.fetches,
		)
	}
	if o.cacheCapacity {
		r.reg.MustRegister(r.cacheBudget, r.cacheCommitted, r.cacheFSSize, r.cacheFSFree, r.cachePatchFailures, r.cacheFenceState)
	}
	if o.webhook {
		r.reg.MustRegister(r.webhookMutations, r.webhookUnresolved)
	}
	if o.webhookConfig {
		r.reg.MustRegister(r.webhookConfigOK)
	}
	return r
}

// SetWebhookConfigOK sets miami_webhook_config_ok.
func (r *Registry) SetWebhookConfigOK(ok bool) {
	v := 0.0
	if ok {
		v = 1
	}
	r.webhookConfigOK.Set(v)
}

// Outcome counts one pod the webhook handled (see webhook.Metrics). A value
// outside WebhookOutcomes is dropped, so the label is a fixed list.
func (r *Registry) Outcome(outcome string) {
	if slices.Contains(WebhookOutcomes, outcome) {
		r.webhookMutations.WithLabelValues(outcome).Inc()
	}
}

// Unresolved counts one pod volume the webhook could not account for.
func (r *Registry) Unresolved() { r.webhookUnresolved.Inc() }

// SetCacheCapacity sets the cache budget gauges (see cachecap.CapacityMetrics).
func (r *Registry) SetCacheCapacity(budget, fsSize, fsFree int64) {
	r.cacheBudget.Set(float64(budget))
	r.cacheFSSize.Set(float64(fsSize))
	r.cacheFSFree.Set(float64(fsFree))
}

// IncCachePatchFailure counts one failed capacity patch (see
// cachecap.CapacityMetrics); any reason but "denied" counts as "error".
func (r *Registry) IncCachePatchFailure(reason string) {
	if reason != "denied" && reason != "unknown" {
		reason = "error"
	}
	r.cachePatchFailures.WithLabelValues(reason).Inc()
}

// SetCacheFenceState sets the one-hot fence state gauge; any state but
// "fenced" or "unfenced" is "unknown".
func (r *Registry) SetCacheFenceState(state string) {
	if state != "fenced" && state != "unfenced" {
		state = "unknown"
	}
	for _, s := range []string{"fenced", "unfenced", "unknown"} {
		v := 0.0
		if s == state {
			v = 1
		}
		r.cacheFenceState.WithLabelValues(s).Set(v)
	}
}

// SetCacheCommitted sets the committed gauge (see cachecap.CapacityMetrics).
func (r *Registry) SetCacheCommitted(committed int64) { r.cacheCommitted.Set(float64(committed)) }

// Gatherer returns the registry's metrics, for tests and for other handlers.
func (r *Registry) Gatherer() prometheus.Gatherer { return r.reg }

// SetIntents implements reconcile.Metrics.
func (r *Registry) SetIntents(kind string, n int) { r.intents.WithLabelValues(kind).Set(float64(n)) }

// SetTransport implements reconcile.Metrics: 1 for mounted, 0 for not.
func (r *Registry) SetTransport(kind, id string, mounted bool) {
	v := 0.0
	if mounted {
		v = 1
	}
	r.transports.WithLabelValues(kind, id).Set(v)
}

// SetTransportUp implements reconcile.Metrics: 1 for a transport that
// answered its probe, 0 for one that did not.
func (r *Registry) SetTransportUp(kind, id string, up bool) {
	v := 0.0
	if up {
		v = 1
	}
	r.transportUp.WithLabelValues(kind, id).Set(v)
}

// SetVolumeConditions implements reconcile.Metrics. Only the conditions the
// reconciler names are set; counts for any other key are dropped, so the label
// values are a fixed list.
func (r *Registry) SetVolumeConditions(counts map[string]int) {
	for _, c := range reconcile.Conditions {
		r.volumeCondition.WithLabelValues(c).Set(float64(counts[c]))
	}
}

// SetVolumeProbe implements reconcile.ProbeMetrics: 1 for a volume whose
// device answers its probe, 0 for one that does not. The label is the PV name;
// an ID that does not name a volume is dropped, so the label is never a path.
func (r *Registry) SetVolumeProbe(volumeID string, ok bool) {
	id, err := volume.ParseID(volumeID)
	if err != nil {
		return
	}
	v := 0.0
	if ok {
		v = 1
	}
	r.volumeProbeOK.WithLabelValues(id.Name).Set(v)
}

// DeleteVolumeProbe implements reconcile.ProbeMetrics.
func (r *Registry) DeleteVolumeProbe(volumeID string) {
	if id, err := volume.ParseID(volumeID); err == nil {
		r.volumeProbeOK.DeleteLabelValues(id.Name)
	}
}

// volumeName returns the PV name of a volume ID, or false when the ID names
// no volume: the {volume} label is a PV name and nothing else, never a path.
func volumeName(volumeID string) (string, bool) {
	id, err := volume.ParseID(volumeID)
	if err != nil {
		return "", false
	}
	return id.Name, true
}

// SetVolumeGeneration implements reconcile.GenerationMetrics.
func (r *Registry) SetVolumeGeneration(volumeID string, g reconcile.VolumeGeneration) {
	name, ok := volumeName(volumeID)
	if !ok {
		return
	}
	r.genAge.WithLabelValues(name).Set(g.Age.Seconds())
	r.genDirty.WithLabelValues(name).Set(float64(g.DirtyBytes))
	r.cacheBytes.WithLabelValues(name, "dirty").Set(float64(g.DirtyBytes))
	r.cacheBytes.WithLabelValues(name, "clean").Set(float64(g.CleanBytes))
	r.fetches.set(name, g.Fetch)
}

// DeleteVolumeGeneration implements reconcile.GenerationMetrics.
func (r *Registry) DeleteVolumeGeneration(volumeID string) {
	name, ok := volumeName(volumeID)
	if !ok {
		return
	}
	r.genAge.DeleteLabelValues(name)
	r.genDirty.DeleteLabelValues(name)
	r.cacheBytes.DeleteLabelValues(name, "dirty")
	r.cacheBytes.DeleteLabelValues(name, "clean")
	r.fetches.delete(name)
}

// ObservePublish implements reconcile.GenerationMetrics. A result that is not
// one of reconcile.PublishResults is dropped. Only a successful generation's
// duration is observed: a refusal or a failure ends early or at a deadline
// and would blur what a publish costs.
func (r *Registry) ObservePublish(result string, d time.Duration) {
	if !slices.Contains(reconcile.PublishResults, result) {
		return
	}
	r.publishes.WithLabelValues(result).Inc()
	if result == reconcile.PublishOK {
		r.publishDur.Observe(d.Seconds())
	}
}

// SetVolumesServed implements reconcile.GenerationMetrics. Only the states the
// reconciler names are set, so the label values are a fixed list.
func (r *Registry) SetVolumesServed(counts map[string]int) {
	for _, s := range reconcile.ServedStates {
		r.volumesServed.WithLabelValues(s).Set(float64(counts[s]))
	}
}

// ObserveReconcile implements reconcile.Metrics.
func (r *Registry) ObserveReconcile(d time.Duration) { r.reconcileDur.Observe(d.Seconds()) }

// IncReconcileError implements reconcile.Metrics.
func (r *Registry) IncReconcileError(step string) { r.reconcileErrs.WithLabelValues(step).Inc() }

// IncObservationTimeout implements reconcile.Metrics.
func (r *Registry) IncObservationTimeout(op string) { r.obsTimeouts.WithLabelValues(op).Inc() }

// IncGarbageUnmount implements reconcile.Metrics.
func (r *Registry) IncGarbageUnmount() { r.garbage.Inc() }

// SetCapability sets whether the node has a prerequisite feature (see
// prereq.Features): 1 present, 0 missing. The label is the feature name and
// nothing else, never a path or an address; a name that is not a feature is
// dropped, so the label values are a fixed list.
func (r *Registry) SetCapability(feature string, present bool) {
	if !slices.Contains(prereq.Features, feature) {
		return
	}
	v := 0.0
	if present {
		v = 1
	}
	r.capability.WithLabelValues(feature).Set(v)
}

// SetIntentLoadFailures implements reconcile.Metrics.
func (r *Registry) SetIntentLoadFailures(n int) { r.loadFailures.Set(float64(n)) }

// MetricsInterceptor is a gRPC unary server interceptor that counts every
// finished call by method and code and observes its duration by method. The
// method label is the short name ("NodeStageVolume"), never the request. Put
// it in csi.ServerOptions.Interceptors; the recover interceptor is inside it,
// so a panic is counted as the Internal error it becomes.
func (r *Registry) MetricsInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		method := shortMethod(info.FullMethod)
		r.requests.WithLabelValues(method, status.Code(err).String()).Inc()
		r.requestDuration.WithLabelValues(method).Observe(time.Since(start).Seconds())
		return resp, err
	}
}

// shortMethod returns the method name of a full gRPC method
// ("/csi.v1.Node/NodeStageVolume" is "NodeStageVolume").
func shortMethod(full string) string {
	if i := strings.LastIndexByte(full, '/'); i >= 0 {
		return full[i+1:]
	}
	return full
}

// Handler returns the HTTP handler of the endpoint: /metrics in the Prometheus
// text format and /healthz, which answers 200 while the process serves.
func (r *Registry) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(r.reg, promhttp.HandlerOpts{
		ErrorLog: slogErrorLogger{},
		Registry: r.reg,
	}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	return mux
}

// slogErrorLogger sends promhttp's gathering errors to the default logger.
type slogErrorLogger struct{}

func (slogErrorLogger) Println(v ...any) {
	slog.Warn("metrics: gathering failed", "error", strings.TrimSpace(fmt.Sprintln(v...)))
}

// Serve serves Handler on lis until ctx ends, then closes the server, waiting
// up to five seconds for requests in flight. It returns nil after a requested
// stop and the error if serving failed first.
func (r *Registry) Serve(ctx context.Context, lis net.Listener) error {
	srv := &http.Server{
		Handler:           r.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	failed := make(chan error, 1)
	go func() { failed <- srv.Serve(lis) }()
	select {
	case err := <-failed:
		return err
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		_ = srv.Close()
	}
	if err := <-failed; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
