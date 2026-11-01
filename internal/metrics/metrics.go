package metrics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"github.com/greghaskins/csi-miami/internal/reconcile"
)

// shutdownTimeout bounds the close of the HTTP server.
const shutdownTimeout = 5 * time.Second

// requestBuckets are the bounds, in seconds, of the CSI request histogram:
// from a metadata call to a stage that waits for a slow transport.
var requestBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}

// reconcileBuckets are the bounds, in seconds, of the converge-pass histogram.
var reconcileBuckets = []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60}

// The metric family names: the contract of plan section 10, exported so that
// tests of a running driver read the names from here and never retype them.
const (
	NameIntents             = "miami_intents"
	NameTransports          = "miami_transports"
	NameTransportUp         = "miami_transport_up"
	NameVolumeCondition     = "miami_volume_condition"
	NameReconcileDuration   = "miami_reconcile_duration_seconds"
	NameReconcileErrors     = "miami_reconcile_errors_total"
	NameObservationTimeouts = "miami_observation_timeouts_total"
	NameGarbageUnmounts     = "miami_garbage_unmounts_total"
	NameIntentLoadFailures  = "miami_intent_load_failures"
	NameCSIRequests         = "miami_csi_requests_total"
	NameCSIRequestDuration  = "miami_csi_request_duration_seconds"
)

// NodeFamilies are the families a node plugin serves: its reconciler's and the
// two CSI request metrics.
var NodeFamilies = []string{
	NameIntents, NameTransports, NameTransportUp, NameVolumeCondition, NameReconcileDuration, NameReconcileErrors,
	NameObservationTimeouts, NameGarbageUnmounts, NameIntentLoadFailures,
	NameCSIRequests, NameCSIRequestDuration,
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
	reconcileDur    prometheus.Histogram
	reconcileErrs   *prometheus.CounterVec
	obsTimeouts     *prometheus.CounterVec
	garbage         prometheus.Counter
	loadFailures    prometheus.Gauge
	requests        *prometheus.CounterVec
	requestDuration *prometheus.HistogramVec
}

var _ reconcile.Metrics = (*Registry)(nil)

// Option configures New.
type Option func(*options)

type options struct{ reconciler bool }

// WithReconciler registers the reconciler's family (the intents, transports,
// reconcile duration and error, timeout, garbage and load-failure metrics).
// Only the node runs a reconciler; without the option those metrics are not
// exposed, so the controller's endpoint does not carry a family that is always
// zero. The Registry still implements reconcile.Metrics, and its methods work
// either way.
func WithReconciler() Option { return func(o *options) { o.reconciler = true } }

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
	r.requests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: NameCSIRequests, Help: "Finished CSI calls, by method and gRPC code.",
	}, []string{"method", "code"})
	r.requestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: NameCSIRequestDuration, Help: "Duration of CSI calls, by method.", Buckets: requestBuckets,
	}, []string{"method"})

	// The steps the reconciler reports are a fixed list (reconcile.Metrics), so
	// their counters exist at zero from the start: a series that appears only
	// at the first failure cannot be rated or alerted on before it.
	// The same goes for every condition a volume can be in.
	for _, c := range reconcile.Conditions {
		r.volumeCondition.WithLabelValues(c)
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
			r.intents, r.transports, r.transportUp, r.volumeCondition, r.reconcileDur, r.reconcileErrs, r.obsTimeouts,
			r.garbage, r.loadFailures,
		)
	}
	return r
}

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

// ObserveReconcile implements reconcile.Metrics.
func (r *Registry) ObserveReconcile(d time.Duration) { r.reconcileDur.Observe(d.Seconds()) }

// IncReconcileError implements reconcile.Metrics.
func (r *Registry) IncReconcileError(step string) { r.reconcileErrs.WithLabelValues(step).Inc() }

// IncObservationTimeout implements reconcile.Metrics.
func (r *Registry) IncObservationTimeout(op string) { r.obsTimeouts.WithLabelValues(op).Inc() }

// IncGarbageUnmount implements reconcile.Metrics.
func (r *Registry) IncGarbageUnmount() { r.garbage.Inc() }

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
