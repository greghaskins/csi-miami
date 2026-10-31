// Package metrics is the driver's Prometheus registry: the node reconciler's
// gauges and counters (it implements reconcile.Metrics), the gRPC request
// counters and histograms of both roles (a unary interceptor), and the HTTP
// endpoint that serves them with the Go and process collectors.
//
// The names and labels are the contract of plan section 10. No label carries
// a volume ID or a path: the labels are the volume kind, the transport kind
// and its short hash ID (a handful of values per node), the reconcile step,
// the gRPC method and its code.
//
//	miami_intents{kind}                       gauge: intents on this node
//	miami_transports{kind,id}                 gauge: 1 mounted, 0 not
//	miami_reconcile_duration_seconds          histogram: one converge pass
//	miami_reconcile_errors_total{step}        counter: failed steps
//	miami_observation_timeouts_total{op}      counter: steps that hit a deadline
//	miami_garbage_unmounts_total              counter: mounts collected
//	miami_intent_load_failures                gauge: records that failed to load
//	miami_csi_requests_total{method,code}     counter: finished CSI calls
//	miami_csi_request_duration_seconds{method} histogram: their duration
//
// The counters of the reconciler's fixed steps exist at zero from the start.
//
// A transport's state is the value of its gauge, not a label: a "mounted"
// label would leave a stale series behind at every change, and one series per
// transport is what an alert on "== 0" wants. The plan names
// miami_intent_load_failures_total, a counter; the reconciler reports the
// number of records unreadable in the latest pass, which goes down when a
// person repairs one, so it is a gauge without the suffix.
package metrics
