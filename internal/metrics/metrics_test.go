package metrics

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/greghaskins/csi-miami/internal/blockd"
	"github.com/greghaskins/csi-miami/internal/prereq"
	"github.com/greghaskins/csi-miami/internal/reconcile"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// family returns the metric family called name, or nil.
func family(t *testing.T, r *Registry, name string) *dto.MetricFamily {
	t.Helper()
	fams, err := r.Gatherer().Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range fams {
		if f.GetName() == name {
			return f
		}
	}
	return nil
}

// sample returns the metric of family name whose labels are exactly want.
func sample(t *testing.T, r *Registry, name string, want map[string]string) *dto.Metric {
	t.Helper()
	f := family(t, r, name)
	if f == nil {
		t.Fatalf("no metric family %s", name)
	}
	for _, m := range f.GetMetric() {
		if len(m.GetLabel()) != len(want) {
			continue
		}
		match := true
		for _, l := range m.GetLabel() {
			if want[l.GetName()] != l.GetValue() {
				match = false
			}
		}
		if match {
			return m
		}
	}
	t.Fatalf("%s has no sample with labels %v", name, want)
	return nil
}

func TestReconcileMetricsHaveThePlansNamesAndLabels(t *testing.T) {
	t.Parallel()
	r := New(WithReconciler())
	r.SetIntents("directory", 3)
	r.SetTransport("local", "abc123", true)
	r.SetTransport("nfs3", "def456", false)
	r.ObserveReconcile(150 * time.Millisecond)
	r.IncReconcileError("item")
	r.IncReconcileError("item")
	r.IncObservationTimeout("transport")
	r.IncGarbageUnmount()
	r.SetIntentLoadFailures(2)

	if v := sample(t, r, "miami_intents", map[string]string{"kind": "directory"}).GetGauge().GetValue(); v != 3 {
		t.Errorf("miami_intents = %v", v)
	}
	if v := sample(t, r, "miami_transports", map[string]string{"kind": "local", "id": "abc123"}).GetGauge().GetValue(); v != 1 {
		t.Errorf("mounted transport = %v", v)
	}
	if v := sample(t, r, "miami_transports", map[string]string{"kind": "nfs3", "id": "def456"}).GetGauge().GetValue(); v != 0 {
		t.Errorf("unmounted transport = %v", v)
	}
	r.SetTransport("nfs3", "def456", true)
	if v := sample(t, r, "miami_transports", map[string]string{"kind": "nfs3", "id": "def456"}).GetGauge().GetValue(); v != 1 {
		t.Errorf("transport after it mounted = %v", v)
	}
	h := sample(t, r, "miami_reconcile_duration_seconds", nil).GetHistogram()
	if h.GetSampleCount() != 1 || h.GetSampleSum() < 0.149 || h.GetSampleSum() > 0.151 {
		t.Errorf("duration histogram: count %d sum %v", h.GetSampleCount(), h.GetSampleSum())
	}
	if v := sample(t, r, "miami_reconcile_errors_total", map[string]string{"step": "item"}).GetCounter().GetValue(); v != 2 {
		t.Errorf("errors = %v", v)
	}
	if v := sample(t, r, "miami_observation_timeouts_total", map[string]string{"op": "transport"}).GetCounter().GetValue(); v != 1 {
		t.Errorf("timeouts = %v", v)
	}
	if v := sample(t, r, "miami_garbage_unmounts_total", nil).GetCounter().GetValue(); v != 1 {
		t.Errorf("garbage = %v", v)
	}
	if v := sample(t, r, "miami_intent_load_failures", nil).GetGauge().GetValue(); v != 2 {
		t.Errorf("load failures = %v", v)
	}
	r.SetIntentLoadFailures(0)
	if v := sample(t, r, "miami_intent_load_failures", nil).GetGauge().GetValue(); v != 0 {
		t.Errorf("load failures after repair = %v", v)
	}
}

var _ reconcile.Metrics = (*Registry)(nil)

func TestTransportUpIsOneWhenTheTransportAnswersAndZeroWhenItDoesNot(t *testing.T) {
	t.Parallel()
	r := New(WithReconciler())
	r.SetTransportUp("nfs3", "def456", true)
	r.SetTransportUp("nfs3", "aaa111", false)
	for _, tt := range []struct {
		id   string
		want float64
	}{{"def456", 1}, {"aaa111", 0}} {
		if v := sample(t, r, "miami_transport_up", map[string]string{"kind": "nfs3", "id": tt.id}).GetGauge().GetValue(); v != tt.want {
			t.Errorf("miami_transport_up{id=%s} = %v, want %v", tt.id, v, tt.want)
		}
	}
	r.SetTransportUp("nfs3", "def456", false)
	if v := sample(t, r, "miami_transport_up", map[string]string{"kind": "nfs3", "id": "def456"}).GetGauge().GetValue(); v != 0 {
		t.Errorf("after the transport stopped answering = %v", v)
	}
	// Not served without the reconciler's family.
	if f := family(t, New(), "miami_transport_up"); f != nil {
		t.Errorf("the controller serves miami_transport_up")
	}
}

// Every condition has a series from the start, at zero, and the label is the
// condition and nothing else (no volume ID).
func TestVolumeConditionHasEveryConditionAtZeroAndNoOtherLabel(t *testing.T) {
	t.Parallel()
	r := New(WithReconciler())
	f := family(t, r, "miami_volume_condition")
	if f == nil {
		t.Fatal("no miami_volume_condition family")
	}
	got := map[string]float64{}
	for _, m := range f.GetMetric() {
		if len(m.GetLabel()) != 1 || m.GetLabel()[0].GetName() != "condition" {
			t.Fatalf("labels = %v, want only condition", m.GetLabel())
		}
		got[m.GetLabel()[0].GetValue()] = m.GetGauge().GetValue()
	}
	want := []string{"ok", "unresponsive", "transport-unavailable", "transport-conflict", "stage-conflict",
		"publish-conflict", "item-missing", "item-not-directory", "failed",
		"prerequisite-missing", "holder-contended", "server-down", "key-required", "bad-key", "generation-overdue", "publish-refused", "probe-failed", "cache-budget-exceeded"}
	if len(got) != len(want) {
		t.Errorf("series = %v, want exactly %v", got, want)
	}
	for _, c := range want {
		if v, ok := got[c]; !ok || v != 0 {
			t.Errorf("condition %q = %v (present %v), want 0 from the start", c, v, ok)
		}
	}

	r.SetVolumeConditions(map[string]int{"ok": 3, "transport-unavailable": 2, "invented": 9})
	for c, v := range map[string]float64{"ok": 3, "transport-unavailable": 2, "failed": 0} {
		if got := sample(t, r, "miami_volume_condition", map[string]string{"condition": c}).GetGauge().GetValue(); got != v {
			t.Errorf("condition %q = %v, want %v", c, got, v)
		}
	}
	if len(family(t, r, "miami_volume_condition").GetMetric()) != len(want) {
		t.Error("a condition the reconciler does not name made a series")
	}
	r.SetVolumeConditions(map[string]int{})
	if got := sample(t, r, "miami_volume_condition", map[string]string{"condition": "ok"}).GetGauge().GetValue(); got != 0 {
		t.Errorf("ok after the volumes went = %v, want 0", got)
	}
}

func TestMetricsInterceptorCountsByMethodAndCode(t *testing.T) {
	t.Parallel()
	r := New()
	ic := r.MetricsInterceptor()
	call := func(method string, err error) {
		t.Helper()
		_, got := ic(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: method},
			func(context.Context, any) (any, error) { return nil, err })
		if !errors.Is(got, err) {
			t.Fatalf("the interceptor changed the error: %v", got)
		}
	}
	call("/csi.v1.Node/NodeStageVolume", nil)
	call("/csi.v1.Node/NodeStageVolume", nil)
	call("/csi.v1.Node/NodeStageVolume", status.Error(codes.NotFound, "x"))
	call("/csi.v1.Controller/CreateVolume", status.Error(codes.Internal, "x"))
	call("/csi.v1.Identity/Probe", nil)

	for _, tt := range []struct {
		method, code string
		want         float64
	}{
		{"NodeStageVolume", "OK", 2},
		{"NodeStageVolume", "NotFound", 1},
		{"CreateVolume", "Internal", 1},
		{"Probe", "OK", 1},
	} {
		got := sample(t, r, "miami_csi_requests_total", map[string]string{"method": tt.method, "code": tt.code}).GetCounter().GetValue()
		if got != tt.want {
			t.Errorf("%s %s = %v, want %v", tt.method, tt.code, got, tt.want)
		}
	}
	h := sample(t, r, "miami_csi_request_duration_seconds", map[string]string{"method": "NodeStageVolume"}).GetHistogram()
	if h.GetSampleCount() != 3 {
		t.Errorf("duration samples = %d, want 3", h.GetSampleCount())
	}
}

func TestShortMethod(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]string{
		"/csi.v1.Node/NodeGetInfo": "NodeGetInfo",
		"NodeGetInfo":              "NodeGetInfo",
		"/a/b/c":                   "c",
	} {
		if got := shortMethod(in); got != want {
			t.Errorf("shortMethod(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestHandlerServesMetricsAndHealth(t *testing.T) {
	t.Parallel()
	r := New(WithReconciler())
	r.SetIntents("directory", 1)
	h := r.Handler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		`miami_intents{kind="directory"} 1`,
		"go_goroutines", "process_cpu_seconds_total",
		"# TYPE miami_intent_load_failures gauge",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics lacks %q", want)
		}
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != "ok" {
		t.Errorf("/healthz = %d %q", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/nope", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("/nope = %d", rec.Code)
	}
}

func TestServeStopsWhenTheContextEnds(t *testing.T) {
	t.Parallel()
	lis, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- New().Serve(ctx, lis) }()

	resp, err := http.Get("http://" + lis.Addr().String() + "/healthz") //nolint:noctx // a loopback test
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status %d", resp.StatusCode)
	}
	cancel()
	if err := <-done; err != nil {
		t.Errorf("Serve returned %v after a requested stop", err)
	}
}

func TestServeReportsAListenerThatFails(t *testing.T) {
	t.Parallel()
	lis, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_ = lis.Close()
	if err := New().Serve(context.Background(), lis); err == nil {
		t.Error("a closed listener was served")
	}
}

// The counters of the reconciler's fixed steps are there at zero before the
// first failure.
func TestStepCountersExistAtZero(t *testing.T) {
	t.Parallel()
	r := New(WithReconciler())
	for _, step := range ReconcileSteps {
		if v := sample(t, r, "miami_reconcile_errors_total", map[string]string{"step": step}).GetCounter().GetValue(); v != 0 {
			t.Errorf("errors[%s] = %v", step, v)
		}
		if v := sample(t, r, "miami_observation_timeouts_total", map[string]string{"op": step}).GetCounter().GetValue(); v != 0 {
			t.Errorf("timeouts[%s] = %v", step, v)
		}
	}
}

// Without WithReconciler the reconciler's family is not exposed (the
// controller), though the methods still work; the request metrics are.
func TestReconcilerFamilyOnlyOnRequest(t *testing.T) {
	t.Parallel()
	r := New()
	r.SetIntents("directory", 1)
	r.IncReconcileError("item")
	r.ObserveReconcile(time.Millisecond)
	for _, name := range []string{
		"miami_intents", "miami_transports", "miami_reconcile_duration_seconds", "miami_reconcile_errors_total",
		"miami_observation_timeouts_total", "miami_garbage_unmounts_total", "miami_intent_load_failures",
	} {
		if family(t, r, name) != nil {
			t.Errorf("%s is exposed without WithReconciler", name)
		}
	}
	_, _ = r.MetricsInterceptor()(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/csi.v1.Identity/Probe"},
		func(context.Context, any) (any, error) { return nil, nil })
	if family(t, r, "miami_csi_requests_total") == nil || family(t, r, "go_goroutines") == nil {
		t.Error("the request metrics or the Go collector are missing")
	}
	if family(t, New(WithReconciler()), "miami_intent_load_failures") == nil {
		t.Error("WithReconciler did not register the family")
	}
}

// The exported family lists are what a running driver serves: the node's
// registry, once every family has a sample, gathers exactly NodeFamilies (and
// the Go and process collectors), and the controller's exactly
// ControllerFamilies, so a test that reads the lists is not reading a stale
// copy.
func TestFamilyListsMatchTheRegistry(t *testing.T) {
	served := func(r *Registry) map[string]bool {
		fams, err := r.Gatherer().Gather()
		if err != nil {
			t.Fatal(err)
		}
		got := map[string]bool{}
		for _, f := range fams {
			if strings.HasPrefix(f.GetName(), "miami_") {
				got[f.GetName()] = true
			}
		}
		return got
	}
	check := func(who string, got map[string]bool, want []string) {
		t.Helper()
		for _, n := range want {
			if !got[n] {
				t.Errorf("%s does not serve %s", who, n)
			}
			delete(got, n)
		}
		for n := range got {
			t.Errorf("%s serves %s, which its family list does not name", who, n)
		}
	}

	node := New(WithReconciler())
	node.SetIntents("directory", 1)
	node.SetTransport("local", "abc", true)
	node.SetTransportUp("local", "abc", true)
	node.ObserveReconcile(time.Millisecond)
	node.SetCapability(prereq.FeatureNBD, true)
	node.requests.WithLabelValues("NodeStageVolume", "OK").Inc()
	node.requestDuration.WithLabelValues("NodeStageVolume").Observe(0.001)
	check("the node registry", served(node), NodeFamilies)

	ctrl := New()
	ctrl.requests.WithLabelValues("CreateVolume", "OK").Inc()
	ctrl.requestDuration.WithLabelValues("CreateVolume").Observe(0.001)
	check("the controller registry", served(ctrl), ControllerFamilies)
}

func TestCapabilityGauges(t *testing.T) {
	t.Parallel()
	r := New(WithReconciler())
	r.SetCapability(prereq.FeatureNBD, true)
	r.SetCapability(prereq.FeatureAdiantum, false)
	r.SetCapability("/run/systemd/private", false) // not a feature: dropped, never a label
	if v := sample(t, r, NameNodeCapability, map[string]string{"feature": "nbd"}).GetGauge().GetValue(); v != 1 {
		t.Errorf("nbd = %v, want 1", v)
	}
	if v := sample(t, r, NameNodeCapability, map[string]string{"feature": "adiantum"}).GetGauge().GetValue(); v != 0 {
		t.Errorf("adiantum = %v, want 0", v)
	}
	f := family(t, r, NameNodeCapability)
	if n := len(f.GetMetric()); n != 2 {
		t.Errorf("%d series, want 2 (a non-feature name must be dropped)", n)
	}
	// A feature that comes back flips to 1.
	r.SetCapability(prereq.FeatureAdiantum, true)
	if v := sample(t, r, NameNodeCapability, map[string]string{"feature": "adiantum"}).GetGauge().GetValue(); v != 1 {
		t.Errorf("adiantum after it came back = %v, want 1", v)
	}
	// Without the reconciler's family the gauge is not exposed.
	if family(t, New(), NameNodeCapability) != nil {
		t.Error("the controller registry serves the capability family")
	}
}

// The probe gauge is per volume, one series each, and a volume that is no
// longer staged loses its series. It is not in NodeFamilies: a node with no
// volume of kind volume has no series, and a family list means "served on every
// node".
func TestVolumeProbeOKGauge(t *testing.T) {
	t.Parallel()
	r := New(WithReconciler())
	const a, b = "volume:pvc-00000000-0000-4000-8000-00000000000a", "volume:pvc-00000000-0000-4000-8000-00000000000b"
	r.SetVolumeProbe(a, true)
	r.SetVolumeProbe(b, false)
	want := func(vol string, v float64) {
		t.Helper()
		m := sample(t, r, NameVolumeProbeOK, map[string]string{"volume": vol})
		if got := m.GetGauge().GetValue(); got != v {
			t.Errorf("%s = %v, want %v", vol, got, v)
		}
	}
	want("pvc-00000000-0000-4000-8000-00000000000a", 1)
	want("pvc-00000000-0000-4000-8000-00000000000b", 0)
	r.DeleteVolumeProbe(b)
	if n := len(family(t, r, NameVolumeProbeOK).GetMetric()); n != 1 {
		t.Errorf("%d series after a delete, want 1", n)
	}
	r.SetVolumeProbe("not a volume id", true) // dropped: the label is a PV name or nothing
	if n := len(family(t, r, NameVolumeProbeOK).GetMetric()); n != 1 {
		t.Errorf("%d series after a bad ID, want 1", n)
	}
}

const (
	genVolA = "volume:pvc-00000000-0000-4000-8000-00000000000a"
	genVolB = "volume:pvc-00000000-0000-4000-8000-00000000000b"
	pvA     = "pvc-00000000-0000-4000-8000-00000000000a"
	pvB     = "pvc-00000000-0000-4000-8000-00000000000b"
)

// The families that have a series on every node are in NodeFamilies, from the
// start; the per-volume ones are in VolumeFamilies and appear with a volume.
func TestFamiliesRegistered(t *testing.T) {
	t.Parallel()
	r := New(WithReconciler())
	for _, name := range []string{NameVolumesServed, NameGenerationPublishDuration, NameGenerationPublishes} {
		if !slices.Contains(NodeFamilies, name) {
			t.Errorf("%s is not in NodeFamilies", name)
		}
		if family(t, r, name) == nil {
			t.Errorf("%s has no series on a fresh node registry", name)
		}
	}
	for _, name := range VolumeFamilies {
		if slices.Contains(NodeFamilies, name) {
			t.Errorf("%s is per volume and must not be in NodeFamilies", name)
		}
		if family(t, r, name) != nil {
			t.Errorf("%s has a series on a node with no volume", name)
		}
	}
	for _, s := range reconcile.ServedStates {
		if v := sample(t, r, NameVolumesServed, map[string]string{"state": s}).GetGauge().GetValue(); v != 0 {
			t.Errorf("served %s = %v at start", s, v)
		}
	}
	for _, s := range reconcile.PublishResults {
		if v := sample(t, r, NameGenerationPublishes, map[string]string{"result": s}).GetCounter().GetValue(); v != 0 {
			t.Errorf("publishes %s = %v at start", s, v)
		}
	}
	// Once a volume reports, every per-volume family has its series.
	r.SetVolumeProbe(genVolA, true)
	r.SetVolumeGeneration(genVolA, reconcile.VolumeGeneration{Fetch: blockd.FetchStats{Buckets: make([]uint64, len(blockd.FetchBuckets))}})
	for _, name := range VolumeFamilies {
		if family(t, r, name) == nil {
			t.Errorf("%s has no series for a reporting volume", name)
		}
	}
	// The controller serves none of them.
	c := New()
	for _, name := range slices.Concat(NodeFamilies, VolumeFamilies) {
		if strings.HasPrefix(name, "miami_generation") || strings.HasPrefix(name, "miami_chunk") || name == NameVolumesServed || name == NameCacheBytes {
			if family(t, c, name) != nil {
				t.Errorf("the controller serves %s", name)
			}
		}
	}
}

func TestVolumeGenerationGauges(t *testing.T) {
	t.Parallel()
	r := New(WithReconciler())
	cum := make([]uint64, len(blockd.FetchBuckets))
	for i := 3; i < len(cum); i++ {
		cum[i] = 4 // four fetches, all within the fourth bound and up
	}
	r.SetVolumeGeneration(genVolA, reconcile.VolumeGeneration{
		Age: 90 * time.Second, DirtyBytes: 4096, CleanBytes: 8192,
		Fetch: blockd.FetchStats{Count: 4, Errors: 1, Sum: 0.5, Buckets: cum},
	})
	r.SetVolumeGeneration(genVolB, reconcile.VolumeGeneration{Fetch: blockd.FetchStats{Buckets: make([]uint64, len(blockd.FetchBuckets))}})
	gauge := func(name string, labels map[string]string) float64 {
		t.Helper()
		return sample(t, r, name, labels).GetGauge().GetValue()
	}
	if v := gauge(NameGenerationAge, map[string]string{"volume": pvA}); v != 90 {
		t.Errorf("age = %v", v)
	}
	if v := gauge(NameGenerationDirty, map[string]string{"volume": pvA}); v != 4096 {
		t.Errorf("dirty = %v", v)
	}
	if v := gauge(NameCacheBytes, map[string]string{"volume": pvA, "state": "dirty"}); v != 4096 {
		t.Errorf("cache dirty = %v", v)
	}
	if v := gauge(NameCacheBytes, map[string]string{"volume": pvA, "state": "clean"}); v != 8192 {
		t.Errorf("cache clean = %v", v)
	}
	if v := sample(t, r, NameChunkFetchErrors, map[string]string{"volume": pvA}).GetCounter().GetValue(); v != 1 {
		t.Errorf("fetch errors = %v", v)
	}
	h := sample(t, r, NameChunkFetchDuration, map[string]string{"volume": pvA}).GetHistogram()
	if h.GetSampleCount() != 4 || h.GetSampleSum() != 0.5 || len(h.GetBucket()) != len(blockd.FetchBuckets) {
		t.Errorf("fetch histogram = count %d sum %v buckets %d", h.GetSampleCount(), h.GetSampleSum(), len(h.GetBucket()))
	}
	// A later status replaces the figures; it does not add series.
	r.SetVolumeGeneration(genVolA, reconcile.VolumeGeneration{Age: time.Second, Fetch: blockd.FetchStats{Buckets: make([]uint64, len(blockd.FetchBuckets))}})
	if v := gauge(NameGenerationAge, map[string]string{"volume": pvA}); v != 1 {
		t.Errorf("age after an update = %v", v)
	}
	if n := len(family(t, r, NameGenerationAge).GetMetric()); n != 2 {
		t.Errorf("%d age series, want 2", n)
	}
}

// Series end with the volume, in every per-volume family.
func TestVolumeGenerationSeriesAreDeletedWhenTheVolumeLeaves(t *testing.T) {
	t.Parallel()
	r := New(WithReconciler())
	zero := blockd.FetchStats{Buckets: make([]uint64, len(blockd.FetchBuckets))}
	r.SetVolumeGeneration(genVolA, reconcile.VolumeGeneration{Fetch: zero})
	r.SetVolumeGeneration(genVolB, reconcile.VolumeGeneration{Fetch: zero})
	r.DeleteVolumeGeneration(genVolA)
	for _, name := range []string{NameGenerationAge, NameGenerationDirty, NameCacheBytes, NameChunkFetchDuration, NameChunkFetchErrors} {
		f := family(t, r, name)
		if f == nil {
			t.Fatalf("%s vanished with one volume left", name)
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "volume" && l.GetValue() == pvA {
					t.Errorf("%s still has a series for the deleted volume", name)
				}
			}
		}
	}
	r.DeleteVolumeGeneration(genVolB)
	for _, name := range VolumeFamilies {
		if family(t, r, name) != nil {
			t.Errorf("%s has series after every volume left", name)
		}
	}
}

// Every {volume} label is a PV name and nothing else: an ID that does not
// name a volume is dropped, a path in particular.
func TestVolumeLabelIsAPVName(t *testing.T) {
	t.Parallel()
	r := New(WithReconciler())
	zero := blockd.FetchStats{Buckets: make([]uint64, len(blockd.FetchBuckets))}
	for _, id := range []string{"", "not a volume id", "/var/lib/kubelet/x", "volume:/var/lib/x", "volume:pvc-xyz", "directory:pvc-00000000-0000-4000-8000-00000000000a"} {
		r.SetVolumeGeneration(id, reconcile.VolumeGeneration{Fetch: zero})
	}
	r.SetVolumeGeneration(genVolA, reconcile.VolumeGeneration{Fetch: zero})
	r.SetVolumeProbe(genVolA, true)
	for _, name := range VolumeFamilies {
		f := family(t, r, name)
		if f == nil {
			t.Fatalf("%s has no series", name)
		}
		vols := map[string]bool{}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "volume" {
					vols[l.GetValue()] = true
					if !volume.IsPVName(l.GetValue()) {
						t.Errorf("%s has volume label %q, not a PV name", name, l.GetValue())
					}
				}
			}
		}
		if len(vols) != 1 {
			t.Errorf("%s has series for %v, want only the valid volume's", name, vols)
		}
	}
}

func TestPublishAndServedMetrics(t *testing.T) {
	t.Parallel()
	r := New(WithReconciler())
	r.ObservePublish(reconcile.PublishOK, 2*time.Second)
	r.ObservePublish(reconcile.PublishOK, time.Second)
	r.ObservePublish(reconcile.PublishRefused, time.Second)
	r.ObservePublish(reconcile.PublishFailed, time.Second)
	r.ObservePublish("skipped", time.Second) // not an outcome: dropped
	for res, want := range map[string]float64{"ok": 2, "refused": 1, "failed": 1} {
		if v := sample(t, r, NameGenerationPublishes, map[string]string{"result": res}).GetCounter().GetValue(); v != want {
			t.Errorf("publishes %s = %v, want %v", res, v, want)
		}
	}
	if n := len(family(t, r, NameGenerationPublishes).GetMetric()); n != 3 {
		t.Errorf("%d result series, want 3", n)
	}
	// The duration is of the successful ones.
	h := sample(t, r, NameGenerationPublishDuration, map[string]string{}).GetHistogram()
	if h.GetSampleCount() != 2 || h.GetSampleSum() != 3 {
		t.Errorf("duration = count %d sum %v, want 2 and 3", h.GetSampleCount(), h.GetSampleSum())
	}
	r.SetVolumesServed(map[string]int{"serving": 2, "down": 1, "other": 9})
	for state, want := range map[string]float64{"serving": 2, "outdated-binary": 0, "down": 1} {
		if v := sample(t, r, NameVolumesServed, map[string]string{"state": state}).GetGauge().GetValue(); v != want {
			t.Errorf("served %s = %v, want %v", state, v, want)
		}
	}
	if n := len(family(t, r, NameVolumesServed).GetMetric()); n != 3 {
		t.Errorf("%d state series, want 3", n)
	}
}

func TestCacheMetrics(t *testing.T) {
	t.Parallel()
	names := []string{"miami_cache_budget_bytes", "miami_cache_committed_bytes", "miami_cache_fs_size_bytes", "miami_cache_fs_free_bytes"}
	off := New(WithReconciler())
	off.SetCacheCapacity(1, 3, 4)
	off.SetCacheCommitted(2)
	for _, n := range names {
		if family(t, off, n) != nil {
			t.Errorf("%s exposed without WithCacheCapacity", n)
		}
	}
	on := New(WithCacheCapacity())
	on.SetCacheCapacity(95<<30, 220<<30, 100<<30)
	on.SetCacheCommitted(30 << 30)
	want := []float64{95 << 30, 30 << 30, 220 << 30, 100 << 30}
	for i, n := range names {
		f := family(t, on, n)
		if f == nil || len(f.GetMetric()) != 1 {
			t.Fatalf("%s: family = %v", n, f)
		}
		if got := f.GetMetric()[0].GetGauge().GetValue(); got != want[i] {
			t.Errorf("%s = %v, want %v", n, got, want[i])
		}
	}
	rec := httptest.NewRecorder()
	on.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if want := "miami_cache_budget_bytes "; !strings.Contains(rec.Body.String(), want) {
		t.Errorf("/metrics lacks %q", want)
	}
}

func TestWebhookMetrics(t *testing.T) {
	t.Parallel()
	off := New(WithReconciler())
	off.Outcome("mutated")
	off.Unresolved()
	for _, n := range []string{NameWebhookMutations, NameWebhookUnresolved} {
		if family(t, off, n) != nil {
			t.Errorf("%s exposed without WithWebhook", n)
		}
	}
	on := New(WithWebhook())
	on.Outcome("mutated")
	on.Outcome("mutated")
	on.Outcome("error")
	on.Outcome("some-path-or-address") // dropped: the label is a fixed list
	on.Unresolved()
	for outcome, want := range map[string]float64{"mutated": 2, "unchanged": 0, "stripped": 0, "error": 1} {
		if got := sample(t, on, NameWebhookMutations, map[string]string{"outcome": outcome}).GetCounter().GetValue(); got != want {
			t.Errorf("outcome %s = %v, want %v", outcome, got, want)
		}
	}
	if n := len(family(t, on, NameWebhookMutations).GetMetric()); n != 4 {
		t.Errorf("%d outcome series, want 4", n)
	}
	if got := family(t, on, NameWebhookUnresolved).GetMetric()[0].GetCounter().GetValue(); got != 1 {
		t.Errorf("unresolved = %v, want 1", got)
	}
}

func TestCachePatchFailures(t *testing.T) {
	t.Parallel()
	off := New(WithReconciler())
	off.IncCachePatchFailure("denied")
	if family(t, off, NameCachePatchFailures) != nil {
		t.Errorf("%s exposed without WithCacheCapacity", NameCachePatchFailures)
	}
	on := New(WithCacheCapacity())
	// Both reasons exist at zero before any failure, so an alert can see them.
	f := family(t, on, NameCachePatchFailures)
	if f == nil || len(f.GetMetric()) != 3 {
		t.Fatalf("family = %v, want three series", f)
	}
	on.IncCachePatchFailure("denied")
	on.IncCachePatchFailure("denied")
	on.IncCachePatchFailure("error")
	on.IncCachePatchFailure("anything else")
	got := map[string]float64{}
	for _, m := range family(t, on, NameCachePatchFailures).GetMetric() {
		got[m.GetLabel()[0].GetValue()] = m.GetCounter().GetValue()
	}
	if len(got) != 3 || got["denied"] != 2 || got["error"] != 2 || got["unknown"] != 0 {
		t.Errorf("series = %v, want denied 2, error 2, unknown 0", got)
	}
	on.IncCachePatchFailure("unknown")
	for _, m := range family(t, on, NameCachePatchFailures).GetMetric() {
		if m.GetLabel()[0].GetValue() == "unknown" && m.GetCounter().GetValue() != 1 {
			t.Errorf("unknown = %v, want 1", m.GetCounter().GetValue())
		}
	}
}

func TestCacheFenceState(t *testing.T) {
	t.Parallel()
	on := New(WithCacheCapacity())
	read := func() map[string]float64 {
		out := map[string]float64{}
		for _, m := range family(t, on, NameCacheFenceState).GetMetric() {
			out[m.GetLabel()[0].GetValue()] = m.GetGauge().GetValue()
		}
		return out
	}
	for _, tc := range []struct{ set, want string }{{"", "unknown"}, {"fenced", "fenced"}, {"unfenced", "unfenced"}, {"nonsense", "unknown"}, {"fenced", "fenced"}} {
		if tc.set != "" {
			on.SetCacheFenceState(tc.set)
		}
		got := read()
		if len(got) != 3 {
			t.Fatalf("series = %v, want three", got)
		}
		for state, v := range got {
			if (v == 1) != (state == tc.want) || (v != 0 && v != 1) {
				t.Errorf("after %q: %v, want only %s at 1", tc.set, got, tc.want)
			}
		}
	}
	off := New(WithReconciler())
	off.SetCacheFenceState("fenced")
	if family(t, off, NameCacheFenceState) != nil {
		t.Error("exposed without WithCacheCapacity")
	}
}

func TestWebhookConfigOKGauge(t *testing.T) {
	t.Parallel()
	off := New(WithWebhook())
	off.SetWebhookConfigOK(true)
	if family(t, off, NameWebhookConfigOK) != nil {
		t.Errorf("%s exposed without WithWebhookConfig", NameWebhookConfigOK)
	}
	on := New(WithWebhook(), WithWebhookConfig())
	if got := family(t, on, NameWebhookConfigOK).GetMetric()[0].GetGauge().GetValue(); got != 0 {
		t.Errorf("initial = %v, want 0", got)
	}
	on.SetWebhookConfigOK(true)
	if got := family(t, on, NameWebhookConfigOK).GetMetric()[0].GetGauge().GetValue(); got != 1 {
		t.Errorf("after true = %v, want 1", got)
	}
	on.SetWebhookConfigOK(false)
	if got := family(t, on, NameWebhookConfigOK).GetMetric()[0].GetGauge().GetValue(); got != 0 {
		t.Errorf("after false = %v, want 0", got)
	}
}

func TestCacheAdvertisedGauge(t *testing.T) {
	t.Parallel()
	off := New(WithReconciler())
	off.SetCacheAdvertised(1)
	if family(t, off, NameCacheAdvertised) != nil {
		t.Errorf("%s exposed without WithCacheCapacity", NameCacheAdvertised)
	}
	on := New(WithCacheCapacity())
	on.SetCacheAdvertised(80 << 30)
	f := family(t, on, NameCacheAdvertised)
	if f == nil || len(f.GetMetric()) != 1 || f.GetMetric()[0].GetGauge().GetValue() != 80<<30 {
		t.Fatalf("%s family = %v", NameCacheAdvertised, f)
	}
}

// With accounting off the runner still removes a key left behind, and a failure
// of that must be countable.
func TestPatchFailuresRegisteredWithoutAccounting(t *testing.T) {
	t.Parallel()
	if family(t, New(WithReconciler()), NameCachePatchFailures) != nil {
		t.Fatalf("%s exposed without an option", NameCachePatchFailures)
	}
	r := New(WithReconciler(), WithCachePatchFailures())
	r.IncCachePatchFailure("denied")
	if got := sample(t, r, NameCachePatchFailures, map[string]string{"reason": "denied"}).GetCounter().GetValue(); got != 1 {
		t.Errorf("denied = %v, want 1", got)
	}
	// With accounting on as well it is registered once, not twice.
	New(WithCacheCapacity(), WithCachePatchFailures())
}

func TestWebhookSecretRefusedCounter(t *testing.T) {
	t.Parallel()
	if family(t, New(WithReconciler()), NameWebhookSecretRefused) != nil {
		t.Errorf("%s exposed without WithWebhook", NameWebhookSecretRefused)
	}
	on := New(WithWebhook())
	on.SecretRefused()
	on.SecretRefused()
	if got := family(t, on, NameWebhookSecretRefused).GetMetric()[0].GetCounter().GetValue(); got != 2 {
		t.Errorf("refused = %v, want 2", got)
	}
}
