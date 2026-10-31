package metrics

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/greghaskins/csi-miami/internal/reconcile"
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
	r := New()
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
	r := New()
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
