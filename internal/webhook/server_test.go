package webhook

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type fakeMutator struct {
	calls atomic.Int32
	patch []byte
}

func (f *fakeMutator) Mutate(_ context.Context, req *admissionv1.AdmissionRequest) *admissionv1.AdmissionResponse {
	f.calls.Add(1)
	resp := &admissionv1.AdmissionResponse{UID: req.UID, Allowed: true}
	if f.patch != nil {
		pt := admissionv1.PatchTypeJSONPatch
		resp.Patch, resp.PatchType = f.patch, &pt
	}
	return resp
}

type testServer struct {
	url  string
	mut  *fakeMutator
	http *http.Client
	ca   *x509.CertPool
	addr string
}

func startServer(t *testing.T) *testServer {
	t.Helper()
	sec := &fakeSecrets{}
	m, err := newSource(sec, time.Now()).Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	store := &CertStore{}
	cert := m.Cert
	store.Set(&cert)
	mut := &fakeMutator{patch: []byte(`[]`)}
	srv := &Server{Addr: "127.0.0.1:0", Mutator: mut, GetCertificate: store.GetCertificate, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run = %v after a requested stop", err)
		}
	})
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(m.CA)
	c := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "miami-webhook.miami-system.svc"}}}
	return &testServer{url: "https://" + srv.ListenAddr().String(), mut: mut, http: c, ca: pool, addr: srv.ListenAddr().String()}
}

func reviewBody(version, kind string, uid types.UID) []byte {
	b, _ := json.Marshal(map[string]any{
		"apiVersion": version, "kind": kind,
		"request": map[string]any{"uid": uid, "operation": "CREATE", "kind": map[string]any{"version": "v1", "kind": "Pod"}},
	})
	return b
}

func TestServerRoundTripsAdmissionReview(t *testing.T) {
	t.Parallel()
	ts := startServer(t)
	resp, err := ts.http.Post(ts.url+"/mutate", "application/json", bytes.NewReader(reviewBody("admission.k8s.io/v1", "AdmissionReview", "u-1")))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out admissionv1.AdmissionReview
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 || out.APIVersion != "admission.k8s.io/v1" || out.Kind != "AdmissionReview" ||
		out.Response == nil || out.Response.UID != "u-1" || !out.Response.Allowed || string(out.Response.Patch) != "[]" {
		t.Errorf("status %d, review %+v", resp.StatusCode, out)
	}
	if resp.Header.Get("Content-Type") != "application/json" {
		t.Errorf("content type = %q", resp.Header.Get("Content-Type"))
	}
}

func TestServerRejectsOtherAdmissionVersions(t *testing.T) {
	t.Parallel()
	ts := startServer(t)
	for name, body := range map[string][]byte{
		"v1beta1":    reviewBody("admission.k8s.io/v1beta1", "AdmissionReview", "u"),
		"wrong kind": reviewBody("admission.k8s.io/v1", "Pod", "u"),
		"no request": []byte(`{"apiVersion":"admission.k8s.io/v1","kind":"AdmissionReview"}`),
		"not json":   []byte(`nope`),
	} {
		resp, err := ts.http.Post(ts.url+"/mutate", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", name, resp.StatusCode)
		}
	}
	if n := ts.mut.calls.Load(); n != 0 {
		t.Errorf("the mutator ran %d times for rejected reviews", n)
	}
}

func TestServerRejectsNonPostAndOversizedBody(t *testing.T) {
	t.Parallel()
	ts := startServer(t)
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		req, _ := http.NewRequest(method, ts.url+"/mutate", nil)
		resp, err := ts.http.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed || resp.Header.Get("Allow") != "POST" {
			t.Errorf("%s: status = %d, Allow = %q", method, resp.StatusCode, resp.Header.Get("Allow"))
		}
	}

	// Above the bound with a declared length: refused up front.
	big := bytes.Repeat([]byte("a"), maxBodyBytes+1)
	resp, err := ts.http.Post(ts.url+"/mutate", "application/json", bytes.NewReader(big))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized: status = %d, want 413", resp.StatusCode)
	}

	// Chunked, no declared length: the reader stops at the limit instead of
	// reading to the end.
	var served atomic.Int64
	r := io.MultiReader(countingReader{bytes.NewReader(bytes.Repeat([]byte("a"), 64<<20)), &served})
	req, _ := http.NewRequest(http.MethodPost, ts.url+"/mutate", r)
	req.Header.Set("Content-Type", "application/json")
	resp, err = ts.http.Do(req)
	if err == nil {
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusRequestEntityTooLarge {
			t.Errorf("chunked oversized: status = %d, want 413", resp.StatusCode)
		}
	}
	if got := served.Load(); got > 16<<20 {
		t.Errorf("the server read %d bytes of a 64 MiB body", got)
	}
	if n := ts.mut.calls.Load(); n != 0 {
		t.Errorf("the mutator ran %d times", n)
	}
}

type countingReader struct {
	r io.Reader
	n *atomic.Int64
}

func (c countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n.Add(int64(n))
	return n, err
}

// A pod the API server accepts (up to its 3 MiB limit) is processed, not
// rejected: a 413 would be a webhook failure, and the pod would be admitted
// as sent.
func TestServerProcessesLargeReview(t *testing.T) {
	t.Parallel()
	ts := startServer(t)
	pad := strings.Repeat("x", 3<<20)
	body, _ := json.Marshal(map[string]any{
		"apiVersion": "admission.k8s.io/v1", "kind": "AdmissionReview",
		"request": map[string]any{"uid": "big", "operation": "CREATE", "kind": map[string]any{"version": "v1", "kind": "Pod"},
			"object": map[string]any{"metadata": map[string]any{"annotations": map[string]any{"pad": pad}}}},
	})
	resp, err := ts.http.Post(ts.url+"/mutate", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out admissionv1.AdmissionReview
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil || resp.StatusCode != 200 || out.Response == nil || out.Response.UID != "big" {
		t.Errorf("status %d, review %+v, err %v", resp.StatusCode, out, err)
	}
	if ts.mut.calls.Load() != 1 {
		t.Errorf("mutator ran %d times", ts.mut.calls.Load())
	}
}

func TestServerServesOnlyMutatePath(t *testing.T) {
	t.Parallel()
	ts := startServer(t)
	for _, p := range []string{"/", "/mutate/", "/mutate/x", "/metrics", "/healthz", "/MUTATE"} {
		resp, err := ts.http.Post(ts.url+p, "application/json", strings.NewReader(string(reviewBody("admission.k8s.io/v1", "AdmissionReview", "u"))))
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("%s: status = %d, want 404", p, resp.StatusCode)
		}
	}
}

func TestServerTLSMinVersion12(t *testing.T) {
	t.Parallel()
	ts := startServer(t)
	conn, err := tls.Dial("tcp", ts.addr, &tls.Config{RootCAs: ts.ca, ServerName: "miami-webhook.miami-system.svc", MaxVersion: tls.VersionTLS11}) //nolint:gosec // proving the server refuses it
	if err == nil {
		_ = conn.Close()
		t.Fatal("a TLS 1.1 handshake succeeded")
	}
	conn, err = tls.Dial("tcp", ts.addr, &tls.Config{RootCAs: ts.ca, ServerName: "miami-webhook.miami-system.svc", MinVersion: tls.VersionTLS12})
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	// And plain HTTP is not served.
	resp, err := http.Get("http://" + ts.addr + "/mutate")
	if err == nil {
		_ = resp.Body.Close()
		if resp.StatusCode == 200 {
			t.Error("plain HTTP answered 200")
		}
	}
}

func TestServerHasTimeouts(t *testing.T) {
	t.Parallel()
	srv := (&Server{Addr: "127.0.0.1:0", Mutator: &fakeMutator{}, GetCertificate: (&CertStore{}).GetCertificate}).httpServer()
	if srv.ReadHeaderTimeout <= 0 || srv.ReadTimeout <= 0 || srv.WriteTimeout <= 0 || srv.IdleTimeout <= 0 || srv.MaxHeaderBytes <= 0 {
		t.Errorf("timeouts not all set: %+v", srv)
	}
	if srv.WriteTimeout <= mutateDeadline {
		t.Errorf("WriteTimeout %v is not above the mutate deadline %v", srv.WriteTimeout, mutateDeadline)
	}
	if srv.TLSConfig.MinVersion < tls.VersionTLS12 {
		t.Error("TLS minimum below 1.2")
	}
}

var _ = metav1.Status{}
