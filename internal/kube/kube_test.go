package kube

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

// apiServer is a stand-in for the API server that answers GETs of PVs.
func apiServer(t *testing.T, h http.HandlerFunc) *rest.Config {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return &rest.Config{Host: srv.URL}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func TestGetPV(t *testing.T) {
	t.Parallel()
	pv := corev1.PersistentVolume{
		TypeMeta:   metav1.TypeMeta{Kind: "PersistentVolume", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1"},
		Spec: corev1.PersistentVolumeSpec{PersistentVolumeSource: corev1.PersistentVolumeSource{
			CSI: &corev1.CSIPersistentVolumeSource{Driver: "d.example.com", VolumeHandle: "directory:pvc-1"},
		}},
	}
	status := func(code int32, reason metav1.StatusReason) metav1.Status {
		return metav1.Status{TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}, Status: metav1.StatusFailure, Code: code, Reason: reason, Message: "x"}
	}
	var mu sync.Mutex
	var paths []string
	cfg := apiServer(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.Method+" "+r.URL.Path)
		mu.Unlock()
		switch r.URL.Path {
		case "/api/v1/persistentvolumes/pvc-1":
			writeJSON(w, 200, pv)
		case "/api/v1/persistentvolumes/gone":
			writeJSON(w, 404, status(404, metav1.StatusReasonNotFound))
		case "/api/v1/persistentvolumes/denied":
			writeJSON(w, 403, status(403, metav1.StatusReasonForbidden))
		default:
			writeJSON(w, 500, status(500, metav1.StatusReasonInternalError))
		}
	})
	c, err := NewClient(cfg, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	got, err := c.GetPV(ctx, "pvc-1")
	if err != nil || got.Name != "pvc-1" || got.Spec.CSI.VolumeHandle != "directory:pvc-1" {
		t.Errorf("GetPV(pvc-1) = %v, %v", got, err)
	}
	if _, err := c.GetPV(ctx, "gone"); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetPV(gone) error = %v, want ErrNotFound", err)
	}
	for _, name := range []string{"denied", "broken"} {
		pv, err := c.GetPV(ctx, name)
		if err == nil || errors.Is(err, ErrNotFound) || pv != nil {
			t.Errorf("GetPV(%s) = %v, %v; want an error that is not ErrNotFound", name, pv, err)
		}
	}
	if _, err := c.GetPV(ctx, ""); err == nil {
		t.Error("an empty name was accepted")
	}
	mu.Lock()
	defer mu.Unlock()
	for _, p := range paths {
		if !strings.HasPrefix(p, "GET ") {
			t.Errorf("request %q is not a plain GET", p)
		}
	}
}

func TestGetPVHonoursTheDeadline(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	cfg := apiServer(t, func(http.ResponseWriter, *http.Request) { <-release })
	t.Cleanup(func() { close(release) })

	c, err := NewClient(cfg, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.GetPV(context.Background(), "pvc-1")
	if err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("error = %v, want a timeout that is not ErrNotFound", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error = %v, want it to wrap context.DeadlineExceeded", err)
	}
}

func TestNewClient(t *testing.T) {
	t.Parallel()
	if _, err := NewClient(nil, 0); err == nil {
		t.Error("a nil config was accepted")
	}
	cfg := &rest.Config{Host: "https://192.0.2.1:6443"}
	c, err := NewClient(cfg, 0)
	if err != nil {
		t.Fatal(err)
	}
	if c.deadline != DefaultDeadline {
		t.Errorf("deadline = %v, want the default", c.deadline)
	}
	if cfg.QPS != 0 || cfg.Burst != 0 || cfg.UserAgent != "" {
		t.Errorf("the caller's config was modified: %+v", cfg)
	}
	if _, err := NewClient(&rest.Config{Host: "https://192.0.2.1:6443", TLSClientConfig: rest.TLSClientConfig{CertFile: "x"}}, 0); err == nil {
		t.Error("an invalid TLS configuration was accepted")
	}
}

const kubeconfigYAML = `apiVersion: v1
kind: Config
clusters:
- name: c
  cluster:
    server: https://192.0.2.1:6443
contexts:
- name: ctx
  context:
    cluster: c
    user: u
current-context: ctx
users:
- name: u
  user: {}
`

func TestRestConfig(t *testing.T) {
	// Not parallel: it sets the environment.
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	dir := t.TempDir()
	good := filepath.Join(dir, "kubeconfig")
	if err := os.WriteFile(good, []byte(kubeconfigYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	bad := filepath.Join(dir, "bad")
	if err := os.WriteFile(bad, []byte("not: [valid"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := RestConfig(good)
	if err != nil || cfg.Host != "https://192.0.2.1:6443" {
		t.Errorf("RestConfig(good) = %v, %v", cfg, err)
	}
	if _, err := RestConfig(""); err == nil || !strings.Contains(err.Error(), "no kubeconfig") {
		t.Errorf("RestConfig(\"\") error = %v, want one about the missing kubeconfig", err)
	}
	if _, err := RestConfig(bad); err == nil || !strings.Contains(err.Error(), "kubeconfig") {
		t.Errorf("RestConfig(bad) error = %v", err)
	}
	if _, err := RestConfig(filepath.Join(dir, "missing")); err == nil {
		t.Error("a missing kubeconfig was accepted")
	}
}

func TestRestConfigInCluster(t *testing.T) {
	// Not parallel: it sets the environment. Without the service account files
	// the in-cluster configuration fails, and that failure is reported rather
	// than papered over by the kubeconfig.
	t.Setenv("KUBERNETES_SERVICE_HOST", "192.0.2.1")
	t.Setenv("KUBERNETES_SERVICE_PORT", "443")
	if _, err := os.Stat("/var/run/secrets/kubernetes.io/serviceaccount/token"); err == nil {
		t.Skip("this machine has a service account token")
	}
	_, err := RestConfig("")
	if err == nil || !strings.Contains(err.Error(), "in-cluster") {
		t.Errorf("error = %v, want an in-cluster configuration error", err)
	}
}
