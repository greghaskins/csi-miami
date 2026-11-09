package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/greghaskins/csi-miami/internal/kube"
	kubefake "github.com/greghaskins/csi-miami/internal/kube/fake"
	"github.com/greghaskins/csi-miami/internal/mount/fake"
)

// memBackend is the webhook's API in memory: a Secret and a configuration,
// with a record of every call.
type memBackend struct {
	mu      sync.Mutex
	secret  *kube.Secret
	bundles [][]byte
	calls   []string
}

func (m *memBackend) rec(s string) { m.calls = append(m.calls, s) }

func (m *memBackend) GetSecret(_ context.Context, _, name string) (*kube.Secret, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rec("GetSecret " + name)
	if m.secret == nil {
		return nil, fmt.Errorf("%w: %q", kube.ErrNotFound, name)
	}
	c := *m.secret
	c.Data = maps.Clone(m.secret.Data)
	return &c, nil
}

func (m *memBackend) CreateSecret(_ context.Context, s *kube.Secret) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rec("CreateSecret " + s.Name)
	if m.secret != nil {
		return kube.ErrAlreadyExists
	}
	c := *s
	c.ResourceVersion = "1"
	m.secret = &c
	return nil
}

func (m *memBackend) UpdateSecret(context.Context, *kube.Secret) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rec("UpdateSecret")
	return errors.New("unexpected")
}

func (m *memBackend) GetMutatingWebhookConfiguration(_ context.Context, name string) (*kube.WebhookConfig, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rec("GetConfig " + name)
	return &kube.WebhookConfig{Name: name, CABundles: append([][]byte(nil), m.bundles...)}, nil
}

func (m *memBackend) PatchCABundle(_ context.Context, name string, idx []int, b []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rec("PatchCABundle " + name)
	for _, i := range idx {
		m.bundles[i] = b
	}
	return nil
}

// Claims: no volumes are ever looked up for the pods these tests send.
func (m *memBackend) GetPVC(context.Context, string, string) (*kube.PVC, error) {
	return nil, errors.New("unexpected")
}
func (m *memBackend) GetPV(context.Context, string) (*kube.PV, error) {
	return nil, errors.New("unexpected")
}
func (m *memBackend) GetStorageClass(context.Context, string) (*kube.StorageClass, error) {
	return nil, errors.New("unexpected")
}
func (m *memBackend) DefaultStorageClass(context.Context) (*kube.StorageClass, error) {
	return nil, errors.New("unexpected")
}

func (m *memBackend) backend() webhookBackend {
	return webhookBackend{claims: m, secrets: m, configs: m}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

func TestControllerFlagsWebhook(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		var out, errb bytes.Buffer
		if code := runController([]string{"-h"}, &out, &errb); code != 0 {
			t.Fatalf("exit %d", code)
		}
		for _, want := range []string{"-webhook-addr string", `(default ":9814")`, `(default "miami-cache")`, `(default "miami-webhook")`, "(default 30s)"} {
			if !strings.Contains(out.String(), want) {
				t.Errorf("help lacks %q:\n%s", want, out.String())
			}
		}
	})
	t.Run("bracketed pod IP addresses are valid, IPv4 and IPv6", func(t *testing.T) {
		for _, addr := range []string{"[10.1.2.3]:9814", "[fd00::1]:9814", "[2001:db8::7]:9814"} {
			cfg := controllerConfig{webhookAddr: addr, webhookSyncInterval: time.Second, namespace: "ns", webhookService: "s", webhookConfig: "c"}
			if msg := cfg.validateWebhook(); msg != "" {
				t.Errorf("%s: %s", addr, msg)
			}
		}
		// What the manifest expands to when POD_IP is an IPv6 address would not
		// be valid without the brackets.
		cfg := controllerConfig{webhookAddr: "fd00::1:9814", webhookSyncInterval: time.Second, namespace: "ns", webhookService: "s", webhookConfig: "c"}
		if cfg.validateWebhook() == "" {
			t.Error("an unbracketed IPv6 address passed validation")
		}
		l, err := net.Listen("tcp", "[127.0.0.1]:0")
		if err != nil {
			t.Fatalf("a bracketed IPv4 address cannot be listened on: %v", err)
		}
		_ = l.Close()
	})
	t.Run("bad address exits 2", func(t *testing.T) {
		t.Setenv("POD_NAMESPACE", "miami-system")
		var errb bytes.Buffer
		if code := runController([]string{"-webhook-addr=nonsense"}, io.Discard, &errb); code != 2 || !strings.Contains(errb.String(), "-webhook-addr") {
			t.Errorf("exit %d, stderr %q", code, errb.String())
		}
	})
	t.Run("no namespace exits 2", func(t *testing.T) {
		t.Setenv("POD_NAMESPACE", "")
		namespaceFile = filepath.Join(t.TempDir(), "absent")
		var errb bytes.Buffer
		if code := runController(nil, io.Discard, &errb); code != 2 || !strings.Contains(errb.String(), "POD_NAMESPACE") {
			t.Errorf("exit %d, stderr %q", code, errb.String())
		}
	})
	t.Run("namespace falls back to the service account's", func(t *testing.T) {
		t.Setenv("POD_NAMESPACE", "")
		f := filepath.Join(t.TempDir(), "ns")
		if err := os.WriteFile(f, []byte("from-file\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		namespaceFile = f
		if got := podNamespace(); got != "from-file" {
			t.Errorf("podNamespace = %q", got)
		}
		t.Setenv("POD_NAMESPACE", "from-env")
		if got := podNamespace(); got != "from-env" {
			t.Errorf("podNamespace = %q", got)
		}
	})
	t.Run("zero sync interval exits 2", func(t *testing.T) {
		t.Setenv("POD_NAMESPACE", "miami-system")
		var errb bytes.Buffer
		if code := runController([]string{"-webhook-sync-interval=0s"}, io.Discard, &errb); code != 2 {
			t.Errorf("exit %d, stderr %q", code, errb.String())
		}
	})
	t.Run("empty address turns it off without a namespace", func(t *testing.T) {
		t.Setenv("POD_NAMESPACE", "")
		t.Setenv("KUBERNETES_SERVICE_HOST", "")
		t.Setenv("KUBERNETES_SERVICE_PORT", "")
		var errb bytes.Buffer
		// Past the flags it reaches the cluster check and fails there (1).
		if code := runController([]string{"-webhook-addr="}, io.Discard, &errb); code != 1 || !strings.Contains(errb.String(), "no kubeconfig") {
			t.Errorf("exit %d, stderr %q", code, errb.String())
		}
	})
	t.Run("disabled touches no Secret and no listener", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		mem := &memBackend{}
		cfg := controllerConfig{endpoint: "unix://" + filepath.Join(dir, "csi.sock"), root: dir, deadline: time.Second, metricsAddr: "127.0.0.1:0"}
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		if err := serveControllerWith(ctx, cfg, &kubefake.Reader{}, &fake.Mounter{}, mem.backend(), slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
			t.Fatal(err)
		}
		if len(mem.calls) != 0 {
			t.Errorf("API calls with the webhook off: %v", mem.calls)
		}
	})
}

func webhookConfig(dir string, addr string) controllerConfig {
	return controllerConfig{
		endpoint: "unix://" + filepath.Join(dir, "csi.sock"), root: filepath.Join(dir, "root"), deadline: time.Second, metricsAddr: "127.0.0.1:0",
		webhookAddr: addr, webhookConfig: "miami-cache", webhookService: "miami-webhook", webhookSyncInterval: 20 * time.Millisecond, namespace: "miami-system",
	}
}

func TestControllerServesWebhookAndCSI(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	addr := freeAddr(t)
	mem := &memBackend{bundles: [][]byte{nil}}
	cfg := webhookConfig(dir, addr)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- serveControllerWith(ctx, cfg, &kubefake.Reader{}, &fake.Mounter{}, mem.backend(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()

	conn, err := grpc.NewClient("unix:"+filepath.Join(dir, "csi.sock"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	callCtx, callCancel := context.WithTimeout(ctx, 10*time.Second)
	defer callCancel()
	if _, err := csi.NewIdentityClient(conn).GetPluginInfo(callCtx, &csi.GetPluginInfoRequest{}, grpc.WaitForReady(true)); err != nil {
		t.Fatalf("CSI: %v", err)
	}

	// The webhook answers once the first sync pass has loaded a certificate.
	hc := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}} //nolint:gosec // the test checks the bundle, not the chain
	body := `{"apiVersion":"admission.k8s.io/v1","kind":"AdmissionReview","request":{"uid":"u1","operation":"DELETE","kind":{"version":"v1","kind":"Pod"}}}`
	var out struct {
		Response struct {
			UID     string `json:"uid"`
			Allowed bool   `json:"allowed"`
		} `json:"response"`
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := hc.Post("https://"+addr+"/mutate", "application/json", strings.NewReader(body))
		if err == nil {
			err = json.NewDecoder(resp.Body).Decode(&out)
			_ = resp.Body.Close()
			if err == nil {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("webhook never answered: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if out.Response.UID != "u1" || !out.Response.Allowed {
		t.Errorf("response = %+v", out)
	}

	// The caBundle was set to the CA in the Secret.
	deadline = time.Now().Add(5 * time.Second)
	for {
		mem.mu.Lock()
		ok := mem.secret != nil && bytes.Equal(mem.bundles[0], mem.secret.Data["ca.crt"]) && len(mem.bundles[0]) > 0
		mem.mu.Unlock()
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the caBundle never matched the Secret's CA")
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("serveControllerWith = %v after a requested stop", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("did not stop")
	}
	if _, err := hc.Post("https://"+addr+"/mutate", "application/json", strings.NewReader(body)); err == nil {
		t.Error("the webhook still answers after the stop")
	}
	if _, err := (&net.Dialer{Timeout: time.Second}).DialContext(context.Background(), "unix", filepath.Join(dir, "csi.sock")); err == nil {
		t.Error("the CSI socket still accepts")
	}
}

func TestControllerWebhookManagedSecretIsReadOnly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	addr := freeAddr(t)
	mem := &memBackend{bundles: [][]byte{nil}} // no Secret: nothing to serve yet
	cfg := webhookConfig(dir, addr)
	cfg.webhookTLSSecret = "their-cert"
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	if err := serveControllerWith(ctx, cfg, &kubefake.Reader{}, &fake.Mounter{}, mem.backend(), slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatal(err)
	}
	mem.mu.Lock()
	defer mem.mu.Unlock()
	if len(mem.calls) == 0 {
		t.Fatal("the managed Secret was never read")
	}
	for _, c := range mem.calls {
		if c != "GetSecret their-cert" {
			t.Errorf("call %q in managed mode", c)
		}
	}
}

func TestControllerWebhookPortInUseIsFatal(t *testing.T) {
	t.Parallel()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	dir := t.TempDir()
	cfg := webhookConfig(dir, l.Addr().String())
	err = serveControllerWith(context.Background(), cfg, &kubefake.Reader{}, &fake.Mounter{}, (&memBackend{}).backend(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil || !strings.Contains(err.Error(), "webhook address") {
		t.Errorf("err = %v", err)
	}
}
