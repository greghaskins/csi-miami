package webhook

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/kube"
)

type patchCall struct {
	name    string
	indexes []int
	bundle  []byte
}

type fakeConfigs struct {
	mu      sync.Mutex
	cfg     *kube.WebhookConfig // nil: absent
	getErr  error
	patches []patchCall
}

func (f *fakeConfigs) GetMutatingWebhookConfiguration(_ context.Context, name string) (*kube.WebhookConfig, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getErr != nil {
		return nil, f.getErr
	}
	if f.cfg == nil {
		return nil, fmt.Errorf("%w: %q", kube.ErrNotFound, name)
	}
	c := *f.cfg
	c.CABundles = append([][]byte(nil), f.cfg.CABundles...)
	return &c, nil
}

func (f *fakeConfigs) PatchCABundle(_ context.Context, name string, indexes []int, bundle []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.patches = append(f.patches, patchCall{name, indexes, bundle})
	for _, i := range indexes {
		f.cfg.CABundles[i] = bundle
	}
	return nil
}

type gauge struct {
	mu    sync.Mutex
	set   bool
	value bool
}

func (g *gauge) SetWebhookConfigOK(ok bool) { g.mu.Lock(); g.set, g.value = true, ok; g.mu.Unlock() }

func newAsserter(f *fakeConfigs, g *gauge) *CABundleAsserter {
	return &CABundleAsserter{Configs: f, Name: "miami-cache", Gauge: g, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func TestAssertsBundleOnEveryWebhookEntry(t *testing.T) {
	t.Parallel()
	f := &fakeConfigs{cfg: &kube.WebhookConfig{Name: "miami-cache", CABundles: [][]byte{nil, []byte("old"), []byte("CA")}}}
	if err := newAsserter(f, &gauge{}).Assert(context.Background(), []byte("CA")); err != nil {
		t.Fatal(err)
	}
	if len(f.patches) != 1 || fmt.Sprint(f.patches[0].indexes) != "[0 1]" || string(f.patches[0].bundle) != "CA" {
		t.Errorf("patches = %+v, want entries 0 and 1 only", f.patches)
	}
}

func TestWipedBundleIsRestoredWithinOnePass(t *testing.T) {
	t.Parallel()
	f := &fakeConfigs{cfg: &kube.WebhookConfig{CABundles: [][]byte{[]byte("CA")}}}
	a := newAsserter(f, &gauge{})
	if err := a.Assert(context.Background(), []byte("CA")); err != nil {
		t.Fatal(err)
	}
	f.cfg.CABundles[0] = nil // a re-apply of the base ships the field empty
	if err := a.Assert(context.Background(), []byte("CA")); err != nil {
		t.Fatal(err)
	}
	if string(f.cfg.CABundles[0]) != "CA" || len(f.patches) != 1 {
		t.Errorf("bundle = %q after %d patches", f.cfg.CABundles[0], len(f.patches))
	}
}

func TestUnchangedBundleIsNotPatched(t *testing.T) {
	t.Parallel()
	f := &fakeConfigs{cfg: &kube.WebhookConfig{CABundles: [][]byte{[]byte("CA"), []byte("CA")}}}
	if err := newAsserter(f, &gauge{}).Assert(context.Background(), []byte("CA")); err != nil {
		t.Fatal(err)
	}
	if len(f.patches) != 0 {
		t.Errorf("patched an unchanged configuration: %+v", f.patches)
	}
}

func TestConfigOKGauge(t *testing.T) {
	t.Parallel()
	for name, cur := range map[string][]byte{"empty": nil, "different": []byte("other")} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := &fakeConfigs{cfg: &kube.WebhookConfig{CABundles: [][]byte{cur}}}
			g := &gauge{}
			// Observe the gauge at the moment of the patch: still 0 then.
			var atPatch bool
			a := newAsserter(f, g)
			a.Configs = &patchObserver{fakeConfigs: f, onPatch: func() { g.mu.Lock(); atPatch = g.value; g.mu.Unlock() }}
			if err := a.Assert(context.Background(), []byte("CA")); err != nil {
				t.Fatal(err)
			}
			if atPatch {
				t.Error("gauge was 1 before the bundle was restored")
			}
			if !g.set || !g.value {
				t.Errorf("gauge after = %+v, want 1", g)
			}
		})
	}
	// A patch that fails leaves it 0.
	f := &fakeConfigs{cfg: &kube.WebhookConfig{CABundles: [][]byte{nil}}}
	g := &gauge{}
	a := newAsserter(f, g)
	a.Configs = &patchObserver{fakeConfigs: f, err: errors.New("denied")}
	if err := a.Assert(context.Background(), []byte("CA")); err == nil {
		t.Error("a failed patch returned no error")
	}
	if !g.set || g.value {
		t.Errorf("gauge after a failed patch = %+v, want 0", g)
	}
}

type patchObserver struct {
	*fakeConfigs
	onPatch func()
	err     error
}

func (p *patchObserver) PatchCABundle(ctx context.Context, name string, idx []int, b []byte) error {
	if p.onPatch != nil {
		p.onPatch()
	}
	if p.err != nil {
		return p.err
	}
	return p.fakeConfigs.PatchCABundle(ctx, name, idx, b)
}

func TestMissingConfigurationLogsAndCountsNotOK(t *testing.T) {
	t.Parallel()
	f := &fakeConfigs{}
	g := &gauge{value: true}
	var logs strings.Builder
	a := newAsserter(f, g)
	a.Log = slog.New(slog.NewTextHandler(&logs, nil))
	if err := a.Assert(context.Background(), []byte("CA")); err == nil {
		t.Error("a missing configuration returned no error")
	}
	if !g.set || g.value {
		t.Errorf("gauge = %+v, want 0", g)
	}
	if !strings.Contains(logs.String(), "miami-cache") {
		t.Errorf("log = %q, want the configuration named", logs.String())
	}
	// An API error is not "missing", and counts the same.
	g = &gauge{value: true}
	a = newAsserter(&fakeConfigs{getErr: errors.New("boom")}, g)
	if err := a.Assert(context.Background(), []byte("CA")); err == nil || g.value {
		t.Errorf("err = %v, gauge = %+v", err, g)
	}
}

func TestConfigWithNoWebhooksIsNotOK(t *testing.T) {
	t.Parallel()
	g := &gauge{value: true}
	f := &fakeConfigs{cfg: &kube.WebhookConfig{}}
	if err := newAsserter(f, g).Assert(context.Background(), []byte("CA")); err == nil || g.value {
		t.Errorf("err = %v, gauge = %+v", err, g)
	}
}

func TestSyncerPassLoadsStoresAndAsserts(t *testing.T) {
	t.Parallel()
	sec := &fakeSecrets{}
	cfg := &fakeConfigs{cfg: &kube.WebhookConfig{CABundles: [][]byte{nil}}}
	store := &CertStore{}
	s := &Syncer{TLS: newSource(sec, t0), Asserter: newAsserter(cfg, &gauge{}), Store: store, Interval: time.Hour,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if _, err := store.GetCertificate(nil); err == nil {
		t.Fatal("a certificate before the first pass")
	}
	if err := s.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetCertificate(nil); err != nil {
		t.Error(err)
	}
	// The caBundle matches the CA actually in the Secret.
	if string(cfg.cfg.CABundles[0]) != string(sec.secret.Data["ca.crt"]) {
		t.Error("the caBundle is not the Secret's CA")
	}
}

func TestSyncerManagedNeverTouchesTheConfiguration(t *testing.T) {
	t.Parallel()
	g := &fakeSecrets{}
	if _, err := newSource(g, t0).Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	sec := &fakeSecrets{secret: g.secret, failWrites: true}
	src := newSource(sec, t0)
	src.Managed = true
	store := &CertStore{}
	s := &Syncer{TLS: src, Store: store, Interval: time.Hour, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if err := s.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetCertificate(nil); err != nil {
		t.Error(err)
	}
}

func TestSyncerRetriesAfterATransientError(t *testing.T) {
	t.Parallel()
	g := &fakeSecrets{}
	src := newSource(g, t0)
	src.Managed = true // absent: not ready
	store := &CertStore{}
	s := &Syncer{TLS: src, Store: store, Interval: 10 * time.Millisecond, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	time.Sleep(50 * time.Millisecond)
	if _, err := store.GetCertificate(nil); err == nil {
		t.Error("served without a Secret")
	}
	h := &fakeSecrets{}
	_, _ = newSource(h, t0).Load(context.Background())
	g.mu.Lock()
	g.secret = h.secret
	g.mu.Unlock()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := store.GetCertificate(nil); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := store.GetCertificate(nil); err != nil {
		t.Errorf("never picked the Secret up: %v", err)
	}
	cancel()
	<-done
}
