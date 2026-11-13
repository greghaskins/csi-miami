package webhook

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/kube"
)

// fakeSecrets is an in-memory Secrets with resourceVersions, so that a
// conflicting write is refused as the API server would.
type fakeSecrets struct {
	mu      sync.Mutex
	secret  *kube.Secret // nil: absent
	version int
	creates int
	updates int
	gets    int
	// failWrites makes every create and update fail the test's expectations.
	failWrites bool
	// beforeWrite runs (without the lock) before a create or update is
	// evaluated: a second replica getting in first.
	beforeWrite func(f *fakeSecrets)
}

func (f *fakeSecrets) GetSecret(_ context.Context, _, name string) (*kube.Secret, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets++
	if f.secret == nil {
		return nil, fmt.Errorf("%w: Secret %q", kube.ErrNotFound, name)
	}
	c := *f.secret
	c.Data = maps.Clone(f.secret.Data)
	return &c, nil
}

func (f *fakeSecrets) hook() {
	f.mu.Lock()
	h := f.beforeWrite
	f.beforeWrite = nil
	f.mu.Unlock()
	if h != nil {
		h(f)
	}
}

func (f *fakeSecrets) CreateSecret(_ context.Context, s *kube.Secret) error {
	f.hook()
	f.mu.Lock()
	defer f.mu.Unlock()
	f.creates++
	if f.failWrites {
		return errors.New("write attempted")
	}
	if f.secret != nil {
		return kube.ErrAlreadyExists
	}
	f.version++
	c := *s
	c.Data = maps.Clone(s.Data)
	c.ResourceVersion = strconv.Itoa(f.version)
	f.secret = &c
	return nil
}

func (f *fakeSecrets) UpdateSecret(_ context.Context, s *kube.Secret) error {
	f.hook()
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updates++
	if f.failWrites {
		return errors.New("write attempted")
	}
	if f.secret == nil || s.ResourceVersion != f.secret.ResourceVersion {
		return kube.ErrConflict
	}
	f.version++
	c := *s
	c.Data = maps.Clone(s.Data)
	c.ResourceVersion = strconv.Itoa(f.version)
	f.secret = &c
	return nil
}

func (f *fakeSecrets) writes() int { f.mu.Lock(); defer f.mu.Unlock(); return f.creates + f.updates }

// ownLabels is what the controller puts on the Secret it creates.
var ownLabels = map[string]string{kube.SecretOwnerLabel: kube.SecretOwnerValue}

// brokenOurs is data the controller wrote and that is no longer usable: it has
// a ca.key, which only the controller writes, but nothing that parses.
func brokenOurs() map[string][]byte { return map[string][]byte{"ca.key": []byte("w")} }

var t0 = time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)

func newSource(f *fakeSecrets, now time.Time) *TLSSource {
	return &TLSSource{Secrets: f, Namespace: "miami-system", Name: "miami-webhook-tls", Service: "miami-webhook",
		Now: func() time.Time { return now }, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func parseCert(t *testing.T, p []byte) *x509.Certificate {
	t.Helper()
	b, _ := pem.Decode(p)
	if b == nil {
		t.Fatal("not PEM")
	}
	c, err := x509.ParseCertificate(b.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestBootstrapCreatesSecretWhenAbsent(t *testing.T) {
	t.Parallel()
	f := &fakeSecrets{}
	m, err := newSource(f, t0).Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if f.creates != 1 || f.updates != 0 {
		t.Errorf("creates = %d, updates = %d", f.creates, f.updates)
	}
	if f.secret.Type != "kubernetes.io/tls" || f.secret.Namespace != "miami-system" || f.secret.Name != "miami-webhook-tls" {
		t.Errorf("secret = %+v", f.secret)
	}
	for _, k := range []string{"tls.crt", "tls.key", "ca.crt", "ca.key"} {
		if len(f.secret.Data[k]) == 0 {
			t.Errorf("secret lacks %s", k)
		}
	}
	if string(m.CA) != string(f.secret.Data["ca.crt"]) {
		t.Error("the CA returned is not the CA in the Secret")
	}
	if len(m.Cert.Certificate) == 0 {
		t.Error("no serving certificate")
	}
	ca, leaf := parseCert(t, m.CA), parseCert(t, f.secret.Data["tls.crt"])
	if err := leaf.CheckSignatureFrom(ca); err != nil {
		t.Errorf("leaf not signed by the CA: %v", err)
	}
	if got := ca.NotAfter.Sub(t0); got < 9*365*24*time.Hour {
		t.Errorf("CA lives %v", got)
	}
	if got := leaf.NotAfter.Sub(t0); got > 366*24*time.Hour || got < 360*24*time.Hour {
		t.Errorf("leaf lives %v", got)
	}
}

func TestSANsAreTheServiceNames(t *testing.T) {
	t.Parallel()
	f := &fakeSecrets{}
	if _, err := newSource(f, t0).Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	leaf := parseCert(t, f.secret.Data["tls.crt"])
	want := []string{"miami-webhook.miami-system.svc", "miami-webhook.miami-system.svc.cluster.local"}
	if got := slices.Clone(leaf.DNSNames); !slices.Equal(got, want) || len(leaf.IPAddresses) != 0 {
		t.Errorf("SANs = %v %v, want %v", got, leaf.IPAddresses, want)
	}
	// A different service name is a different set of names: regenerated.
	src := newSource(f, t0)
	src.Service = "other"
	before := f.secret.Data["tls.crt"]
	if _, err := src.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	if string(f.secret.Data["tls.crt"]) == string(before) {
		t.Error("a certificate for other names was reused")
	}
}

func TestBootstrapReusesValidSecret(t *testing.T) {
	t.Parallel()
	f := &fakeSecrets{}
	if _, err := newSource(f, t0).Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	w := f.writes()
	m, err := newSource(f, t0.Add(24*time.Hour)).Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if f.writes() != w {
		t.Error("a valid Secret was written")
	}
	if string(m.CA) != string(f.secret.Data["ca.crt"]) {
		t.Error("CA differs")
	}
}

func TestBootstrapRegeneratesExpired(t *testing.T) {
	t.Parallel()
	f := &fakeSecrets{}
	if _, err := newSource(f, t0).Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	old := f.secret.Data["tls.crt"]
	if _, err := newSource(f, t0.Add(400*24*time.Hour)).Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	if string(f.secret.Data["tls.crt"]) == string(old) {
		t.Error("an expired certificate was kept")
	}
}

func TestBootstrapRegeneratesNearExpiry(t *testing.T) {
	t.Parallel()
	f := &fakeSecrets{}
	if _, err := newSource(f, t0).Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	old := f.secret.Data["tls.crt"]
	// 31 days left: kept. 29 days left: renewed.
	if _, err := newSource(f, t0.Add(334*24*time.Hour)).Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	if string(f.secret.Data["tls.crt"]) != string(old) {
		t.Error("renewed with 31 days left")
	}
	if _, err := newSource(f, t0.Add(336*24*time.Hour)).Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	if string(f.secret.Data["tls.crt"]) == string(old) {
		t.Error("not renewed with 29 days left")
	}
}

func TestBootstrapKeepsCAWhenRenewingServingCert(t *testing.T) {
	t.Parallel()
	f := &fakeSecrets{}
	m1, err := newSource(f, t0).Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	oldLeaf := f.secret.Data["tls.crt"]
	m2, err := newSource(f, t0.Add(340*24*time.Hour)).Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if string(f.secret.Data["tls.crt"]) == string(oldLeaf) {
		t.Fatal("the serving certificate was not renewed")
	}
	if string(m1.CA) != string(m2.CA) {
		t.Error("the CA changed with the serving certificate")
	}
	if err := parseCert(t, f.secret.Data["tls.crt"]).CheckSignatureFrom(parseCert(t, m1.CA)); err != nil {
		t.Errorf("renewed leaf is not signed by the kept CA: %v", err)
	}
}

func TestBootstrapRegeneratesGarbage(t *testing.T) {
	t.Parallel()
	for name, data := range map[string]map[string][]byte{
		"only a CA key": brokenOurs(),
		"garbage":       {"tls.crt": []byte("x"), "tls.key": []byte("y"), "ca.crt": []byte("z"), "ca.key": []byte("w")},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := &fakeSecrets{secret: &kube.Secret{Namespace: "miami-system", Name: "miami-webhook-tls", Type: "kubernetes.io/tls", Data: data, Labels: ownLabels, ResourceVersion: "1"}, version: 1}
			m, err := newSource(f, t0).Load(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if f.updates != 1 || f.creates != 0 {
				t.Errorf("creates = %d, updates = %d", f.creates, f.updates)
			}
			if string(m.CA) != string(f.secret.Data["ca.crt"]) {
				t.Error("CA returned is not the CA in the Secret")
			}
		})
	}
}

func TestBootstrapRegeneratesWhenSignedByAnotherCA(t *testing.T) {
	t.Parallel()
	a, b := &fakeSecrets{}, &fakeSecrets{}
	if _, err := newSource(a, t0).Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := newSource(b, t0).Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	mixed := maps.Clone(a.secret.Data)
	mixed["tls.crt"], mixed["tls.key"] = b.secret.Data["tls.crt"], b.secret.Data["tls.key"]
	f := &fakeSecrets{secret: &kube.Secret{Namespace: "miami-system", Name: "miami-webhook-tls", Data: mixed, Labels: ownLabels, ResourceVersion: "1"}, version: 1}
	if _, err := newSource(f, t0).Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := parseCert(t, f.secret.Data["tls.crt"]).CheckSignatureFrom(parseCert(t, f.secret.Data["ca.crt"])); err != nil {
		t.Errorf("not repaired: %v", err)
	}
	if string(f.secret.Data["ca.crt"]) != string(a.secret.Data["ca.crt"]) {
		t.Error("a usable CA was replaced")
	}
}

func TestManagedSecretIsNeverWritten(t *testing.T) {
	t.Parallel()
	// Material written by the self-signed path, minus ca.key, as a manager
	// like cert-manager would leave it.
	g := &fakeSecrets{}
	if _, err := newSource(g, t0).Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	good := maps.Clone(g.secret.Data)
	delete(good, "ca.key")

	cases := map[string]struct {
		secret  *kube.Secret
		wantErr bool
	}{
		"absent":          {nil, true},
		"complete":        {&kube.Secret{Data: good}, false},
		"missing ca.crt":  {&kube.Secret{Data: map[string][]byte{"tls.crt": good["tls.crt"], "tls.key": good["tls.key"]}}, true},
		"missing tls.key": {&kube.Secret{Data: map[string][]byte{"tls.crt": good["tls.crt"], "ca.crt": good["ca.crt"]}}, true},
		"missing tls.crt": {&kube.Secret{Data: map[string][]byte{"tls.key": good["tls.key"], "ca.crt": good["ca.crt"]}}, true},
		"garbage":         {&kube.Secret{Data: map[string][]byte{"tls.crt": []byte("x"), "tls.key": []byte("y"), "ca.crt": []byte("z")}}, true},
		"expired":         {&kube.Secret{Data: good}, false}, // an expired managed certificate is still the manager's to renew
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := &fakeSecrets{secret: tc.secret, failWrites: true}
			src := newSource(f, t0.Add(1000*24*time.Hour))
			src.Managed = true
			m, err := src.Load(context.Background())
			if (err != nil) != tc.wantErr {
				t.Errorf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if err == nil && (m == nil || len(m.Cert.Certificate) == 0) {
				t.Error("no certificate")
			}
			if f.creates+f.updates != 0 {
				t.Errorf("a managed Secret was written (%d creates, %d updates)", f.creates, f.updates)
			}
			if tc.secret == nil && !errors.Is(err, errNotReady) {
				t.Errorf("err = %v, want errNotReady", err)
			}
		})
	}
}

func TestUpdateConflictRetriesThenSucceeds(t *testing.T) {
	t.Parallel()
	// An unusable Secret; another replica repairs it between our read and our
	// write. Our update conflicts, we re-read, find the winner's material
	// valid, and adopt it instead of overwriting.
	g := &fakeSecrets{}
	winner, err := newSource(g, t0).Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeSecrets{secret: &kube.Secret{Namespace: "miami-system", Name: "miami-webhook-tls", Data: brokenOurs(), Labels: ownLabels, ResourceVersion: "1"}, version: 1}
	f.beforeWrite = func(f *fakeSecrets) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.version++
		f.secret = &kube.Secret{Namespace: "miami-system", Name: "miami-webhook-tls", Data: maps.Clone(g.secret.Data), ResourceVersion: strconv.Itoa(f.version)}
	}
	m, err := newSource(f, t0).Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if string(m.CA) != string(winner.CA) || string(f.secret.Data["ca.crt"]) != string(winner.CA) {
		t.Error("the CA did not converge on the winner's")
	}
	if f.updates != 1 {
		t.Errorf("updates = %d, want only the one that conflicted", f.updates)
	}
}

func TestCreateRaceConvergesOnOneCA(t *testing.T) {
	t.Parallel()
	g := &fakeSecrets{}
	winner, _ := newSource(g, t0).Load(context.Background())
	f := &fakeSecrets{}
	f.beforeWrite = func(f *fakeSecrets) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.version++
		f.secret = &kube.Secret{Namespace: "miami-system", Name: "miami-webhook-tls", Data: maps.Clone(g.secret.Data), ResourceVersion: "9"}
	}
	m, err := newSource(f, t0).Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if string(m.CA) != string(winner.CA) {
		t.Error("the loser kept its own CA")
	}
	if f.creates != 1 {
		t.Errorf("creates = %d", f.creates)
	}
}

func TestPersistentConflictGivesUp(t *testing.T) {
	t.Parallel()
	f := &fakeSecrets{secret: &kube.Secret{Namespace: "miami-system", Name: "miami-webhook-tls", Data: brokenOurs(), Labels: ownLabels, ResourceVersion: "1"}, version: 1}
	// Every write loses: the version moves under us each time.
	src := newSource(f, t0)
	src.Secrets = &alwaysConflict{fakeSecrets: f}
	if _, err := src.Load(context.Background()); err == nil {
		t.Error("no error after endless conflicts")
	}
}

type alwaysConflict struct{ *fakeSecrets }

func (alwaysConflict) UpdateSecret(context.Context, *kube.Secret) error { return kube.ErrConflict }

func TestCreatedSecretCarriesTheOwnerLabel(t *testing.T) {
	t.Parallel()
	f := &fakeSecrets{}
	if _, err := newSource(f, t0).Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f.secret.Labels[kube.SecretOwnerLabel] != kube.SecretOwnerValue {
		t.Errorf("labels = %v, want the ownership label", f.secret.Labels)
	}
}

type refusals struct{ n int }

func (r *refusals) SecretRefused() { r.n++ }

// TestSelfSignedModeDoesNotOverwriteASecretItDidNotCreate: forgetting
// -webhook-tls-secret must not cost a cert-manager Secret its key and CA. The
// Secret is left as it was, the Error names the flag, and the refusal is
// counted on every pass.
func TestSelfSignedModeDoesNotOverwriteASecretItDidNotCreate(t *testing.T) {
	t.Parallel()
	g := &fakeSecrets{}
	if _, err := newSource(g, t0).Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	withoutCAKey := maps.Clone(g.secret.Data)
	delete(withoutCAKey, "ca.key")
	for name, sec := range map[string]*kube.Secret{
		"no ca.key (cert-manager's)": {Data: withoutCAKey, Labels: ownLabels},
		"no ca.key and no label":     {Data: withoutCAKey},
		"empty":                      {Data: map[string][]byte{}},
		"no label":                   {Data: map[string][]byte{"ca.key": []byte("k"), "tls.crt": []byte("x")}},
		"another owner's label":      {Data: brokenOurs(), Labels: map[string]string{kube.SecretOwnerLabel: "cert-manager"}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			sec.Namespace, sec.Name, sec.Type, sec.ResourceVersion = "miami-system", "miami-webhook-tls", "kubernetes.io/tls", "1"
			before := maps.Clone(sec.Data)
			f := &fakeSecrets{secret: sec, version: 1, failWrites: true}
			var logs bytes.Buffer
			ref := &refusals{}
			src := newSource(f, t0)
			src.Log, src.Metrics = slog.New(slog.NewTextHandler(&logs, nil)), ref
			for i := 0; i < 2; i++ {
				if _, err := src.Load(context.Background()); !errors.Is(err, errNotOurs) {
					t.Fatalf("pass %d: err = %v, want errNotOurs", i, err)
				}
			}
			if f.creates+f.updates != 0 {
				t.Errorf("the Secret was written (%d creates, %d updates)", f.creates, f.updates)
			}
			if !maps.EqualFunc(before, f.secret.Data, slices.Equal[[]byte]) {
				t.Error("the Secret's data changed")
			}
			if ref.n != 2 {
				t.Errorf("refusals counted = %d, want 2", ref.n)
			}
			if n := strings.Count(logs.String(), "level=ERROR"); n != 1 || !strings.Contains(logs.String(), "-webhook-tls-secret") {
				t.Errorf("want one Error naming -webhook-tls-secret, got %d:\n%s", n, logs.String())
			}
		})
	}
}

// TestAValidSecretIsUsedWhoseverItIs: nothing is written, so nothing is refused.
func TestAValidSecretIsUsedWhoseverItIs(t *testing.T) {
	t.Parallel()
	g := &fakeSecrets{}
	if _, err := newSource(g, t0).Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	sec := *g.secret
	sec.Labels = nil
	f := &fakeSecrets{secret: &sec, version: 1, failWrites: true}
	if _, err := newSource(f, t0).Load(context.Background()); err != nil {
		t.Fatal(err)
	}
}
