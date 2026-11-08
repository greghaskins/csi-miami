package webhook

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/greghaskins/csi-miami/internal/kube"
)

// Configs is the part of the API the caBundle assertion needs.
// *kube.Client implements it.
type Configs interface {
	GetMutatingWebhookConfiguration(ctx context.Context, name string) (*kube.WebhookConfig, error)
	// PatchCABundle sets clientConfig.caBundle of the webhooks at indexes, and
	// touches nothing else.
	PatchCABundle(ctx context.Context, name string, indexes []int, bundle []byte) error
}

var _ Configs = (*kube.Client)(nil)

// ConfigGauge is where the assertion reports whether the configuration carries
// the bundle (miami_webhook_config_ok).
type ConfigGauge interface{ SetWebhookConfigOK(ok bool) }

// CABundleAsserter keeps the caBundle of every webhook of one
// MutatingWebhookConfiguration equal to the CA the controller serves with. The
// base ships the field empty and a re-apply of it wipes the bundle, so this
// runs on every pass, not only at start.
type CABundleAsserter struct {
	Configs Configs
	Name    string
	Gauge   ConfigGauge
	Log     *slog.Logger
}

func (a *CABundleAsserter) log() *slog.Logger {
	if a.Log != nil {
		return a.Log
	}
	return slog.Default()
}

func (a *CABundleAsserter) ok(v bool) {
	if a.Gauge != nil {
		a.Gauge.SetWebhookConfigOK(v)
	}
}

// Assert makes every webhook of the configuration carry ca. The gauge is 1
// only once the configuration is known to, and 0 whenever it is not (a missing
// or unreadable configuration, a patch that failed, a configuration with no
// webhook to carry it).
func (a *CABundleAsserter) Assert(ctx context.Context, ca []byte) error {
	cfg, err := a.Configs.GetMutatingWebhookConfiguration(ctx, a.Name)
	if err != nil {
		a.ok(false)
		if errors.Is(err, kube.ErrNotFound) {
			a.log().Error("the webhook configuration does not exist, so there is nowhere to put the caBundle", "configuration", a.Name)
		} else {
			a.log().Warn("could not read the webhook configuration", "configuration", a.Name, "error", err)
		}
		return fmt.Errorf("webhook configuration %q: %w", a.Name, err)
	}
	if len(cfg.CABundles) == 0 {
		a.ok(false)
		a.log().Error("the webhook configuration has no webhooks", "configuration", a.Name)
		return fmt.Errorf("webhook configuration %q has no webhooks", a.Name)
	}
	var stale []int
	for i, b := range cfg.CABundles {
		if !sameBundle(b, ca) {
			stale = append(stale, i)
		}
	}
	if len(stale) == 0 {
		a.ok(true)
		return nil
	}
	a.ok(false)
	if err := a.Configs.PatchCABundle(ctx, a.Name, stale, ca); err != nil {
		a.log().Warn("could not set the caBundle of the webhook configuration", "configuration", a.Name, "error", err)
		return err
	}
	a.log().Info("set the caBundle of the webhook configuration", "configuration", a.Name, "webhooks", len(stale))
	a.ok(true)
	return nil
}

// Syncer is the loop behind the webhook's TLS: each pass loads the material
// (creating or repairing the Secret when self-signed), hands the serving
// certificate to the store, and asserts the caBundle when there is an
// Asserter. A failed pass is logged and retried at the next interval, never
// fatal: the webhook must come up while the API server is slow.
type Syncer struct {
	TLS      *TLSSource
	Asserter *CABundleAsserter // nil when the configuration is not ours to write
	Store    *CertStore
	Interval time.Duration
	Log      *slog.Logger
}

func (s *Syncer) log() *slog.Logger {
	if s.Log != nil {
		return s.Log
	}
	return slog.Default()
}

// Pass runs one sync. The caBundle asserted is the CA of the material just
// loaded, which is the CA in the Secret, so the two cannot disagree.
func (s *Syncer) Pass(ctx context.Context) error {
	m, err := s.TLS.Load(ctx)
	if err != nil {
		return err
	}
	cert := m.Cert
	s.Store.Set(&cert)
	if s.Asserter != nil {
		return s.Asserter.Assert(ctx, m.CA)
	}
	return nil
}

// Run passes immediately and then every Interval until ctx ends.
func (s *Syncer) Run(ctx context.Context) {
	interval := s.Interval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if err := s.Pass(ctx); err != nil && ctx.Err() == nil {
			s.log().Warn("webhook TLS sync failed; will retry", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
