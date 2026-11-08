package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/greghaskins/csi-miami/internal/csi"
	"github.com/greghaskins/csi-miami/internal/kube"
	"github.com/greghaskins/csi-miami/internal/metrics"
	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/webhook"
)

// Defaults of the webhook's flags.
const (
	defaultWebhookAddr       = ":9814"
	defaultWebhookSecret     = "miami-webhook-tls" //nolint:gosec // a Secret's name
	defaultWebhookConfig     = "miami-cache"
	defaultWebhookService    = "miami-webhook"
	defaultWebhookSyncPeriod = 30 * time.Second
)

// controllerConfig is what the controller's flags set.
type controllerConfig struct {
	endpoint   string
	root       string
	kubeconfig string
	deadline   time.Duration

	// metricsAddr is where /metrics and /healthz are served; empty means off.
	metricsAddr string

	// webhookAddr is where the admission webhook listens; empty turns it off
	// (and then no Secret or configuration is touched).
	webhookAddr string
	// webhookTLSSecret names a Secret someone else manages; empty means the
	// controller makes and keeps its own, named defaultWebhookSecret.
	webhookTLSSecret string
	// webhookConfig is the MutatingWebhookConfiguration whose caBundle is
	// asserted (self-signed mode only).
	webhookConfig string
	// webhookService names the Service whose DNS names the certificate carries.
	webhookService string
	// webhookSyncInterval is the period of the certificate reload and the
	// caBundle assertion.
	webhookSyncInterval time.Duration
	// namespace is the driver's namespace (POD_NAMESPACE): where the Secret
	// lives and part of the certificate's names.
	namespace string
}

// webhookBackend is what the webhook reads and writes through. serveController
// fills it from the API client; tests pass fakes. Unused when the webhook is
// off.
type webhookBackend struct {
	claims  webhook.Claims
	secrets webhook.Secrets
	configs webhook.Configs
}

// runController parses the flags of `miami controller` and serves CSI until
// SIGTERM or SIGINT, then stops gracefully. Help goes to stdout and exits 0;
// a flag error goes to stderr and exits 2; a failure to serve exits 1.
func runController(args []string, stdout, stderr io.Writer) int {
	var out bytes.Buffer
	var cfg controllerConfig
	fs := flag.NewFlagSet("miami controller", flag.ContinueOnError)
	fs.SetOutput(&out)
	fs.StringVar(&cfg.endpoint, "endpoint", "unix:///csi/csi.sock", "CSI endpoint to listen on, unix:///<absolute path>")
	fs.StringVar(&cfg.root, "transports-root", "/var/lib/miami", "directory to mount transports under; nothing in it needs to survive a restart")
	fs.StringVar(&cfg.kubeconfig, "kubeconfig", "", "kubeconfig file, for development outside a cluster; in a cluster the service account is used")
	fs.DurationVar(&cfg.deadline, "deadline", 10*time.Second, "deadline of one API request, of one mount operation and of ListVolumes")
	fs.StringVar(&cfg.metricsAddr, "metrics-addr", ":9811", "address to serve /metrics and /healthz on; empty turns it off")
	fs.StringVar(&cfg.webhookAddr, "webhook-addr", defaultWebhookAddr, "address to serve the cache admission webhook on (TLS); empty turns it off. Not 9812, the node's health port: both pods are on the host network")
	fs.StringVar(&cfg.webhookTLSSecret, "webhook-tls-secret", "", "name of a TLS Secret (tls.crt, tls.key, ca.crt) in the driver's namespace that something else manages; read-only. Empty: make and keep a self-signed one named "+defaultWebhookSecret)
	fs.StringVar(&cfg.webhookConfig, "webhook-config", defaultWebhookConfig, "MutatingWebhookConfiguration whose webhooks' caBundle is kept equal to the self-signed CA")
	fs.StringVar(&cfg.webhookService, "webhook-service", defaultWebhookService, "Service that fronts the webhook; the certificate is valid for its in-cluster names")
	fs.DurationVar(&cfg.webhookSyncInterval, "webhook-sync-interval", defaultWebhookSyncPeriod, "how often the webhook's certificate is reloaded and the caBundle asserted")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			_, _ = stdout.Write(out.Bytes())
			return 0
		}
		_, _ = stderr.Write(out.Bytes())
		return 2
	}
	if fs.NArg() > 0 {
		_, _ = fmt.Fprintf(stderr, "miami controller: unexpected argument %q\n", fs.Arg(0))
		return 2
	}
	if cfg.deadline <= 0 {
		_, _ = fmt.Fprintln(stderr, "miami controller: -deadline must be positive")
		return 2
	}

	cfg.namespace = podNamespace()
	if msg := cfg.validateWebhook(); msg != "" {
		_, _ = fmt.Fprintf(stderr, "miami controller: %s\n", msg)
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	log := slog.New(slog.NewTextHandler(stderr, nil))
	if err := serveController(ctx, cfg, log); err != nil {
		_, _ = fmt.Fprintf(stderr, "miami controller: %v\n", err)
		return 1
	}
	return 0
}

// namespaceFile is where a pod finds its own namespace without configuration.
var namespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

// podNamespace is POD_NAMESPACE, else the namespace the service account
// token was issued in, else "".
func podNamespace() string {
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns
	}
	b, err := os.ReadFile(namespaceFile)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// validateWebhook returns what is wrong with the webhook's flags, or "". A
// disabled webhook needs nothing.
func (c controllerConfig) validateWebhook() string {
	if c.webhookAddr == "" {
		return ""
	}
	if _, _, err := net.SplitHostPort(c.webhookAddr); err != nil {
		return fmt.Sprintf("-webhook-addr %q: %v", c.webhookAddr, err)
	}
	if c.webhookSyncInterval <= 0 {
		return "-webhook-sync-interval must be positive"
	}
	if c.namespace == "" {
		return "the webhook needs the pod's namespace: set POD_NAMESPACE (downward API), or use -webhook-addr= to turn the webhook off"
	}
	if c.webhookService == "" || (c.webhookTLSSecret == "" && c.webhookConfig == "") {
		return "-webhook-service and -webhook-config must not be empty"
	}
	return ""
}

// serveController connects to the API server and serves until ctx ends.
func serveController(ctx context.Context, cfg controllerConfig, log *slog.Logger) error {
	restCfg, err := kube.RestConfig(cfg.kubeconfig)
	if err != nil {
		return err
	}
	pvs, err := kube.NewClient(restCfg, cfg.deadline)
	if err != nil {
		return err
	}
	return serveControllerWith(ctx, cfg, pvs, mount.New(cfg.deadline), webhookBackend{claims: pvs, secrets: pvs, configs: pvs}, log)
}

// serveControllerWith serves with the given PV reader, mounter and webhook
// backend; tests replace them.
func serveControllerWith(ctx context.Context, cfg controllerConfig, pvs kube.PVReader, m mount.Mounter, wb webhookBackend, log *slog.Logger) error {
	root, err := resolveRoot(cfg.root)
	if err != nil {
		return err
	}
	ctrl, err := csi.NewController(csi.ControllerOptions{Root: root, Mounter: m, PVs: pvs, Log: log, Deadline: cfg.deadline})
	if err != nil {
		return err
	}
	var mopts []metrics.Option
	if cfg.webhookAddr != "" {
		mopts = append(mopts, metrics.WithWebhook())
		if cfg.webhookTLSSecret == "" {
			mopts = append(mopts, metrics.WithWebhookConfig())
		}
	}
	reg := metrics.New(mopts...)
	wh, err := newWebhookServer(cfg, wb, reg, log)
	if err != nil {
		return err
	}
	srv, err := csi.NewServer(csi.ServerOptions{
		Endpoint:     cfg.endpoint,
		Identity:     csi.NewIdentity(version),
		Controller:   ctrl,
		Interceptors: []grpc.UnaryServerInterceptor{reg.MetricsInterceptor()},
		Log:          log,
	})
	if err != nil {
		return err
	}
	if err := srv.Listen(); err != nil {
		return err
	}
	lis, err := listenMetrics(cfg.metricsAddr)
	if err != nil {
		return err
	}
	// A webhook that cannot bind its port is fatal like the metrics server.
	if wh != nil {
		if err := wh.server.Listen(); err != nil {
			if lis != nil {
				_ = lis.Close()
			}
			return fmt.Errorf("webhook address: %w", err)
		}
	}
	// The metrics server outlives the CSI server's graceful stop, so the last
	// calls are still reported to it. The webhook stops with the CSI server
	// (started last, so stopped first of the two) and before the metrics.
	mctx, stopMetrics := context.WithCancel(context.Background())
	sctx, stopServing := context.WithCancel(ctx)
	defer stopServing()
	var wg, whWG sync.WaitGroup
	var metricsErr, webhookErr error
	if lis != nil {
		wg.Go(func() { metricsErr = reg.Serve(mctx, lis) })
	}
	if wh != nil {
		wctx, stopWebhook := context.WithCancel(sctx)
		defer stopWebhook()
		whWG.Go(func() { wh.syncer.Run(wctx) })
		whWG.Go(func() {
			if webhookErr = wh.server.Run(wctx); webhookErr != nil {
				stopServing() // a webhook that died takes the controller down, like the metrics server
			}
		})
		log.Info("webhook starting", "addr", wh.server.ListenAddr().String(), "selfSigned", cfg.webhookTLSSecret == "")
	}
	log.Info("controller starting", "version", version, "endpoint", cfg.endpoint)
	err = srv.Serve(sctx)
	stopServing()
	whWG.Wait()
	stopMetrics()
	wg.Wait()
	if err != nil {
		return err
	}
	if webhookErr != nil {
		return fmt.Errorf("webhook server: %w", webhookErr)
	}
	if metricsErr != nil {
		return fmt.Errorf("metrics server: %w", metricsErr)
	}
	return nil
}

// controllerWebhook is the webhook's two long-running parts.
type controllerWebhook struct {
	server *webhook.Server
	syncer *webhook.Syncer
}

// newWebhookServer builds the webhook, or returns nil when it is off. It
// makes no API call: the Secret is first read by the sync loop.
func newWebhookServer(cfg controllerConfig, wb webhookBackend, reg *metrics.Registry, log *slog.Logger) (*controllerWebhook, error) {
	if cfg.webhookAddr == "" {
		return nil, nil
	}
	mut, err := webhook.NewMutator(wb.claims, reg, log)
	if err != nil {
		return nil, err
	}
	managed := cfg.webhookTLSSecret != ""
	name := cfg.webhookTLSSecret
	if !managed {
		name = defaultWebhookSecret
	}
	store := &webhook.CertStore{}
	syncer := &webhook.Syncer{
		TLS: &webhook.TLSSource{
			Secrets: wb.secrets, Namespace: cfg.namespace, Name: name, Service: cfg.webhookService,
			Managed: managed, Log: log,
		},
		Store: store, Interval: cfg.webhookSyncInterval, Log: log,
	}
	if !managed {
		// With a Secret someone else manages, the configuration is theirs too
		// (a CA injector fills the bundle): this controller writes neither.
		syncer.Asserter = &webhook.CABundleAsserter{Configs: wb.configs, Name: cfg.webhookConfig, Gauge: reg, Log: log}
	}
	return &controllerWebhook{
		server: &webhook.Server{Addr: cfg.webhookAddr, Mutator: mut, GetCertificate: store.GetCertificate, Log: log},
		syncer: syncer,
	}, nil
}

// resolveRoot makes the transports root exist and returns it as an absolute,
// cleaned path with no symlink in it, which is what mounting needs.
func resolveRoot(root string) (string, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(abs, 0o700); err != nil {
		return "", fmt.Errorf("transports root: %w", err)
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return "", fmt.Errorf("transports root: %w", err)
	}
	return resolved, nil
}
