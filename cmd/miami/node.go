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
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/greghaskins/csi-miami/internal/csi"
	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/kube"
	"github.com/greghaskins/csi-miami/internal/metrics"
	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/reconcile"
)

// nodeConfig is what the node's flags set.
type nodeConfig struct {
	endpoint       string
	nodeID         string
	kubeletDir     string
	transportsRoot string
	stateDir       string
	stageRoot      string
	metricsAddr    string
	interval       time.Duration
	deadline       time.Duration
	transportIdle  time.Duration
	kubeconfig     string
	noEvents       bool
}

// withDefaults fills the paths that default to places under the kubelet
// directory.
func (c nodeConfig) withDefaults() nodeConfig {
	if c.stateDir == "" {
		c.stateDir = filepath.Join(c.kubeletDir, "plugins", csi.DriverName, "intents")
	}
	if c.transportsRoot == "" {
		c.transportsRoot = filepath.Join(c.kubeletDir, "plugins", csi.DriverName, "transports")
	}
	if c.stageRoot == "" {
		c.stageRoot = filepath.Join(c.kubeletDir, "plugins", "kubernetes.io", "csi", csi.DriverName)
	}
	return c
}

// eventRunner is the event recorder of the node: the reconciler's Events and
// the goroutine that talks to the API server.
type eventRunner interface {
	reconcile.Events
	Run(ctx context.Context)
}

// runNode parses the flags of `miami node` and serves CSI until SIGTERM or
// SIGINT, then stops gracefully. Help goes to stdout and exits 0; a flag error
// goes to stderr and exits 2; a failure to serve exits 1.
func runNode(args []string, stdout, stderr io.Writer) int {
	var out bytes.Buffer
	var cfg nodeConfig
	fs := flag.NewFlagSet("miami node", flag.ContinueOnError)
	fs.SetOutput(&out)
	fs.StringVar(&cfg.endpoint, "endpoint", "unix:///csi/csi.sock", "CSI endpoint to listen on, unix:///<absolute path>")
	fs.StringVar(&cfg.nodeID, "node-id", "", "this node's name, as Kubernetes knows it (required)")
	fs.StringVar(&cfg.kubeletDir, "kubelet-dir", "/var/lib/kubelet", "the kubelet's root directory on the host")
	fs.StringVar(&cfg.transportsRoot, "transports-root", "", "directory to mount transports under (default <kubelet-dir>/plugins/"+csi.DriverName+"/transports, where the kubelet guarantees mount propagation)")
	fs.StringVar(&cfg.stateDir, "state-dir", "", "directory of the intent records (default <kubelet-dir>/plugins/"+csi.DriverName+"/intents)")
	fs.StringVar(&cfg.stageRoot, "stage-root", "", "the driver's staging directory (default <kubelet-dir>/plugins/kubernetes.io/csi/"+csi.DriverName+")")
	fs.StringVar(&cfg.metricsAddr, "metrics-addr", ":9810", "address to serve /metrics and /healthz on; empty turns it off")
	fs.DurationVar(&cfg.interval, "reconcile-interval", reconcile.DefaultInterval, "period of the converge pass that runs when nothing asks for one")
	fs.DurationVar(&cfg.deadline, "deadline", 10*time.Second, "deadline of one mount operation, one stat of a transport and one API request")
	fs.DurationVar(&cfg.transportIdle, "transport-idle", 0, "unmount a transport that no volume uses after this long; 0 means never")
	fs.StringVar(&cfg.kubeconfig, "kubeconfig", "", "kubeconfig file, for development outside a cluster; in a cluster the service account is used")
	fs.BoolVar(&cfg.noEvents, "no-events", false, "do not record Kubernetes events (for tests, where there is no API server)")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			_, _ = stdout.Write(out.Bytes())
			return 0
		}
		_, _ = stderr.Write(out.Bytes())
		return 2
	}
	if fs.NArg() > 0 {
		_, _ = fmt.Fprintf(stderr, "miami node: unexpected argument %q\n", fs.Arg(0))
		return 2
	}
	for _, p := range []struct {
		name string
		bad  bool
	}{
		{"-node-id is required", cfg.nodeID == ""},
		{"-deadline must be positive", cfg.deadline <= 0},
		{"-reconcile-interval must be positive", cfg.interval <= 0},
		{"-transport-idle must not be negative", cfg.transportIdle < 0},
		{"-kubelet-dir must be an absolute path", !filepath.IsAbs(cfg.kubeletDir)},
	} {
		if p.bad {
			_, _ = fmt.Fprintf(stderr, "miami node: %s\n", p.name)
			return 2
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	log := slog.New(slog.NewTextHandler(stderr, nil))
	if err := serveNode(ctx, cfg.withDefaults(), log); err != nil {
		_, _ = fmt.Fprintf(stderr, "miami node: %v\n", err)
		return 1
	}
	return 0
}

// serveNode builds the event recorder (unless events are off) and serves until
// ctx ends.
func serveNode(ctx context.Context, cfg nodeConfig, log *slog.Logger) error {
	var events eventRunner
	if !cfg.noEvents {
		restCfg, err := kube.RestConfig(cfg.kubeconfig)
		if err != nil {
			return err
		}
		rec, err := kube.NewEventRecorder(restCfg, kube.RecorderOptions{
			Component: csi.DriverName, Node: cfg.nodeID, Deadline: cfg.deadline, Log: log,
		})
		if err != nil {
			return err
		}
		events = rec
	}
	return serveNodeWith(ctx, cfg, mount.New(cfg.deadline), events, log, nil)
}

// serveNodeWith serves with the given mounter and event recorder (nil for
// none); tests replace them. ready, if not nil, is called with the address the
// metrics server listens on once everything is listening.
//
// On ctx ending it stops in the order that loses nothing: the CSI server stops
// accepting and finishes the calls in flight, which are waiting on the
// reconciler; then the reconciler is cancelled; then the event recorder and
// the metrics server, which the last calls were still reporting to.
func serveNodeWith(ctx context.Context, cfg nodeConfig, m mount.Mounter, events eventRunner, log *slog.Logger, ready func(metricsAddr string)) error {
	tRoot, err := resolveRoot(cfg.transportsRoot)
	if err != nil {
		return err
	}
	sRoot, err := resolveRoot(cfg.stageRoot)
	if err != nil {
		return fmt.Errorf("stage root: %w", err)
	}
	store, err := intent.Open(cfg.stateDir)
	if err != nil {
		return err
	}

	reg := metrics.New(metrics.WithReconciler())
	opts := reconcile.Options{
		TransportsRoot: tRoot,
		StageRoot:      sRoot,
		Interval:       cfg.interval,
		StatTimeout:    cfg.deadline,
		TransportIdle:  cfg.transportIdle,
		Logger:         log,
		Metrics:        reg,
	}
	if events != nil {
		opts.Events = events
	}
	rec, err := reconcile.New(store, m, opts)
	if err != nil {
		return err
	}
	node, err := csi.NewNode(csi.NodeOptions{
		NodeID: cfg.nodeID, Reconciler: csi.NodeBackend{Reconciler: rec, Store: store},
		Mounter: m, Deadline: cfg.deadline, Log: log,
	})
	if err != nil {
		return err
	}
	srv, err := csi.NewServer(csi.ServerOptions{
		Endpoint:     cfg.endpoint,
		Identity:     csi.NewIdentity(version),
		Node:         node,
		Interceptors: []grpc.UnaryServerInterceptor{reg.MetricsInterceptor()},
		Log:          log,
	})
	if err != nil {
		return err
	}
	if err := srv.Listen(); err != nil {
		return err
	}
	metricsLis, err := listenMetrics(cfg.metricsAddr)
	if err != nil {
		return err
	}

	// Each part has a context of its own, cancelled in the order above.
	// start runs f and returns the function that stops it: cancel its context
	// and wait for f to return, so the order below is the order that happens.
	start := func(f func(context.Context)) (stop func()) {
		c, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			f(c)
		}()
		return func() { cancel(); <-done }
	}
	srvCtx, srvCancel := context.WithCancel(ctx)
	defer srvCancel()
	var fatal error
	var fatalOnce sync.Once
	fail := func(err error) {
		fatalOnce.Do(func() { fatal = err })
		srvCancel()
	}

	stopRec := start(func(c context.Context) {
		if err := rec.Run(c); err != nil {
			fail(fmt.Errorf("reconciler: %w", err))
		}
	})
	stopEvents := func() {}
	if events != nil {
		stopEvents = start(events.Run)
	}
	stopMetrics := func() {}
	if metricsLis != nil {
		stopMetrics = start(func(c context.Context) {
			if err := reg.Serve(c, metricsLis); err != nil {
				fail(fmt.Errorf("metrics server: %w", err))
			}
		})
	}

	log.Info("node starting", "version", version, "endpoint", cfg.endpoint, "node", cfg.nodeID)
	if ready != nil {
		addr := ""
		if metricsLis != nil {
			addr = metricsLis.Addr().String()
		}
		ready(addr)
	}
	serveErr := srv.Serve(srvCtx)

	stopRec()
	stopEvents()
	stopMetrics()
	if serveErr != nil {
		return serveErr
	}
	return fatal
}

// listenMetrics listens on addr for the metrics server, or returns nil if addr
// is empty.
func listenMetrics(addr string) (net.Listener, error) {
	if addr == "" {
		return nil, nil
	}
	lis, err := net.Listen("tcp4", addr)
	if err != nil {
		return nil, fmt.Errorf("metrics address: %w", err)
	}
	return lis, nil
}
