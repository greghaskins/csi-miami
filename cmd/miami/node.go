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
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/greghaskins/csi-miami/internal/cachecap"
	"github.com/greghaskins/csi-miami/internal/csi"
	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/kube"
	"github.com/greghaskins/csi-miami/internal/metrics"
	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/prereq"
	"github.com/greghaskins/csi-miami/internal/reconcile"
	volkinds "github.com/greghaskins/csi-miami/internal/volume"
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

	// For the volume kind.
	cacheDir      string
	binDir        string
	systemdSocket string
	sharedDirs    stringList

	// The node's cache budget: the flags as given, and what they parse to
	// (cacheConfig; the zero value, accounting off, in tests that build a
	// nodeConfig directly).
	cacheAccounting bool
	cacheShare      float64
	cacheReserve    string
	cacheBudget     string
	cache           cachecap.Config

	// The defaults of the generation timers, for a volume whose class sets none.
	genInterval, genMaxAge, genTimeout time.Duration
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
	if c.cacheDir == "" {
		c.cacheDir = filepath.Join(c.kubeletDir, "plugins", csi.DriverName, "cache")
	}
	if c.binDir == "" {
		c.binDir = filepath.Join(c.kubeletDir, "plugins", csi.DriverName, "bin")
	}
	return c
}

// cacheConfig parses the cache budget flags. An empty -cache-budget means the
// formula; "0" is an explicit zero budget.
func (c nodeConfig) cacheConfig() (cachecap.Config, error) {
	if !(c.cacheShare > 0 && c.cacheShare <= 1) {
		return cachecap.Config{}, errors.New("-cache-budget-share must be above 0 and at most 1")
	}
	out := cachecap.Config{Accounting: c.cacheAccounting, Share: c.cacheShare}
	q, err := resource.ParseQuantity(c.cacheReserve)
	if err != nil || q.Sign() < 0 {
		return cachecap.Config{}, errors.New("-cache-budget-reserve must be a quantity of zero or more, such as 15Gi")
	}
	out.Reserve = q.Value()
	if c.cacheBudget != "" {
		q, err := resource.ParseQuantity(c.cacheBudget)
		if err != nil || q.Sign() < 0 {
			return cachecap.Config{}, errors.New("-cache-budget must be a quantity of zero or more, such as 100Gi")
		}
		out.Absolute, out.HaveAbsolute = q.Value(), true
	}
	return out, nil
}

// noopNodeCapacity is the NodeCapacity of a node that does not yet patch its
// status: it reports the key absent and changes nothing.
type noopNodeCapacity struct{}

func (noopNodeCapacity) CacheCapacity(context.Context, string) (int64, bool, error) {
	return 0, false, nil
}
func (noopNodeCapacity) SetCacheCapacity(context.Context, string, int64) error { return nil }
func (noopNodeCapacity) RemoveCacheCapacity(context.Context, string) error     { return nil }

// eventRunner is the event recorder of the node: the reconciler's Events and
// the goroutine that talks to the API server.
type eventRunner interface {
	reconcile.Events
	Run(ctx context.Context)
}

// nodeFlags defines the flags of `miami node` on fs, writing into cfg.
func nodeFlags(fs *flag.FlagSet, cfg *nodeConfig) {
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
	fs.StringVar(&cfg.cacheDir, "cache-dir", "", "directory the volume caches live under on the host, writable and seen at the same path by the container and the host (default <kubelet-dir>/plugins/"+csi.DriverName+"/cache)")
	fs.StringVar(&cfg.binDir, "bin-dir", "", "directory the per-hash copies of the miami binary are kept in (default <kubelet-dir>/plugins/"+csi.DriverName+"/bin)")
	fs.StringVar(&cfg.systemdSocket, "systemd-socket", "/run/systemd/private", "systemd's private socket, used to run each volume's block server as a unit")
	fs.Var(&cfg.sharedDirs, "shared-dir", "a directory the node container sees at the same absolute path as the host, besides -kubelet-dir; -cache-dir and -bin-dir must lie under one (repeatable)")
	fs.BoolVar(&cfg.cacheAccounting, "cache-accounting", true, "advertise this node's cache budget to the scheduler and refuse a new volume that does not fit; false removes the advertisement and skips the check")
	fs.Float64Var(&cfg.cacheShare, "cache-budget-share", 0.5, "the share of the cache filesystem offered as budget, above 0 and at most 1")
	fs.StringVar(&cfg.cacheReserve, "cache-budget-reserve", "15Gi", "space kept back from the share, a quantity such as 15Gi")
	fs.StringVar(&cfg.cacheBudget, "cache-budget", "", "an absolute budget, a quantity such as 100Gi (0 is a budget of zero); empty uses -cache-budget-share and -cache-budget-reserve")
	fs.DurationVar(&cfg.genInterval, "generation-interval", volkinds.DefaultGenerationEvery, "how often a volume with unpublished writes publishes a generation, unless its class sets generations.interval")
	fs.DurationVar(&cfg.genMaxAge, "generation-max-age", volkinds.DefaultGenerationMaxAge, "how old the newest generation of a volume with unpublished writes may get before the volume is reported overdue, unless its class sets generations.maxAge")
	fs.DurationVar(&cfg.genTimeout, "generation-timeout", reconcile.DefaultGenerationTimeout, "deadline of one generation, freeze to upload")
}

// runNode parses the flags of `miami node` and serves CSI until SIGTERM or
// SIGINT, then stops gracefully. Help goes to stdout and exits 0; a flag error
// goes to stderr and exits 2; a failure to serve exits 1.
func runNode(args []string, stdout, stderr io.Writer) int {
	var out bytes.Buffer
	var cfg nodeConfig
	fs := flag.NewFlagSet("miami node", flag.ContinueOnError)
	fs.SetOutput(&out)
	nodeFlags(fs, &cfg)

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
	cache, cacheErr := cfg.cacheConfig()
	if cacheErr != nil {
		_, _ = fmt.Fprintf(stderr, "miami node: %v\n", cacheErr)
		return 2
	}
	cfg.cache = cache
	for _, p := range []struct {
		name string
		bad  bool
	}{
		{"-node-id is required", cfg.nodeID == ""},
		{"-deadline must be positive", cfg.deadline <= 0},
		{"-reconcile-interval must be positive", cfg.interval <= 0},
		{"-transport-idle must not be negative", cfg.transportIdle < 0},
		{"-kubelet-dir must be an absolute path", !filepath.IsAbs(cfg.kubeletDir)},
		{"-cache-dir must be an absolute path", cfg.cacheDir != "" && !filepath.IsAbs(cfg.cacheDir)},
		{"-bin-dir must be an absolute path", cfg.binDir != "" && !filepath.IsAbs(cfg.binDir)},
		{"-systemd-socket must be an absolute path", !filepath.IsAbs(cfg.systemdSocket)},
		{"-shared-dir must be an absolute path", slices.ContainsFunc(cfg.sharedDirs, func(d string) bool { return !filepath.IsAbs(d) })},
		{"-generation-interval must be at least " + volkinds.MinGenerationInterval.String(), cfg.genInterval < volkinds.MinGenerationInterval},
		{"-generation-max-age must not be under -generation-interval", cfg.genMaxAge < cfg.genInterval},
		{"-generation-timeout must be positive", cfg.genTimeout <= 0},
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
	var cluster clusterAPI
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
		kc, err := kube.NewClient(restCfg, cfg.deadline)
		if err != nil {
			return err
		}
		cluster = kc
	}
	return serveNodeWith(ctx, cfg, mount.New(cfg.deadline), events, cluster, log, nil)
}

// serveNodeWith serves with the given mounter, event recorder and cluster
// reads (nil for none); tests replace them. ready, if not nil, is called with the address the
// metrics server listens on once everything is listening.
//
// On ctx ending it stops in the order that loses nothing: the CSI server stops
// accepting and finishes the calls in flight, which are waiting on the
// reconciler; then the reconciler and the cache advertisement loop are cancelled; then the event recorder and
// the metrics server, which the last calls were still reporting to.
func serveNodeWith(ctx context.Context, cfg nodeConfig, m mount.Mounter, events eventRunner, cluster clusterAPI, log *slog.Logger, ready func(metricsAddr string)) error {
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
	defer func() { _ = store.Close() }()

	mopts := []metrics.Option{metrics.WithReconciler()}
	if cfg.cache.Accounting {
		mopts = append(mopts, metrics.WithCacheCapacity())
	}
	reg := metrics.New(mopts...)
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
	opts.CacheDir, opts.BinDir, opts.SystemdSocket = cfg.cacheDir, cfg.binDir, cfg.systemdSocket
	// The prerequisites of the volume kind are probed now and on every pass;
	// a node without them still serves directory volumes.
	probe := newProber(prereq.Options{SystemdSocket: cfg.systemdSocket, CacheDir: cfg.cacheDir}, cacheLayoutErr(cfg), reg, log)
	opts.Prereq = probe
	probe(ctx)
	deps := realDeps(cfg)
	defer func() { _ = deps.Systemd.(io.Closer).Close() }()
	// One Source for the stage check and the advertisement loop, so they never
	// disagree about the budget.
	cacheSrc := cachecap.NewSource(cfg.cache, cfg.cacheDir, cachecap.RealFSStat)
	volItem, err := wireItems(&opts, cfg, tRoot, deps, m, events, cluster, cacheSrc, log)
	if err != nil {
		return err
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
	// The advertisement loop has a goroutine of its own, so a slow API server
	// cannot stall the reconciler. Until the node patches its status for real,
	// it talks to a NodeCapacity that does nothing. The committed figure is the
	// volume item's own sum, the one its stage check uses.
	cacheRunner := cachecap.NewRunner(cacheSrc, noopNodeCapacity{}, cfg.nodeID, volItem.Committed, reg, log, cfg.interval)
	stopCache := start(cacheRunner.Run)
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
	stopCache()
	stopEvents()
	stopMetrics()
	if serveErr != nil {
		return serveErr
	}
	return fatal
}

// newProber returns the probe of the node's prerequisites: it runs
// prereq.Probe, counts a layout the host cannot share (layout, see
// cacheLayoutErr) as a missing cache dir, sets the capability gauges and logs when the set of missing
// features changes (not on every pass). It is safe for concurrent use.
func newProber(o prereq.Options, layout error, reg *metrics.Registry, log *slog.Logger) func(context.Context) prereq.Report {
	var mu sync.Mutex
	var last string
	first := true
	return func(ctx context.Context) prereq.Report {
		rep := prereq.Probe(ctx, o)
		if layout != nil {
			for i := range rep.Features {
				if rep.Features[i].Name == prereq.FeatureCacheDir {
					rep.Features[i].Err = errors.Join(layout, rep.Features[i].Err)
				}
			}
		}
		for _, f := range rep.Features {
			reg.SetCapability(f.Name, f.Err == nil)
		}
		missing := strings.Join(rep.Missing(), ",")
		mu.Lock()
		changed := first || missing != last
		first, last = false, missing
		mu.Unlock()
		switch {
		case !changed:
		case missing == "":
			log.Info("node prerequisites for volumes are all present")
		default:
			log.Warn("node prerequisites for volumes are missing; volume-kind volumes will not stage here", "missing", missing, "error", rep.Err())
		}
		return rep
	}
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
