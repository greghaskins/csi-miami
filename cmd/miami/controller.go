package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/greghaskins/csi-miami/internal/csi"
	"github.com/greghaskins/csi-miami/internal/kube"
	"github.com/greghaskins/csi-miami/internal/metrics"
	"github.com/greghaskins/csi-miami/internal/mount"
)

// controllerConfig is what the controller's flags set.
type controllerConfig struct {
	endpoint   string
	root       string
	kubeconfig string
	deadline   time.Duration

	// metricsAddr is where /metrics and /healthz are served; empty means off.
	metricsAddr string
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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	log := slog.New(slog.NewTextHandler(stderr, nil))
	if err := serveController(ctx, cfg, log); err != nil {
		_, _ = fmt.Fprintf(stderr, "miami controller: %v\n", err)
		return 1
	}
	return 0
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
	return serveControllerWith(ctx, cfg, pvs, mount.New(cfg.deadline), log)
}

// serveControllerWith serves with the given PV reader and mounter; tests
// replace them.
func serveControllerWith(ctx context.Context, cfg controllerConfig, pvs kube.PVReader, m mount.Mounter, log *slog.Logger) error {
	root, err := resolveRoot(cfg.root)
	if err != nil {
		return err
	}
	ctrl, err := csi.NewController(csi.ControllerOptions{Root: root, Mounter: m, PVs: pvs, Log: log, Deadline: cfg.deadline})
	if err != nil {
		return err
	}
	reg := metrics.New()
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
	// The metrics server outlives the CSI server's graceful stop, so the last
	// calls are still reported to it.
	mctx, stopMetrics := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var metricsErr error
	if lis != nil {
		wg.Go(func() { metricsErr = reg.Serve(mctx, lis) })
	}
	log.Info("controller starting", "version", version, "endpoint", cfg.endpoint)
	err = srv.Serve(ctx)
	stopMetrics()
	wg.Wait()
	if err != nil {
		return err
	}
	if metricsErr != nil {
		return fmt.Errorf("metrics server: %w", metricsErr)
	}
	return nil
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
