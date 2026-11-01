//go:build e2e

package e2e

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// The environment hack/e2e.sh sets. All of it is required except the NFS
// values, which are required only for the nfs3 transport.
const (
	envKubeconfig   = "KUBECONFIG"
	envNamespace    = "MIAMI_E2E_NAMESPACE"
	envTransport    = "MIAMI_E2E_TRANSPORT"
	envStorageClass = "MIAMI_E2E_STORAGE_CLASS"
	envCluster      = "MIAMI_E2E_KIND_CLUSTER"
	envArtifactDir  = "MIAMI_E2E_ARTIFACT_DIR"
	envNFSServer    = "MIAMI_E2E_NFS_SERVER"
	envNFSContainer = "MIAMI_E2E_NFS_CONTAINER"
	envTimeout      = "MIAMI_E2E_TIMEOUT"
)

// defaultTimeout bounds each wait unless -e2e.timeout or MIAMI_E2E_TIMEOUT
// says otherwise.
const defaultTimeout = 3 * time.Minute

// pollEvery is the interval of every poll.
const pollEvery = 2 * time.Second

var timeoutFlag = flag.Duration("e2e.timeout", 0, "deadline of each wait (default "+defaultTimeout.String()+", or "+envTimeout+")")

// Transport kinds, as the StorageClasses name them.
const (
	transportLocal = "local"
	transportNFS3  = "nfs3"
)

// env is what the run is for: the cluster, the driver, the transport under
// test and where to put artifacts.
type env struct {
	Kubeconfig   string
	DriverNS     string
	Transport    string // transportLocal or transportNFS3
	StorageClass string
	Cluster      string
	ArtifactDir  string
	NFSServer    string // nfs3 only
	NFSContainer string // nfs3 only: the NFS server container's name
	Timeout      time.Duration

	Config *rest.Config
	Client kubernetes.Interface
}

var (
	envOnce sync.Once
	envVal  *env
	envErr  error
)

// loadEnv reads the environment once. A missing or malformed value fails the
// calling test with every problem named.
func loadEnv(t *testing.T) *env {
	t.Helper()
	envOnce.Do(func() { envVal, envErr = readEnv() })
	if envErr != nil {
		t.Fatalf("the e2e environment is incomplete: %v", envErr)
	}
	return envVal
}

func readEnv() (*env, error) {
	var problems []string
	need := func(name string) string {
		v := os.Getenv(name)
		if v == "" {
			problems = append(problems, name+" is not set")
		}
		return v
	}
	e := &env{
		Kubeconfig:   need(envKubeconfig),
		DriverNS:     need(envNamespace),
		Transport:    need(envTransport),
		StorageClass: need(envStorageClass),
		Cluster:      need(envCluster),
		ArtifactDir:  need(envArtifactDir),
		Timeout:      defaultTimeout,
	}
	switch e.Transport {
	case transportNFS3:
		e.NFSServer = need(envNFSServer)
		e.NFSContainer = need(envNFSContainer)
	case transportLocal, "":
	default:
		problems = append(problems, fmt.Sprintf("%s is %q, want %s or %s", envTransport, e.Transport, transportLocal, transportNFS3))
	}
	if v := os.Getenv(envTimeout); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			problems = append(problems, fmt.Sprintf("%s is %q, want a positive duration", envTimeout, v))
		} else {
			e.Timeout = d
		}
	}
	if *timeoutFlag > 0 {
		e.Timeout = *timeoutFlag
	}
	if len(problems) > 0 {
		return nil, fmt.Errorf("%s", strings.Join(problems, "; "))
	}

	cfg, err := clientcmd.BuildConfigFromFlags("", e.Kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", envKubeconfig, err)
	}
	cfg.QPS, cfg.Burst = 50, 100
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	e.Config, e.Client = cfg, client
	if err := os.MkdirAll(e.ArtifactDir, 0o755); err != nil { //nolint:gosec // the runner user uploads what root wrote
		return nil, err
	}
	return e, nil
}

// runVariant runs fn as the subtest named variant when the run is for that
// transport. A criterion that does not apply to the transport of the run
// runs nothing and says so; it is not skipped, because the script requires
// the variants it expects to have passed.
func runVariant(t *testing.T, e *env, variant string, fn func(t *testing.T)) {
	t.Helper()
	if e.Transport != variant {
		t.Logf("the %s variant is not for this run, which is for %s", variant, e.Transport)
		return
	}
	t.Run(variant, fn)
}

// poll calls check every pollEvery until it reports done or the deadline
// passes. check returns what it saw (kept for the failure message), whether
// the condition holds, and an error for a transient failure, which is kept
// as the last observation and retried.
func poll(timeout time.Duration, check func(ctx context.Context) (seen string, done bool, err error)) (last string, ok bool) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	tick := time.NewTicker(pollEvery)
	defer tick.Stop()
	var lastSeen, lastErr string
	report := func() string {
		switch {
		case lastSeen == "" && lastErr == "":
			return "nothing"
		case lastErr == "":
			return lastSeen
		case lastSeen == "":
			return "error: " + lastErr
		}
		return lastSeen + " (and a later error: " + lastErr + ")"
	}
	for {
		seen, done, err := check(ctx)
		if err == nil {
			// An observation made while the deadline passed is not news.
			lastSeen, lastErr = seen, ""
			if done {
				return report(), true
			}
		} else if ctx.Err() == nil {
			lastErr = err.Error()
		}
		select {
		case <-ctx.Done():
			return report(), false
		case <-tick.C:
		}
	}
}

// waitFor fails the test, naming what it waited for and what it last saw, if
// check does not report done within the deadline.
func (e *env) waitFor(t *testing.T, what string, check func(ctx context.Context) (string, bool, error)) {
	t.Helper()
	if last, ok := poll(e.Timeout, check); !ok {
		t.Fatalf("timed out after %s waiting for %s; last saw: %s", e.Timeout, what, last)
	}
}
