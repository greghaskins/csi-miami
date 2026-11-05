package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log/slog"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/holder"
	"github.com/greghaskins/csi-miami/internal/intent"
	volitem "github.com/greghaskins/csi-miami/internal/item/volume"
	"github.com/greghaskins/csi-miami/internal/kube"
	"github.com/greghaskins/csi-miami/internal/metrics"
	"github.com/greghaskins/csi-miami/internal/mount/fake"
	"github.com/greghaskins/csi-miami/internal/nbd"
	"github.com/greghaskins/csi-miami/internal/prereq"
	"github.com/greghaskins/csi-miami/internal/reconcile"
	"github.com/greghaskins/csi-miami/internal/systemd"
	sdfake "github.com/greghaskins/csi-miami/internal/systemd/fake"
	toolfake "github.com/greghaskins/csi-miami/internal/tool/fake"
)

type stubCluster struct{}

func (stubCluster) PVAnnotation(context.Context, string, string) (string, error) { return "", nil }
func (stubCluster) NodeOutOfService(context.Context, string) (bool, error)       { return false, nil }
func (stubCluster) PVExists(context.Context, string) (bool, error)               { return true, nil }

var _ clusterAPI = (*kube.Client)(nil)

type stubEvents struct{ eventRunner }

func (stubEvents) Event(string, string) {}

type stubNBD struct{ nbd.Devicer }

func testDeps() nodeDeps {
	return nodeDeps{
		Tools: &toolfake.Runner{}, Systemd: &sdfake.Manager{}, NBD: stubNBD{}, Holder: holder.Real{},
		Dial: func(context.Context, string) (volitem.Control, error) { return nil, errors.New("unused") },
	}
}

func parsedConfig(t *testing.T, args ...string) nodeConfig {
	t.Helper()
	var cfg nodeConfig
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	nodeFlags(fs, &cfg)
	if err := fs.Parse(append([]string{"-node-id=n"}, args...)); err != nil {
		t.Fatal(err)
	}
	cfg.kubeletDir = "/var/lib/kubelet"
	return cfg.withDefaults()
}

// Every field of the volume item's Config is set by the production wiring,
// except the ones that are left zero on purpose.
func TestVolumeConfigSetsEveryField(t *testing.T) {
	t.Parallel()
	cfg := parsedConfig(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	c := volumeConfig(cfg, "/r", testDeps(), &fake.Mounter{}, stubEvents{}, stubCluster{}, log)

	zeroOnPurpose := map[string]bool{
		"DevDir": true, "Exe": true, "Current": true, "Freeze": true, "Thaw": true,
		"StartTimeout": true, "PollEvery": true, "FreezeBound": true, "PublishTimeout": true,
	}
	v := reflect.ValueOf(c)
	for i := range v.NumField() {
		name := v.Type().Field(i).Name
		if v.Field(i).IsZero() == zeroOnPurpose[name] {
			continue
		}
		if zeroOnPurpose[name] {
			t.Errorf("Config.%s is set but listed as zero on purpose", name)
		} else {
			t.Errorf("Config.%s is left zero", name)
		}
	}
	if c.BinDir != "/var/lib/kubelet/plugins/miami.csi.greghaskins.com/bin" || c.CacheDir != "/var/lib/kubelet/plugins/miami.csi.greghaskins.com/cache" || c.Root != "/r" || c.Node != "n" || c.Timeout != 10*time.Second {
		t.Errorf("paths or node wrong: %+v", c)
	}
	if c.Sysfs != "/sys" {
		t.Errorf("Sysfs = %q", c.Sysfs)
	}
}

// Without a cluster the seams that need one stay nil rather than panic.
func TestVolumeConfigWithoutACluster(t *testing.T) {
	t.Parallel()
	c := volumeConfig(parsedConfig(t), "/r", testDeps(), &fake.Mounter{}, nil, nil, nil)
	if c.Judge != nil || c.Takeover != nil || c.VolumeExists != nil || c.Event != nil {
		t.Errorf("seams set without a cluster or recorder: %+v", c)
	}
}

// wireItems registers both kinds and sets the options the volume kind's timers
// read.
func TestWireItems(t *testing.T) {
	t.Parallel()
	cfg := parsedConfig(t, "-generation-interval=2m", "-generation-max-age=30m", "-generation-timeout=3m")
	opts := reconcile.Options{StatTimeout: time.Second}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := wireItems(&opts, cfg, "/r", testDeps(), &fake.Mounter{}, stubEvents{}, stubCluster{}, log); err != nil {
		t.Fatal(err)
	}
	kinds := map[string]bool{}
	for _, it := range opts.Items {
		kinds[string(it.Kind())] = true
	}
	if !kinds["directory"] || !kinds["volume"] || len(kinds) != 2 {
		t.Errorf("kinds = %v", kinds)
	}
	if opts.PVAnnotation == nil {
		t.Error("PVAnnotation unset")
	}
	want := reconcile.GenerationOptions{Interval: 2 * time.Minute, MaxAge: 30 * time.Minute, Timeout: 3 * time.Minute}
	if opts.Generations != want {
		t.Errorf("Generations = %+v, want %+v", opts.Generations, want)
	}
	// The reconciler accepts what was wired.
	dir := t.TempDir()
	store, err := intent.Open(filepath.Join(dir, "intents"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	opts.TransportsRoot, opts.StageRoot = filepath.Join(dir, "t"), filepath.Join(dir, "s")
	if _, err := reconcile.New(store, &fake.Mounter{}, opts); err != nil {
		t.Errorf("reconcile.New: %v", err)
	}
}

func TestGenerationFlagDefaultsAreTheSpecs(t *testing.T) {
	t.Parallel()
	c := parsedConfig(t)
	if c.genInterval != 5*time.Minute || c.genMaxAge != time.Hour || c.genTimeout != reconcile.DefaultGenerationTimeout {
		t.Errorf("defaults: %s %s %s", c.genInterval, c.genMaxAge, c.genTimeout)
	}
}

// A node whose systemd socket is absent still builds the lazy manager, and its
// calls fail until the socket is there, then work.
func TestLazySystemdDialsOnUseAndRetries(t *testing.T) {
	t.Parallel()
	up := false
	l := &lazySystemd{socket: "/s", open: func(string) (systemd.Manager, error) {
		if !up {
			return nil, systemd.ErrNoSystemd
		}
		return &sdfake.Manager{}, nil
	}}
	if _, err := l.State(context.Background(), "u"); !errors.Is(err, systemd.ErrNoSystemd) {
		t.Fatalf("State while down = %v", err)
	}
	up = true
	if err := l.Stop(context.Background(), "u"); err != nil {
		t.Errorf("Stop after the socket appeared: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestCacheLayoutErr(t *testing.T) {
	t.Parallel()
	long := "/var/lib/rancher/k3s/agent/kubelet"
	tests := []struct {
		name string
		args []string
		kube string
		want string // substring of the error, "" for none
	}{
		{name: "the defaults share the kubelet dir", kube: "/var/lib/kubelet"},
		{name: "a cache dir elsewhere is not shared", args: []string{"-cache-dir=/var/lib/miami/cache"}, kube: "/var/lib/kubelet", want: "/var/lib/miami/cache"},
		{name: "a declared shared dir covers it", args: []string{"-cache-dir=/var/lib/miami/cache", "-shared-dir=/var/lib/miami"}, kube: "/var/lib/kubelet"},
		{name: "a sibling prefix is not under the dir", args: []string{"-cache-dir=/var/lib/kubelet2/c"}, kube: "/var/lib/kubelet", want: "/var/lib/kubelet2/c"},
		{name: "a bin dir elsewhere is not shared", args: []string{"-bin-dir=/opt/bin"}, kube: "/var/lib/kubelet", want: "/opt/bin"},
		{name: "a long kubelet dir crosses the socket limit", kube: long, want: "107"},
		{name: "a short cache dir on a long kubelet dir fits", args: []string{"-cache-dir=/var/lib/rancher/k3s/agent/kubelet/c"}, kube: long},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var cfg nodeConfig
			fs := flag.NewFlagSet("t", flag.ContinueOnError)
			fs.SetOutput(io.Discard)
			nodeFlags(fs, &cfg)
			if err := fs.Parse(append([]string{"-node-id=n", "-kubelet-dir=" + tt.kube}, tt.args...)); err != nil {
				t.Fatal(err)
			}
			err := cacheLayoutErr(cfg.withDefaults())
			switch {
			case tt.want == "" && err != nil:
				t.Errorf("unexpected error: %v", err)
			case tt.want != "" && (err == nil || !strings.Contains(err.Error(), tt.want)):
				t.Errorf("error = %v, want one naming %q", err, tt.want)
			}
		})
	}
}

// A layout the host cannot share makes the cache-dir prerequisite missing, so
// the volume kind refuses while the node runs on.
func TestProberReportsABadLayoutAsMissingCacheDir(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := metrics.New(metrics.WithReconciler())
	o := prereq.Options{SystemdSocket: filepath.Join(dir, "none"), CacheDir: filepath.Join(dir, "cache")}
	if rep := newProber(o, nil, reg, log)(context.Background()); !rep.Present(prereq.FeatureCacheDir) {
		t.Fatalf("cache dir missing without a layout error: %v", rep.Err())
	}
	rep := newProber(o, errors.New("not shared"), reg, log)(context.Background())
	if rep.Present(prereq.FeatureCacheDir) || rep.Err() == nil || !strings.Contains(rep.Err().Error(), "not shared") {
		t.Errorf("cache dir present despite a bad layout: %v", rep.Err())
	}
}
