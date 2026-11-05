package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log/slog"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/holder"
	"github.com/greghaskins/csi-miami/internal/intent"
	volitem "github.com/greghaskins/csi-miami/internal/item/volume"
	"github.com/greghaskins/csi-miami/internal/kube"
	"github.com/greghaskins/csi-miami/internal/mount/fake"
	"github.com/greghaskins/csi-miami/internal/nbd"
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
	if c.BinDir != "/var/lib/kubelet/plugins/miami.csi.greghaskins.com/bin" || c.CacheDir != "/var/lib/miami/cache" || c.Root != "/r" || c.Node != "n" || c.Timeout != 10*time.Second {
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
