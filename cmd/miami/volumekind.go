package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/greghaskins/csi-miami/internal/cachecap"
	"github.com/greghaskins/csi-miami/internal/holder"
	"github.com/greghaskins/csi-miami/internal/item"
	"github.com/greghaskins/csi-miami/internal/item/directory"
	volitem "github.com/greghaskins/csi-miami/internal/item/volume"
	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/nbd"
	"github.com/greghaskins/csi-miami/internal/reconcile"
	"github.com/greghaskins/csi-miami/internal/systemd"
	"github.com/greghaskins/csi-miami/internal/tool"
)

// clusterAPI is what the node reads from the API server: the judge's two reads,
// the PV annotation of the generation timers and the drain's proof of a deleted
// volume, and the node's own cache capacity patch. *kube.Client implements it. It is nil only when the node runs without
// a cluster (-no-events, for tests); the volume kind then cannot take a volume
// over, cannot be asked for a generation and keeps every surviving cache.
type clusterAPI interface {
	reconcile.Cluster
	PVExists(ctx context.Context, name string) (bool, error)
	// The node's own advertisement of its cache capacity (decision 66).
	cachecap.NodeCapacity
}

// nodeDeps are the clients the volume kind runs on. The node builds the real
// ones (realDeps); tests build the same wiring over fakes.
type nodeDeps struct {
	Tools   tool.Runner
	Systemd systemd.Manager
	NBD     nbd.Devicer
	Holder  holder.Acquirer
	Dial    func(ctx context.Context, socket string) (volitem.Control, error)
}

// realDeps returns the production clients. The systemd manager dials on first
// use, so a node whose systemd socket is absent still starts and serves the
// directory kind; the volume kind refuses with prerequisite-missing before it
// touches the manager.
func realDeps(cfg nodeConfig) nodeDeps {
	return nodeDeps{
		Tools:   tool.Real(),
		Systemd: &lazySystemd{socket: cfg.systemdSocket, open: systemd.New},
		NBD:     nbd.Real(volitem.DefaultSysfs),
		Holder:  holder.Real{},
		Dial:    volitem.DialBlockd,
	}
}

// volumeConfig is the volume item's Config, built in one place for the node and
// for the test that checks nothing the kind needs is left unset.
//
// Left zero on purpose: DevDir, Exe, Current, Freeze and Thaw (the defaults are
// the real thing), StartTimeout, PollEvery, FreezeBound and PublishTimeout (the
// item's defaults are the spec's bounds), and Event, Judge, Takeover and
// VolumeExists when there is no cluster or no event recorder.
func volumeConfig(cfg nodeConfig, root string, d nodeDeps, m mount.Mounter, events eventRunner, cluster clusterAPI, src *cachecap.Source, log *slog.Logger) volitem.Config {
	c := volitem.Config{
		Root: root, BinDir: cfg.binDir, CacheDir: cfg.cacheDir, Sysfs: volitem.DefaultSysfs,
		Tools: d.Tools, Systemd: d.Systemd, NBD: d.NBD, Holder: d.Holder, Mounter: m, Dial: d.Dial,
		Node: cfg.nodeID, BinaryHash: version, Timeout: cfg.deadline, Logger: log,
	}
	if src != nil {
		c.CacheBudget = src.Current
	}
	if events != nil {
		c.Event = events.Event
	}
	if cluster != nil {
		var ev reconcile.Events
		if events != nil {
			ev = events
		}
		j := reconcile.NewEmergencyJudge(reconcile.JudgeOptions{Cluster: cluster, Events: ev, Log: log})
		c.Judge, c.Takeover = j.Judge, j.TookOver
		c.VolumeExists = cluster.PVExists
	}
	return c
}

// wireItems sets the reconciler's items (the directory kind and the volume
// kind) and the options the volume kind's timers read. It returns the volume
// item, whose Committed the advertisement loop reports.
func wireItems(opts *reconcile.Options, cfg nodeConfig, root string, d nodeDeps, m mount.Mounter, events eventRunner, cluster clusterAPI, src *cachecap.Source, log *slog.Logger) (*volitem.Item, error) {
	dir, err := directory.New(directory.Config{Root: root, Mounter: m, Timeout: opts.StatTimeout})
	if err != nil {
		return nil, err
	}
	vol, err := volitem.New(volumeConfig(cfg, root, d, m, events, cluster, src, log))
	if err != nil {
		return nil, err
	}
	opts.Items = []item.Item{dir, vol}
	opts.Probe = &reconcile.ProbeOptions{}
	opts.Generations = reconcile.GenerationOptions{Interval: cfg.genInterval, MaxAge: cfg.genMaxAge, Timeout: cfg.genTimeout}
	if cluster != nil {
		opts.PVAnnotation = cluster.PVAnnotation
	}
	return vol, nil
}

// lazySystemd is a systemd.Manager that connects on the first call that finds
// no connection, and again after a failed attempt.
type lazySystemd struct {
	socket string
	open   func(socket string) (systemd.Manager, error)

	mu sync.Mutex
	m  systemd.Manager
}

func (l *lazySystemd) get() (systemd.Manager, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.m == nil {
		m, err := l.open(l.socket)
		if err != nil {
			return nil, err
		}
		l.m = m
	}
	return l.m, nil
}

func (l *lazySystemd) Start(ctx context.Context, u systemd.Unit) error {
	m, err := l.get()
	if err != nil {
		return err
	}
	return m.Start(ctx, u)
}

func (l *lazySystemd) Stop(ctx context.Context, name string) error {
	m, err := l.get()
	if err != nil {
		return err
	}
	return m.Stop(ctx, name)
}

func (l *lazySystemd) State(ctx context.Context, name string) (systemd.State, error) {
	m, err := l.get()
	if err != nil {
		return systemd.State{}, err
	}
	return m.State(ctx, name)
}

// Close closes the connection, if one was made.
func (l *lazySystemd) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if c, ok := l.m.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// maxSocketPath is the longest path a unix socket can have: sun_path is 108
// bytes, one of them the terminating NUL.
const maxSocketPath = 107

// stringList is a repeatable string flag.
type stringList []string

func (l *stringList) String() string     { return strings.Join(*l, ",") }
func (l *stringList) Set(v string) error { *l = append(*l, v); return nil }

// cacheLayoutErr says why the volume kind cannot work with the configured
// cache and bin directories, or returns nil. The plugin in its container and
// the block server on the host must see them at the same absolute path, so
// each must lie under the kubelet dir (the one directory the DaemonSet shares
// with the host) or under a -shared-dir; and the control socket in the cache
// dir must fit the unix socket path limit. Either is a node that looks healthy
// and never finds its servers, so the node reports the cache dir missing
// instead.
func cacheLayoutErr(cfg nodeConfig) error {
	shared := append([]string{cfg.kubeletDir}, cfg.sharedDirs...)
	for _, d := range []struct{ flag, path string }{{"-cache-dir", cfg.cacheDir}, {"-bin-dir", cfg.binDir}} {
		if !slices.ContainsFunc(shared, func(s string) bool { return pathWithin(s, d.path) }) {
			return fmt.Errorf("%s %s is under neither -kubelet-dir nor a -shared-dir, so the host and the container may see different directories", d.flag, d.path)
		}
	}
	if n := volitem.SocketPathLen(cfg.cacheDir); n > maxSocketPath {
		return fmt.Errorf("-cache-dir %s makes a control socket path of %d bytes, over the %d-byte limit; use a shorter -cache-dir", cfg.cacheDir, n, maxSocketPath)
	}
	return nil
}

// pathWithin reports whether path is dir or lies below it; both are cleaned.
func pathWithin(dir, path string) bool {
	rel, err := filepath.Rel(filepath.Clean(dir), filepath.Clean(path))
	return err == nil && rel != ".." && !strings.HasPrefix(rel, "../")
}
