package main

import (
	"context"
	"io"
	"log/slog"
	"sync"

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
// volume. *kube.Client implements it. It is nil only when the node runs without
// a cluster (-no-events, for tests); the volume kind then cannot take a volume
// over, cannot be asked for a generation and keeps every surviving cache.
type clusterAPI interface {
	reconcile.Cluster
	PVExists(ctx context.Context, name string) (bool, error)
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
func volumeConfig(cfg nodeConfig, root string, d nodeDeps, m mount.Mounter, events eventRunner, cluster clusterAPI, log *slog.Logger) volitem.Config {
	c := volitem.Config{
		Root: root, BinDir: cfg.binDir, CacheDir: cfg.cacheDir, Sysfs: volitem.DefaultSysfs,
		Tools: d.Tools, Systemd: d.Systemd, NBD: d.NBD, Holder: d.Holder, Mounter: m, Dial: d.Dial,
		Node: cfg.nodeID, BinaryHash: version, Timeout: cfg.deadline, Logger: log,
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
// kind) and the options the volume kind's timers read.
func wireItems(opts *reconcile.Options, cfg nodeConfig, root string, d nodeDeps, m mount.Mounter, events eventRunner, cluster clusterAPI, log *slog.Logger) error {
	dir, err := directory.New(directory.Config{Root: root, Mounter: m, Timeout: opts.StatTimeout})
	if err != nil {
		return err
	}
	vol, err := volitem.New(volumeConfig(cfg, root, d, m, events, cluster, log))
	if err != nil {
		return err
	}
	opts.Items = []item.Item{dir, vol}
	opts.Generations = reconcile.GenerationOptions{Interval: cfg.genInterval, MaxAge: cfg.genMaxAge, Timeout: cfg.genTimeout}
	if cluster != nil {
		opts.PVAnnotation = cluster.PVAnnotation
	}
	return nil
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
