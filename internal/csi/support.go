package csi

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/transport"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// contextStatus converts the end of a request's own context into the gRPC
// status the caller expects: DeadlineExceeded or Canceled.
func contextStatus(err error) error {
	return status.FromContextError(err).Err()
}

// unavailable is the status for an operation that could not be completed and
// is safe to retry: nothing was done that a retry would repeat.
func unavailable(format string, args ...any) error {
	return status.Errorf(codes.Unavailable, format, args...)
}

// blocking runs fn, which makes a system call that can hang on a dead network
// file system and ignores cancellation, in a goroutine of its own and gives up
// waiting when ctx ends. The goroutine is abandoned, not stopped; it writes to
// a buffered channel so it can exit whenever the kernel lets it go. The
// caller then reports the context's error, and what fn did or did not finish
// is for a retry to find out: every operation passed here is idempotent.
func blocking[T any](ctx context.Context, fn func() (T, error)) (T, error) {
	type result struct {
		v   T
		err error
	}
	done := make(chan result, 1)
	go func() {
		v, err := fn()
		done <- result{v, err}
	}()
	select {
	case r := <-done:
		return r.v, r.err
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

// inflight admits one operation per volume name at a time. A second request
// for a volume whose first is still running is Aborted, as the CSI
// specification asks, which also means a create and a delete of one volume
// can never interleave.
type inflight struct {
	mu    sync.Mutex
	names map[string]struct{}
}

// acquire marks name busy and returns the function that releases it, or false
// if name is already busy.
func (f *inflight) acquire(name string) (release func(), ok bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, busy := f.names[name]; busy {
		return nil, false
	}
	if f.names == nil {
		f.names = map[string]struct{}{}
	}
	f.names[name] = struct{}{}
	return func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		delete(f.names, name)
	}, true
}

func busyStatus(op, name string) error {
	return status.Errorf(codes.Aborted, "another %s of volume %q is in progress", op, name)
}

// memoLimit bounds the volumes remembered by memo.
const memoLimit = 4096

// created is what CreateVolume answered for a name.
type created struct {
	context  map[string]string // volume.Context.Encode
	capacity int64             // RequiredBytes, as echoed
}

// memo remembers, for the life of the process only, what CreateVolume
// answered for each name, so that a repeated name with other parameters is
// AlreadyExists. It is a convenience and not a record: the controller is
// stateless, a restart forgets everything, and a name that is repeated with
// other parameters after one is then simply created again. Kubernetes never
// repeats a name with other parameters (a StorageClass's parameters cannot be
// edited, and the name is the PV's), so what is lost is only the check. The
// oldest entries are dropped past memoLimit so it cannot grow without bound.
type memo struct {
	mu    sync.Mutex
	byKey map[string]created
	order []string
}

func (m *memo) get(name string) (created, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.byKey[name]
	return c, ok
}

func (m *memo) put(name string, c created) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.byKey == nil {
		m.byKey = map[string]created{}
	}
	if _, known := m.byKey[name]; !known {
		m.order = append(m.order, name)
	}
	m.byKey[name] = c
	for len(m.order) > memoLimit {
		delete(m.byKey, m.order[0])
		m.order = m.order[1:]
	}
}

func (m *memo) forget(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.byKey, name)
	for i, n := range m.order {
		if n == name {
			m.order = append(m.order[:i], m.order[i+1:]...)
			break
		}
	}
}

// sameContext reports whether two encoded volume contexts are equal.
func sameContext(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || w != v {
			return false
		}
	}
	return true
}

// mounts mounts transports under root, once each. A Transport does not
// serialize its callers and the Mounter refuses a second call on a path whose
// first has not returned, while the controller serves many requests at once,
// so mounts admits one Mount per transport at a time and lets the others wait
// for it (or for their own context). The Transport values are kept so that
// the guard against piling up name lookups that hang lives as long as the
// process.
type mounts struct {
	root string
	m    mount.Mounter
	opts transport.Options

	mu      sync.Mutex
	entries map[string]*mountEntry
}

type mountEntry struct {
	tr  transport.Transport
	sem chan struct{} // one token: held while Mount runs
}

func newMounts(root string, m mount.Mounter, opts transport.Options) *mounts {
	return &mounts{root: root, m: m, opts: opts, entries: map[string]*mountEntry{}}
}

func (c *mounts) entry(spec transport.Spec) (*mountEntry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := spec.ID()
	if e, ok := c.entries[id]; ok {
		if e.tr.Spec() != spec {
			// Two different specs with one 48-bit ID. Refuse rather than
			// share a mount point.
			return nil, fmt.Errorf("transport %s has the same ID as %s", spec.Canonical(), e.tr.Spec().Canonical())
		}
		return e, nil
	}
	tr, err := transport.New(spec, c.root, c.m, c.opts)
	if err != nil {
		return nil, err
	}
	e := &mountEntry{tr: tr, sem: make(chan struct{}, 1)}
	c.entries[id] = e
	return e, nil
}

// ensure mounts the transport for spec if it is not mounted, and returns it.
// The error is a gRPC status: the context's own if it ended, else
// Unavailable.
func (c *mounts) ensure(ctx context.Context, spec transport.Spec) (transport.Transport, error) {
	e, err := c.entry(spec)
	if err != nil {
		return nil, unavailable("transport cannot be set up: %v", err)
	}
	select {
	case e.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, contextStatus(ctx.Err())
	}
	defer func() { <-e.sem }()

	if err := e.tr.Mount(ctx); err != nil {
		if ctx.Err() != nil {
			return nil, contextStatus(ctx.Err())
		}
		return nil, unavailable("transport %s cannot be mounted: %v", spec.Canonical(), err)
	}
	return e.tr, nil
}

var errNoSuchVolume = errors.New("no such volume")

// parseVolumeID parses a volume ID of the driver's own kinds.
func parseVolumeID(s string) (volume.ID, error) {
	id, err := volume.ParseID(s)
	if err != nil {
		return volume.ID{}, errNoSuchVolume
	}
	return id, nil
}
