// Package probe is the active probe of a volume's device: a direct read of its
// first sector through the device a pod's file system sits on (the mapper, or
// the nbd device of a plain volume), which fails when the block server, its
// connection or the layers above it no longer answer.
//
// A read on a device whose server is gone cannot be interrupted: it returns
// when the kernel's dead-connection timeout (60 s for an nbd device) declares
// the connection dead, or never. So a probe runs in a goroutine of its own
// that nothing waits for. Observe, called from the reconcile pass, starts at
// most one probe per volume and returns at once; a probe that has run longer
// than the timeout is counted as a failure by the next Observe, while its
// goroutine is left to finish by itself. Two consecutive failures mark the
// volume abnormal; one success clears it.
package probe

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// SectorSize is how much a probe reads: the largest logical block size a
// device of ours can have, so an O_DIRECT read is aligned whatever it is.
const SectorSize = 4096

// Defaults for the zero fields of Options.
const (
	DefaultTimeout  = 10 * time.Second
	DefaultFailures = 2
)

// The messages of an abnormal state. They are fixed text: a message reaches
// the kubelet and the pod's events, so it names no path and carries no error
// text, which stays in the node's log.
const (
	MessageFailed  = "the volume's device did not answer a read of its first sector" + Guidance
	MessageTimeout = "the volume's device did not answer a read of its first sector in time" + Guidance
)

// Guidance is what an operator does about an abnormal volume (decision 53:
// M2 repairs nothing by itself). ext4 stays read-only after an I/O error until
// it is mounted again, so waiting does not help: the pod has to restart.
const Guidance = "; restart the volume's pod once the store is reachable and the cache has drained, because the file system stays read-only until it is remounted"

// Options configure a Prober.
type Options struct {
	// Read reads the first sector of the device at path. Nil means Read. A
	// read that does not return is expected and tolerated.
	Read func(ctx context.Context, path string) error
	// Timeout is how long a probe may run before it counts as a failure. Zero
	// means DefaultTimeout.
	Timeout time.Duration
	// Failures is how many consecutive failures make a volume abnormal. Zero
	// means DefaultFailures.
	Failures int
	// Now is the clock. Nil means time.Now.
	Now func() time.Time
}

// State is what the probe knows of one volume.
type State struct {
	// Known says a probe has finished (or timed out) for the volume.
	Known bool
	// Abnormal says Failures consecutive probes failed.
	Abnormal bool
	// Message says why, fixed text; empty unless Abnormal.
	Message string
}

// Prober probes volumes. It is safe for concurrent use.
type Prober struct {
	opts   Options
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu   sync.Mutex
	vols map[string]*entry
}

type entry struct {
	inflight bool
	deadline time.Time // when the running probe counts as failed
	counted  bool      // the running probe has been counted as a timeout
	fails    int       // consecutive failures
	known    bool
	message  string // of the latest failure
}

// New returns a Prober.
func New(o Options) *Prober {
	if o.Read == nil {
		o.Read = Read
	}
	if o.Timeout <= 0 {
		o.Timeout = DefaultTimeout
	}
	if o.Failures <= 0 {
		o.Failures = DefaultFailures
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Prober{opts: o, ctx: ctx, cancel: cancel, vols: map[string]*entry{}}
}

// Observe asks for a probe of the volume's device. When one is already in
// flight it starts none; it counts that one as a failure if it has run past
// the timeout, once per timeout. It never waits for a read.
func (p *Prober) Observe(id, device string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.opts.Now()
	e := p.vols[id]
	if e == nil {
		e = &entry{}
		p.vols[id] = e
	}
	if e.inflight {
		if !now.Before(e.deadline) {
			p.fail(e, MessageTimeout)
			e.counted = true
			e.deadline = now.Add(p.opts.Timeout)
		}
		return
	}
	e.inflight, e.counted = true, false
	e.deadline = now.Add(p.opts.Timeout)
	p.wg.Add(1)
	go p.run(e, device)
}

// run is one probe's goroutine.
func (p *Prober) run(e *entry, device string) {
	defer p.wg.Done()
	err := p.opts.Read(p.ctx, device)
	p.mu.Lock()
	defer p.mu.Unlock()
	e.inflight = false
	switch {
	case err == nil:
		e.known, e.fails, e.message = true, 0, ""
	case p.ctx.Err() != nil:
		// Closed: the read was cancelled, not answered.
	case !e.counted:
		p.fail(e, MessageFailed)
	}
}

// fail counts one failure. Called with p.mu held.
func (p *Prober) fail(e *entry, msg string) {
	e.known = true
	e.fails++
	e.message = msg
}

// State returns what is known of the volume.
func (p *Prober) State(id string) State {
	p.mu.Lock()
	defer p.mu.Unlock()
	e := p.vols[id]
	if e == nil {
		return State{}
	}
	st := State{Known: e.known}
	if e.fails >= p.opts.Failures {
		st.Abnormal, st.Message = true, e.message
	}
	return st
}

// Forget drops the volume's state, for a volume that is no longer staged or
// whose stack is being rebuilt. A read still in flight finishes unobserved.
func (p *Prober) Forget(id string) {
	p.mu.Lock()
	delete(p.vols, id)
	p.mu.Unlock()
}

// IDs lists the volumes the prober holds state for.
func (p *Prober) IDs() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	ids := make([]string, 0, len(p.vols))
	for id := range p.vols {
		ids = append(ids, id)
	}
	return ids
}

// Wait blocks until no read is in flight. Tests use it; a read on a dead
// device may take a minute to return.
func (p *Prober) Wait() { p.wg.Wait() }

// Close cancels the context of reads in flight. It does not wait: a read that
// ignores it is a kernel's to give back.
func (p *Prober) Close() { p.cancel() }

// openDevice is open(2), a seam for the test of the flags.
var openDevice = unix.Open

// Read is the real probe: it opens the device read-only with O_DIRECT, so the
// page cache cannot answer for a dead server, and reads its first SectorSize
// bytes. A short read is a failure.
func Read(ctx context.Context, path string) error {
	_, err := readFirstSector(ctx, path)
	return err
}

func readFirstSector(ctx context.Context, path string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fd, err := openDevice(path, unix.O_RDONLY|unix.O_DIRECT|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("probe: open: %w", err)
	}
	defer func() { _ = unix.Close(fd) }()
	// O_DIRECT wants a buffer aligned to the logical block size.
	raw := make([]byte, 2*SectorSize)
	off := 0
	if rem := int(uintptr(unsafe.Pointer(&raw[0])) % SectorSize); rem != 0 { //nolint:gosec // only the address's low bits are read
		off = SectorSize - rem
	}
	buf := raw[off : off+SectorSize]
	n, err := unix.Pread(fd, buf, 0)
	switch {
	case err != nil:
		return nil, fmt.Errorf("probe: read: %w", err)
	case n < SectorSize:
		return nil, fmt.Errorf("probe: read: %w: %d of %d bytes", errShort, n, SectorSize)
	}
	return buf, nil
}

var errShort = errors.New("short read")
