package blockd

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/nbd"
)

// connectTimeout bounds the netlink connect, and shutdownTimeout the disconnect
// and final sync.
const (
	connectTimeout  = 60 * time.Second
	shutdownTimeout = 30 * time.Second
)

// confirmAttempt is how long a reconfigure is given to show a request on the
// new socket before it is issued again, and confirmBudget the whole of it. The
// budget stays well inside the device's dead-connection timeout (60 s by
// default), after which requests held for the dead connection fail with EIO.
// Variables so tests can shorten them.
var (
	confirmAttempt = 3 * time.Second
	confirmBudget  = 15 * time.Second
)

// kernelAPI is the nbd kernel interface Run needs; the kernel in production, a
// fake in tests.
type kernelAPI interface {
	devices() ([]nbd.Device, error)
	// connect creates a device on sock (which has not been through the
	// handshake) and returns its index.
	connect(ctx context.Context, cfg Config, sock *os.File) (int, error)
	// reconnect hands sock, already through the handshake, to the device.
	reconnect(ctx context.Context, idx int, sock *os.File) error
	disconnect(ctx context.Context, idx int) error
	// resize sets the device's size, sending no socket.
	resize(ctx context.Context, idx int, size int64) error
	// touch sends one request through the device to offset 0 and returns
	// once it completes. b is the backend that serves the device's socket;
	// the kernel reaches it over the socket, a fake may call it directly.
	touch(ctx context.Context, idx int, b nbd.Backend) error
}

// realKernel is the host's nbd.
type realKernel struct{ sysfs string }

func (r realKernel) devices() ([]nbd.Device, error) { return nbd.Devices(r.sysfs) }

func (realKernel) connect(ctx context.Context, cfg Config, sock *os.File) (int, error) {
	return nbd.Connect(ctx, sock, nbd.ConnectOptions{
		Index:           -1,
		Size:            cfg.Size,
		BlockSize:       blockSize,
		Timeout:         cfg.Timeout,
		DeadConnTimeout: cfg.DeadConnTimeout,
		ServerFlags:     serverFlags(cfg.ReadOnly),
		Backend:         cfg.Volume,
	})
}

func (realKernel) reconnect(ctx context.Context, idx int, sock *os.File) error {
	return nbd.ReconnectSocket(ctx, idx, sock)
}

func (realKernel) disconnect(ctx context.Context, idx int) error { return nbd.Disconnect(ctx, idx) }

func (realKernel) resize(ctx context.Context, idx int, size int64) error {
	return nbd.Reconfigure(ctx, idx, size)
}

// touch reads one block at offset 0 of /dev/nbdN with O_DIRECT, so the read
// goes to the server rather than the page cache. The read blocks while the
// connection is dead, and cannot be interrupted; ctx only stops it starting.
func (realKernel) touch(ctx context.Context, idx int, _ nbd.Backend) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	fd, err := unix.Open(fmt.Sprintf("/dev/nbd%d", idx), unix.O_RDONLY|unix.O_DIRECT|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("blockd: open nbd%d: %w", idx, err)
	}
	defer func() { _ = unix.Close(fd) }()
	// O_DIRECT wants an aligned buffer; an anonymous mapping is page-aligned.
	buf, err := unix.Mmap(-1, 0, blockSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANON|unix.MAP_PRIVATE)
	if err != nil {
		return fmt.Errorf("blockd: map read buffer: %w", err)
	}
	defer func() { _ = unix.Munmap(buf) }()
	if _, err := unix.Pread(fd, buf, 0); err != nil {
		return fmt.Errorf("blockd: read nbd%d: %w", idx, err)
	}
	return nil
}

// notifyBackend marks the first request that reaches the serving side. A
// reconfigure the kernel dropped sends nothing to the new socket, so a request
// arriving on it is the proof that the device is served by this process.
type notifyBackend struct {
	nbd.Backend
	served chan struct{}
	once   sync.Once
}

func newNotifyBackend(b nbd.Backend) *notifyBackend {
	return &notifyBackend{Backend: b, served: make(chan struct{})}
}

func (n *notifyBackend) mark() { n.once.Do(func() { close(n.served) }) }

func (n *notifyBackend) ReadAt(p []byte, off int64) (int, error) {
	n.mark()
	return n.Backend.ReadAt(p, off)
}

func (n *notifyBackend) WriteAt(p []byte, off int64, fua bool) (int, error) {
	n.mark()
	return n.Backend.WriteAt(p, off, fua)
}

func (n *notifyBackend) Flush() error { n.mark(); return n.Backend.Flush() }

func (n *notifyBackend) Trim(off, nn int64) error { n.mark(); return n.Backend.Trim(off, nn) }

// kernelDevice is the real nbd device.
type kernelDevice struct{ idx int }

func (k kernelDevice) index() int { return k.idx }
func (k kernelDevice) reconfigure(ctx context.Context, size int64) error {
	return nbd.Reconfigure(ctx, k.idx, size)
}
func (k kernelDevice) disconnect(ctx context.Context) error { return nbd.Disconnect(ctx, k.idx) }

// Run is the whole block-server program: open or create the cache (resuming
// from its index after a crash, else building from current), put an nbd device
// on it, serve control RPCs, and prefetch the current manifest when the whole
// volume fits the cache. The device is a new one, unless a configured device
// already carries this volume's name: a server that crashed left it, with its
// mapping and mount, and this one re-points it at its own socket (decision 51).
// It returns nil after a Stop RPC or a cancelled ctx, having disconnected the
// device and synced the cache. A server that cannot take over a left device
// returns an error and leaves the device alone.
func Run(ctx context.Context, cfg Config, log *slog.Logger) error {
	return runOn(ctx, cfg, log, realKernel{sysfs: "/sys"})
}

func runOn(ctx context.Context, cfg Config, log *slog.Logger, k kernelAPI) error {
	cfg = cfg.withDefaults()
	// First of all, so that no two incarnations of this volume's server can
	// both look at the device, let alone re-point it. Released last (deferred
	// first), after the serve loop has returned and the cache is closed: a
	// process that lingers past giving up (the probe read below cannot be
	// interrupted) must not touch the cache once the next one may hold it.
	var s *server
	lock, err := lockCache(cfg.CacheDir)
	if err != nil {
		return err
	}
	defer func() { lock.release(s) }()

	left, err := leftDevice(k, cfg.Volume)
	if err != nil {
		return err
	}
	if left != nil {
		// The device may have been grown after this unit's arguments were written.
		if cfg, err = adoptDeviceSize(cfg, *left); err != nil {
			return err
		}
	}
	// A device that is already carrying a mount is only ever re-pointed at the
	// cache that served it: one rebuilt from the store would be older than the
	// filesystem above it.
	s, err = openServer(ctx, cfg, log, left != nil)
	if err != nil {
		if left != nil {
			err = fmt.Errorf("blockd: nbd%d carries volume %q and is left in place: %w", left.Index, cfg.Volume, err)
		}
		return err
	}
	defer func() { _ = s.close() }()

	lis, err := listenControl(s.cfg.ControlSocket)
	if err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// The control service answers from here on, with Status.Index -1 until the
	// device is known to be served by this process.
	ctlErr := make(chan error, 1)
	go func() { ctlErr <- s.serveControl(runCtx, lis) }()

	srv, err := startServe(runCtx, s, left != nil)
	if err != nil {
		cancel()
		drainControl(log, s, ctlErr)
		return err
	}

	var idx int
	path := "connect"
	if left == nil {
		idx, err = connectDevice(ctx, k, s.cfg, srv.cli)
	} else {
		path = "reconfigure"
		idx = left.Index
		err = reconfigureDevice(ctx, k, s.cfg, idx, srv.cli, srv.notify)
		if err == nil && left.Size < s.cfg.Size {
			// Finish a grow that was interrupted before its kernel half.
			err = k.resize(ctx, idx, s.cfg.Size)
		}
	}
	_ = srv.cli.Close() // the kernel holds its own reference
	if err != nil {
		cancel()
		srv.drain(log, s)
		drainControl(log, s, ctlErr)
		return err
	}
	s.setDevice(kernelDevice{idx: idx})
	log.Info("blockd serving", "volume", s.cfg.Volume, "path", path, "device", idx, "size", s.cfg.Size, "binaryHash", s.binaryHash)

	if s.cfg.CacheBound >= s.cfg.Size {
		go s.back.prefetch(runCtx)
	}

	// The device is disconnected only on a Stop request, which is unstage: the
	// mount above it is gone. Every other way out leaves it configured, with
	// its mapping and mount, for the next incarnation to re-point (decision 51).
	var stopped bool
	var fail error
loop:
	for {
		select {
		case <-s.stop:
			log.Info("blockd stopping on request", "volume", s.cfg.Volume)
			stopped = true
			break loop
		case <-ctx.Done():
			log.Info("blockd stopping on signal; the device is left in place", "volume", s.cfg.Volume, "device", idx)
			break loop
		case serr := <-srv.err:
			srv = nil // its result is taken
			// Serve returned on its own. A request timing out makes the kernel
			// shut our socket down, and it then holds requests for the
			// dead-connection timeout waiting for a new one: give it one.
			if ctx.Err() != nil {
				break loop
			}
			if serr == nil {
				// A clean NBD_CMD_DISC: someone disconnected the device.
				log.Warn("device disconnected from outside", "volume", s.cfg.Volume, "device", idx)
				break loop
			}
			log.Error("serve ended unexpectedly; re-pointing the device", "volume", s.cfg.Volume, "device", idx, "err", serr)
			nsrv, err := repoint(runCtx, ctx, k, s, idx)
			srv = nsrv
			if err != nil {
				fail = err
				break loop
			}
			log.Info("blockd serving", "volume", s.cfg.Volume, "path", "repoint", "device", idx, "size", s.cfg.Size)
		}
	}

	cancel() // stops the serve loop and the control loop

	if stopped {
		dctx, dcancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer dcancel()
		if err := k.disconnect(dctx, idx); err != nil {
			log.Warn("disconnect failed", "volume", s.cfg.Volume, "device", idx, "err", err)
		}
	}
	srv.drain(log, s)
	drainControl(log, s, ctlErr)
	if err := s.back.cache.Sync(); err != nil && fail == nil {
		fail = fmt.Errorf("blockd: final sync: %w", err)
	}
	// A failure to take the device back after a serve death is returned, so the
	// unit exits non-zero and systemd restarts it; the device is still there.
	return fail
}

// serving is one serve loop and the client end of its socket.
type serving struct {
	cli    *os.File
	err    chan error
	notify *notifyBackend // nil when the first request is not awaited
}

// startServe starts a serve loop on a new socketpair; cli is the end for the
// kernel. With confirm set the backend marks the first request it sees.
func startServe(ctx context.Context, s *server, confirm bool) (*serving, error) {
	srvConn, cliFile, err := socketpair()
	if err != nil {
		return nil, err
	}
	sv := &serving{cli: cliFile, err: make(chan error, 1)}
	var back nbd.Backend = s.back
	if confirm {
		sv.notify = newNotifyBackend(s.back)
		back = sv.notify
	}
	go func() {
		sv.err <- nbd.Serve(ctx, srvConn, back, nbd.ServeOptions{
			ReadOnly: s.cfg.ReadOnly, BlockSize: blockSize, MaxRequest: maxRequest,
		})
	}()
	return sv, nil
}

// drain waits for the serve loop (whose context is cancelled) and logs how it ended.
func (sv *serving) drain(log *slog.Logger, s *server) {
	if sv == nil {
		return
	}
	if serr := <-sv.err; serr != nil {
		log.Warn("serve ended", "volume", s.cfg.Volume, "err", serr)
	}
}

func drainControl(log *slog.Logger, s *server, ctlErr <-chan error) {
	if cerr := <-ctlErr; cerr != nil {
		log.Warn("control ended", "volume", s.cfg.Volume, "err", cerr)
	}
}

// repoint serves the device again after its serve loop died: a new socket, the
// handshake, a reconfigure and the same confirmation as at start. On failure
// the device is left as it is.
func repoint(runCtx, ctx context.Context, k kernelAPI, s *server, idx int) (*serving, error) {
	sv, err := startServe(runCtx, s, true)
	if err != nil {
		return nil, err
	}
	err = reconfigureDevice(ctx, k, s.cfg, idx, sv.cli, sv.notify)
	_ = sv.cli.Close()
	return sv, err
}

// serverFlags are the nbd transmission flags the device advertises.
func serverFlags(readOnly bool) uint64 {
	f := uint64(nbd.FlagHasFlags | nbd.FlagSendFlush | nbd.FlagSendFUA | nbd.FlagSendTrim)
	if readOnly {
		f |= nbd.FlagReadOnly
	}
	return f
}

// leftDevice finds the device a crashed server of this volume left: configured
// (a nonzero pid) with the volume's name as its backend identifier. It returns
// nil when there is none, and an error when there are several, which are not
// ours to choose between.
func leftDevice(k kernelAPI, volume string) (*nbd.Device, error) {
	devs, err := k.devices()
	if err != nil {
		return nil, fmt.Errorf("blockd: list nbd devices: %w", err)
	}
	var found []nbd.Device
	for _, d := range devs {
		if d.Backend == volume && d.Pid != 0 {
			found = append(found, d)
		}
	}
	switch len(found) {
	case 0:
		return nil, nil
	case 1:
		return &found[0], nil
	}
	idxs := make([]int, len(found))
	for i, d := range found {
		idxs[i] = d.Index
	}
	return nil, fmt.Errorf("blockd: volume %q is on several nbd devices %v; refusing to choose", volume, idxs)
}

// adoptDeviceSize returns cfg with the size of a device left by a previous
// server taken into account. A size never shrinks, so the size served is the
// larger of the unit's arguments and the device: the device can have been
// grown after the arguments were written, or the arguments can have been
// raised for a grow that never reached the device (Run then finishes it).
// The cache index is the third party to this; resuming it adopts a larger one.
// Only a size that is not whole chunks is refused.
func adoptDeviceSize(cfg Config, d nbd.Device) (Config, error) {
	if cfg.Chunk <= 0 || d.Size%cfg.Chunk != 0 {
		return cfg, fmt.Errorf("blockd: nbd%d is %d bytes, not a whole number of %d byte chunks", d.Index, d.Size, cfg.Chunk)
	}
	cfg.Size = max(cfg.Size, d.Size)
	return cfg, nil
}

// connectDevice connects a new device to the serving end of the socket pair. A
// Connect cut off by its context may still have created the device, so on an
// error it looks for a device THIS process created and disconnects it before
// returning the error.
func connectDevice(ctx context.Context, k kernelAPI, cfg Config, cliFile *os.File) (int, error) {
	cctx, ccancel := context.WithTimeout(ctx, connectTimeout)
	defer ccancel()
	idx, err := k.connect(cctx, cfg, cliFile)
	if err != nil {
		if d, ok := findOurDevice(k, cfg.Volume); ok {
			dctx, dcancel := context.WithTimeout(context.Background(), shutdownTimeout)
			defer dcancel()
			_ = k.disconnect(dctx, d.Index)
		}
		return -1, fmt.Errorf("blockd: connect device: %w", err)
	}
	return idx, nil
}

// reconfigureDevice re-points the left device at cliFile and confirms the
// kernel took it. The kernel puts a new socket only into a connection slot
// already marked dead, which the receive thread does the moment the old server
// dies; with none it reports success and drops the socket. So after each
// reconfigure it waits for a request to reach the served backend (a read
// through the device, started once, is held by the dead connection and resent
// on the new socket), and issues the reconfigure again if none does. It
// returns an error when confirmBudget passes, never having disconnected: the
// device carries a live mount, and the unit will start us again.
func reconfigureDevice(ctx context.Context, k kernelAPI, cfg Config, idx int, cliFile *os.File, n *notifyBackend) error {
	bctx, cancel := context.WithTimeout(ctx, confirmBudget)
	defer cancel()
	// The handshake runs once: the serve loop is past it after this.
	if err := nbd.Handshake(bctx, cliFile, cfg.Size); err != nil {
		return fmt.Errorf("blockd: reconfigure nbd%d: %w", idx, err)
	}
	probeCtx, stopProbe := context.WithCancel(bctx)
	defer stopProbe()
	go func() { _ = k.touch(probeCtx, idx, n) }()
	for {
		if err := k.reconnect(bctx, idx, cliFile); err != nil {
			return fmt.Errorf("blockd: reconfigure nbd%d: %w", idx, err)
		}
		t := time.NewTimer(confirmAttempt)
		select {
		case <-n.served:
			t.Stop()
			return nil
		case <-t.C:
		case <-bctx.Done():
			t.Stop()
			select {
			case <-n.served:
				return nil
			default:
			}
			return fmt.Errorf("blockd: reconfigure nbd%d: no request reached this server within %s; the device is left in place: %w", idx, confirmBudget, bctx.Err())
		}
	}
}

// findOurDevice finds a configured device this process created: the backend
// identifier matches and the sysfs serving pid (a TID) belongs to this process.
// After a crash the dead device keeps the same backend string, so matching the
// name alone could pick another incarnation's corpse — still carrying its
// dm-crypt mapping and mount — and disconnect it. The pid check (I3) avoids it.
func findOurDevice(k kernelAPI, name string) (nbd.Device, bool) {
	devs, err := k.devices()
	if err != nil {
		return nbd.Device{}, false
	}
	me := os.Getpid()
	for _, d := range devs {
		if d.Backend == name && d.Pid != 0 && tgidOf(d.Pid) == me {
			return d, true
		}
	}
	return nbd.Device{}, false
}

// tgidOf maps a thread id (what sysfs records as an nbd device's pid) to its
// thread-group id, the process it belongs to, or -1 when it cannot be read.
func tgidOf(tid int) int {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", tid)) //nolint:gosec // a /proc path from a sysfs-reported tid
	if err != nil {
		return -1
	}
	for _, line := range strings.Split(string(b), "\n") {
		if rest, ok := strings.CutPrefix(line, "Tgid:"); ok {
			if g, err := strconv.Atoi(strings.TrimSpace(rest)); err == nil {
				return g
			}
			return -1
		}
	}
	return -1
}

// socketpair returns a connected pair: a net.Conn for Serve and an *os.File for
// Connect (which hands it to the kernel).
func socketpair() (net.Conn, *os.File, error) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("blockd: socketpair: %w", err)
	}
	srvFile := os.NewFile(uintptr(fds[0]), "nbd-serve") //nolint:gosec // a socket fd we just created
	srvConn, err := net.FileConn(srvFile)
	_ = srvFile.Close() // FileConn dups the descriptor
	if err != nil {
		_ = unix.Close(fds[1])
		return nil, nil, fmt.Errorf("blockd: socketpair conn: %w", err)
	}
	cliFile := os.NewFile(uintptr(fds[1]), "nbd-conn") //nolint:gosec // a socket fd we just created
	return srvConn, cliFile, nil
}
