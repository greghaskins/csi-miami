package blockd

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
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

// kernelDevice is the real nbd device.
type kernelDevice struct{ idx int }

func (k kernelDevice) index() int { return k.idx }
func (k kernelDevice) reconfigure(ctx context.Context, size int64) error {
	return nbd.Reconfigure(ctx, k.idx, size)
}
func (k kernelDevice) disconnect(ctx context.Context) error { return nbd.Disconnect(ctx, k.idx) }

// Run is the whole block-server program: open or create the cache (resuming
// from its index after a crash, else building from current), connect an nbd
// device backed by it, serve control RPCs, and prefetch the current manifest
// when the whole volume fits the cache. It returns nil after a Stop RPC or a
// cancelled ctx, having disconnected the device and synced the cache.
func Run(ctx context.Context, cfg Config, log *slog.Logger) error {
	s, err := newServer(ctx, cfg, log)
	if err != nil {
		return err
	}
	defer func() { _ = s.close() }()

	lis, err := listenControl(s.cfg.ControlSocket)
	if err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	srvConn, cliFile, err := socketpair()
	if err != nil {
		return err
	}
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- nbd.Serve(runCtx, srvConn, s.back, nbd.ServeOptions{
			ReadOnly: s.cfg.ReadOnly, BlockSize: blockSize, MaxRequest: maxRequest,
		})
	}()

	idx, err := connectDevice(ctx, s.cfg, cliFile)
	_ = cliFile.Close() // the kernel holds its own reference
	if err != nil {
		cancel()
		if serr := <-serveErr; serr != nil {
			log.Warn("serve ended", "volume", s.cfg.Volume, "err", serr)
		}
		return err
	}
	s.setDevice(kernelDevice{idx: idx})
	log.Info("blockd serving", "volume", s.cfg.Volume, "device", idx, "size", s.cfg.Size, "binaryHash", s.binaryHash)

	if s.cfg.CacheBound >= s.cfg.Size {
		go s.back.prefetch(runCtx)
	}

	ctlErr := make(chan error, 1)
	go func() { ctlErr <- s.serveControl(runCtx, lis) }()

	select {
	case <-s.stop:
		log.Info("blockd stopping on request", "volume", s.cfg.Volume)
	case <-ctx.Done():
		log.Info("blockd stopping on signal", "volume", s.cfg.Volume)
	}

	cancel() // stops the serve loop and the control loop

	dctx, dcancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer dcancel()
	if err := nbd.Disconnect(dctx, idx); err != nil {
		log.Warn("disconnect failed", "volume", s.cfg.Volume, "device", idx, "err", err)
	}
	if serr := <-serveErr; serr != nil {
		log.Warn("serve ended", "volume", s.cfg.Volume, "err", serr)
	}
	if cerr := <-ctlErr; cerr != nil {
		log.Warn("control ended", "volume", s.cfg.Volume, "err", cerr)
	}
	if err := s.back.cache.Sync(); err != nil {
		return fmt.Errorf("blockd: final sync: %w", err)
	}
	return nil
}

// serverFlags are the nbd transmission flags the device advertises.
func serverFlags(readOnly bool) uint64 {
	f := uint64(nbd.FlagHasFlags | nbd.FlagSendFlush | nbd.FlagSendFUA | nbd.FlagSendTrim)
	if readOnly {
		f |= nbd.FlagReadOnly
	}
	return f
}

// connectDevice connects the device to the serving end of the socket pair. A
// Connect cut off by its context may still have created the device, so on an
// error it looks for a device carrying this volume's backend name and
// disconnects it before returning the error.
func connectDevice(ctx context.Context, cfg Config, cliFile *os.File) (int, error) {
	cctx, ccancel := context.WithTimeout(ctx, connectTimeout)
	defer ccancel()
	idx, err := nbd.Connect(cctx, cliFile, nbd.ConnectOptions{
		Index:           -1,
		Size:            cfg.Size,
		BlockSize:       blockSize,
		Timeout:         cfg.Timeout,
		DeadConnTimeout: cfg.DeadConnTimeout,
		ServerFlags:     serverFlags(cfg.ReadOnly),
		Backend:         cfg.Volume,
	})
	if err != nil {
		if d, ok := findByBackend(cfg.Volume); ok {
			dctx, dcancel := context.WithTimeout(context.Background(), shutdownTimeout)
			defer dcancel()
			_ = nbd.Disconnect(dctx, d.Index)
		}
		return -1, fmt.Errorf("blockd: connect device: %w", err)
	}
	return idx, nil
}

// findByBackend finds a configured device whose backend identifier is name.
func findByBackend(name string) (nbd.Device, bool) {
	devs, err := nbd.Devices("/sys")
	if err != nil {
		return nbd.Device{}, false
	}
	for _, d := range devs {
		if d.Backend == name && d.Pid != 0 {
			return d, true
		}
	}
	return nbd.Device{}, false
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
