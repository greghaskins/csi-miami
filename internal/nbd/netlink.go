package nbd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/mdlayher/genetlink"
	"github.com/mdlayher/netlink"
)

// ErrNoNBD is returned when the kernel's nbd generic netlink family cannot be
// resolved: the nbd module is not loaded (or not built).
var ErrNoNBD = errors.New("nbd: the kernel's nbd netlink family is not available; load the nbd module")

// The nbd generic netlink interface (include/uapi/linux/nbd-netlink.h).
const (
	genlFamily = "nbd"

	nlCmdConnect     = 1
	nlCmdDisconnect  = 2
	nlCmdReconfigure = 3

	attrIndex             = 1  // u32
	attrSizeBytes         = 2  // u64
	attrBlockSizeBytes    = 3  // u64
	attrTimeout           = 4  // u64, seconds
	attrServerFlags       = 5  // u64
	attrSockets           = 7  // nested NBD_SOCK_ITEM
	attrDeadConnTimeout   = 8  // u64, seconds
	attrBackendIdentifier = 10 // string

	sockItem = 1 // nested
	sockFD   = 1 // u32
)

// availableTimeout bounds Available, which takes no context.
const availableTimeout = 10 * time.Second

// ConnectOptions describe the device Connect creates.
type ConnectOptions struct {
	Index                    int // -1: allocate
	Size                     int64
	BlockSize                uint32
	Timeout, DeadConnTimeout time.Duration // seconds on the wire, rounded up
	ServerFlags              uint64        // the three flags above, plus HAS_FLAGS
	Backend                  string        // written to /sys/block/nbdN/backend
}

// Connect hands sock to the kernel as the connection of an nbd device and
// returns the device's index (NBD_CMD_CONNECT).
//
// sock is the client end of a stream socket whose other end runs Serve. The
// kernel speaks only the transmission phase, so Connect first runs the client
// half of the handshake on sock (NBD_OPT_GO for the default export) and checks
// that the export is at least o.Size bytes; then it passes the descriptor to
// the kernel, which keeps its own reference. The caller may close sock once
// Connect returns.
//
// The whole call is bounded by ctx: when ctx ends Connect returns ctx's error,
// abandoning a kernel call that has not returned.
func Connect(ctx context.Context, sock *os.File, o ConnectOptions) (int, error) {
	if o.Size <= 0 || o.BlockSize < 512 || o.BlockSize&(o.BlockSize-1) != 0 || o.Size%int64(o.BlockSize) != 0 {
		return -1, fmt.Errorf("nbd: connect: size %d and block size %d: the size must be a positive multiple of a power-of-two block size", o.Size, o.BlockSize)
	}
	e, err := handshakeFile(ctx, sock)
	if err != nil {
		return -1, fmt.Errorf("nbd: connect: handshake: %w", err)
	}
	if e.size < uint64(o.Size) {
		return -1, fmt.Errorf("nbd: connect: export is %d bytes, smaller than the device's %d", e.size, o.Size)
	}
	return bounded(ctx, func(ctx context.Context) (int, error) {
		// Fd puts sock into blocking mode, which the kernel does not mind:
		// it sends and receives with its own flags.
		fd := sock.Fd()
		attrs, err := connectAttrs(o, fd)
		if err != nil {
			return -1, err
		}
		msgs, err := execute(ctx, nlCmdConnect, attrs)
		runtime.KeepAlive(sock)
		if err != nil {
			return -1, fmt.Errorf("nbd: connect: %w", err)
		}
		return replyIndex(msgs)
	})
}

// Reconfigure sets the size of a connected device (NBD_CMD_RECONFIGURE with
// NBD_ATTR_SIZE_BYTES). It passes no sockets: the kernel takes them on
// reconfigure only to replace a dead connection. A device connected with a
// backend identifier can be reconfigured only by naming it again, so the
// identifier is read back from /sys/block/nbdN/backend and sent along.
func Reconfigure(ctx context.Context, index int, size int64) error {
	return reconfigure(ctx, "/sys", index, size)
}

func reconfigure(ctx context.Context, sysfs string, index int, size int64) error {
	if index < 0 || size <= 0 {
		return fmt.Errorf("nbd: reconfigure: index %d, size %d", index, size)
	}
	backend, _, err := readAttr(filepath.Join(sysfs, "block", "nbd"+strconv.Itoa(index)), "backend")
	if err != nil {
		return fmt.Errorf("nbd: reconfigure nbd%d: %w", index, err)
	}
	_, err = bounded(ctx, func(ctx context.Context) (struct{}, error) {
		attrs, err := reconfigureAttrs(index, size, backend)
		if err != nil {
			return struct{}{}, err
		}
		_, err = execute(ctx, nlCmdReconfigure, attrs)
		return struct{}{}, err
	})
	if err != nil {
		return fmt.Errorf("nbd: reconfigure nbd%d: %w", index, err)
	}
	return nil
}

// reconfigureAttrs encodes NBD_CMD_RECONFIGURE's attributes.
func reconfigureAttrs(index int, size int64, backend string) ([]byte, error) {
	ae := netlink.NewAttributeEncoder()
	ae.Uint32(attrIndex, uint32(index))    //nolint:gosec // checked non-negative; device indexes are small
	ae.Uint64(attrSizeBytes, uint64(size)) //nolint:gosec // checked positive
	if backend != "" {
		ae.String(attrBackendIdentifier, backend)
	}
	return ae.Encode()
}

// Handshake runs the client half of the handshake on sock (NBD_OPT_GO for the
// default export) and checks that the export is at least size bytes. The
// kernel speaks only the transmission phase, so a socket must go through this
// once, before it is handed over with ReconnectSocket (Connect does the same
// for itself). It is bounded by ctx.
func Handshake(ctx context.Context, sock *os.File, size int64) error {
	e, err := handshakeFile(ctx, sock)
	if err != nil {
		return fmt.Errorf("nbd: handshake: %w", err)
	}
	if e.size < uint64(max(size, 0)) { //nolint:gosec // clamped non-negative
		return fmt.Errorf("nbd: handshake: export is %d bytes, smaller than the device's %d", e.size, size)
	}
	return nil
}

// ReconnectSocket hands sock, already through Handshake, to a connected device
// whose server is gone (NBD_CMD_RECONFIGURE with NBD_ATTR_SOCKETS). The device,
// and whatever is stacked on it, stays; requests held while the connection was
// dead are resent on sock. No size is sent: a size that differed from the
// device's would notify a uevent.
//
// The kernel puts a socket only into a connection slot already marked dead.
// With none it reports success and drops the socket, so a nil return does not
// mean the device is served by sock: the caller confirms that by seeing a
// request arrive on it, and may call again. The backend identifier is read
// back from /sys/block/nbdN/backend and sent along, as Reconfigure does.
func ReconnectSocket(ctx context.Context, index int, sock *os.File) error {
	return reconnectSocket(ctx, "/sys", index, sock)
}

func reconnectSocket(ctx context.Context, sysfs string, index int, sock *os.File) error {
	if index < 0 || sock == nil {
		return fmt.Errorf("nbd: reconnect: index %d, socket %v", index, sock)
	}
	backend, _, err := readAttr(filepath.Join(sysfs, "block", "nbd"+strconv.Itoa(index)), "backend")
	if err != nil {
		return fmt.Errorf("nbd: reconnect nbd%d: %w", index, err)
	}
	_, err = bounded(ctx, func(ctx context.Context) (struct{}, error) {
		fd := sock.Fd()
		attrs, err := reconnectAttrs(index, backend, fd)
		if err != nil {
			return struct{}{}, err
		}
		_, err = execute(ctx, nlCmdReconfigure, attrs)
		runtime.KeepAlive(sock)
		return struct{}{}, err
	})
	if err != nil {
		return fmt.Errorf("nbd: reconnect nbd%d: %w", index, err)
	}
	return nil
}

// reconnectAttrs encodes NBD_CMD_RECONFIGURE's attributes for a new socket:
// the index, the backend identifier when there is one, and the socket.
func reconnectAttrs(index int, backend string, fd uintptr) ([]byte, error) {
	ae := netlink.NewAttributeEncoder()
	ae.Uint32(attrIndex, uint32(index)) //nolint:gosec // checked non-negative
	if backend != "" {
		ae.String(attrBackendIdentifier, backend)
	}
	encodeSocket(ae, fd)
	return ae.Encode()
}

// Disconnect asks the kernel to disconnect a device (NBD_CMD_DISCONNECT): it
// sends NBD_CMD_DISC to the server and shuts the connection down.
func Disconnect(ctx context.Context, index int) error {
	if index < 0 {
		return fmt.Errorf("nbd: disconnect: index %d", index)
	}
	_, err := bounded(ctx, func(ctx context.Context) (struct{}, error) {
		ae := netlink.NewAttributeEncoder()
		ae.Uint32(attrIndex, uint32(index)) //nolint:gosec // checked non-negative
		attrs, err := ae.Encode()
		if err != nil {
			return struct{}{}, err
		}
		_, err = execute(ctx, nlCmdDisconnect, attrs)
		return struct{}{}, err
	})
	if err != nil {
		return fmt.Errorf("nbd: disconnect nbd%d: %w", index, err)
	}
	return nil
}

// Available reports whether the kernel's nbd family resolves; ErrNoNBD if not.
func Available() error {
	ctx, cancel := context.WithTimeout(context.Background(), availableTimeout)
	defer cancel()
	_, err := bounded(ctx, func(ctx context.Context) (struct{}, error) {
		c, err := dial(ctx)
		if err != nil {
			return struct{}{}, err
		}
		defer func() { _ = c.Close() }()
		_, err = family(c)
		return struct{}{}, ctxErr(ctx, err)
	})
	return err
}

// connectAttrs encodes NBD_CMD_CONNECT's attributes.
func connectAttrs(o ConnectOptions, fd uintptr) ([]byte, error) {
	ae := netlink.NewAttributeEncoder()
	if o.Index >= 0 {
		ae.Uint32(attrIndex, uint32(o.Index)) //nolint:gosec // checked non-negative
	}
	ae.Uint64(attrSizeBytes, uint64(o.Size)) //nolint:gosec // checked positive
	ae.Uint64(attrBlockSizeBytes, uint64(o.BlockSize))
	if s := seconds(o.Timeout); s > 0 {
		ae.Uint64(attrTimeout, s)
	}
	if s := seconds(o.DeadConnTimeout); s > 0 {
		ae.Uint64(attrDeadConnTimeout, s)
	}
	ae.Uint64(attrServerFlags, o.ServerFlags)
	if o.Backend != "" {
		ae.String(attrBackendIdentifier, o.Backend)
	}
	encodeSocket(ae, fd)
	return ae.Encode()
}

// encodeSocket adds the nested NBD_ATTR_SOCKETS list naming one descriptor.
func encodeSocket(ae *netlink.AttributeEncoder, fd uintptr) {
	ae.Nested(attrSockets, func(nae *netlink.AttributeEncoder) error {
		nae.Nested(sockItem, func(sae *netlink.AttributeEncoder) error {
			sae.Uint32(sockFD, uint32(fd)) //nolint:gosec // a file descriptor fits
			return nil
		})
		return nil
	})
}

// seconds rounds d up to whole seconds; 0 for none.
func seconds(d time.Duration) uint64 {
	if d <= 0 {
		return 0
	}
	return uint64((d + time.Second - 1) / time.Second)
}

// replyIndex finds NBD_ATTR_INDEX in the reply to NBD_CMD_CONNECT.
func replyIndex(msgs []genetlink.Message) (int, error) {
	for _, m := range msgs {
		ad, err := netlink.NewAttributeDecoder(m.Data)
		if err != nil {
			continue
		}
		for ad.Next() {
			if ad.Type() == attrIndex {
				return int(ad.Uint32()), nil
			}
		}
	}
	return -1, errors.New("nbd: connect: the kernel's reply carries no device index")
}

// handshakeFile runs the client handshake on a duplicate of sock's descriptor
// under ctx.
func handshakeFile(ctx context.Context, sock *os.File) (export, error) {
	c, err := net.FileConn(sock)
	if err != nil {
		return export{}, err
	}
	defer func() { _ = c.Close() }()
	if dl, ok := ctx.Deadline(); ok {
		if err := c.SetDeadline(dl); err != nil {
			return export{}, err
		}
	}
	stop := context.AfterFunc(ctx, func() { _ = c.SetDeadline(time.Unix(1, 0)) })
	defer stop()
	e, err := clientHandshake(c)
	if err = ctxErr(ctx, err); err != nil {
		return export{}, err
	}
	return e, nil
}

func dial(ctx context.Context) (*genetlink.Conn, error) {
	c, err := genetlink.Dial(nil)
	if err != nil {
		return nil, err
	}
	if dl, ok := ctx.Deadline(); ok {
		if err := c.SetDeadline(dl); err != nil {
			_ = c.Close()
			return nil, err
		}
	}
	return c, nil
}

func family(c *genetlink.Conn) (genetlink.Family, error) {
	f, err := c.GetFamily(genlFamily)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return f, ErrNoNBD
		}
		return f, fmt.Errorf("nbd: resolve the %q netlink family: %w", genlFamily, err)
	}
	return f, nil
}

// execute sends one nbd command and returns the kernel's replies.
func execute(ctx context.Context, cmd uint8, attrs []byte) ([]genetlink.Message, error) {
	c, err := dial(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = c.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = c.SetDeadline(time.Unix(1, 0)) })
	defer stop()
	f, err := family(c)
	if err = ctxErr(ctx, err); err != nil {
		return nil, err
	}
	msgs, err := c.Execute(genetlink.Message{
		Header: genetlink.Header{Command: cmd, Version: f.Version},
		Data:   attrs,
	}, f.ID, netlink.Request|netlink.Acknowledge)
	if err = ctxErr(ctx, err); err != nil {
		return nil, err
	}
	return msgs, nil
}

// ctxErr maps the result of a socket call whose only deadlines come from
// ctx. The socket's deadline is ctx's, and the two timers fire at the same
// instant in no fixed order, so a socket timeout may be seen while ctx.Err
// is still nil. Once ctx is done, or its deadline has passed, ctx's error is
// the answer, whatever the socket reported; otherwise err stands.
func ctxErr(ctx context.Context, err error) error {
	if cerr := ctx.Err(); cerr != nil {
		return cerr
	}
	if dl, ok := ctx.Deadline(); ok && !time.Now().Before(dl) {
		return context.DeadlineExceeded
	}
	return err
}

// bounded runs fn in its own goroutine and returns its result, or ctx's error
// once ctx ends. A call still inside the kernel then is abandoned, never
// joined; the channel is buffered so it can finish whenever it likes.
func bounded[T any](ctx context.Context, fn func(context.Context) (T, error)) (T, error) {
	type result struct {
		v   T
		err error
	}
	ch := make(chan result, 1)
	go func() {
		v, err := fn(ctx)
		ch <- result{v, err}
	}()
	select {
	case r := <-ch:
		return r.v, r.err
	case <-ctx.Done():
		select {
		case r := <-ch:
			return r.v, r.err
		default:
		}
		var zero T
		return zero, ctx.Err()
	}
}
