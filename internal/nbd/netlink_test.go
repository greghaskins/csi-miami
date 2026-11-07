package nbd

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/mdlayher/genetlink"
	"github.com/mdlayher/netlink"
	"golang.org/x/sys/unix"
)

func TestConnectAttrs(t *testing.T) {
	t.Parallel()
	o := ConnectOptions{
		Index: 3, Size: 64 << 20, BlockSize: 4096,
		Timeout: 30 * time.Second, DeadConnTimeout: 1500 * time.Millisecond,
		ServerFlags: FlagHasFlags | FlagSendFlush | FlagSendFUA | FlagSendTrim,
		Backend:     "pvc-00000000-0000-0000-0000-000000000001",
	}
	b, err := connectAttrs(o, 7)
	if err != nil {
		t.Fatal(err)
	}
	ad, err := netlink.NewAttributeDecoder(b)
	if err != nil {
		t.Fatal(err)
	}
	got := map[uint16]any{}
	for ad.Next() {
		switch ad.Type() {
		case attrIndex:
			got[attrIndex] = ad.Uint32()
		case attrBackendIdentifier:
			got[attrBackendIdentifier] = ad.String()
		case attrSockets:
			ad.Nested(func(nad *netlink.AttributeDecoder) error {
				for nad.Next() {
					if nad.Type() != sockItem {
						t.Errorf("socket list item type %d", nad.Type())
					}
					nad.Nested(func(sad *netlink.AttributeDecoder) error {
						for sad.Next() {
							if sad.Type() == sockFD {
								got[attrSockets] = sad.Uint32()
							}
						}
						return nil
					})
				}
				return nil
			})
		default:
			got[ad.Type()] = ad.Uint64()
		}
	}
	if err := ad.Err(); err != nil {
		t.Fatal(err)
	}
	want := map[uint16]any{
		attrIndex: uint32(3), attrSizeBytes: uint64(64 << 20), attrBlockSizeBytes: uint64(4096),
		attrTimeout: uint64(30), attrDeadConnTimeout: uint64(2), attrServerFlags: uint64(0x2d),
		attrBackendIdentifier: o.Backend, attrSockets: uint32(7),
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("attribute %d = %v, want %v", k, got[k], v)
		}
	}
	if len(got) != len(want) {
		t.Errorf("attributes %v", got)
	}

	// Index -1 allocates: no index attribute; no timeouts, no backend.
	b, err = connectAttrs(ConnectOptions{Index: -1, Size: 4096, BlockSize: 4096}, 7)
	if err != nil {
		t.Fatal(err)
	}
	ad, _ = netlink.NewAttributeDecoder(b)
	for ad.Next() {
		switch ad.Type() {
		case attrIndex, attrTimeout, attrDeadConnTimeout, attrBackendIdentifier:
			t.Errorf("unexpected attribute %d", ad.Type())
		}
	}
}

func TestReplyIndex(t *testing.T) {
	t.Parallel()
	ae := netlink.NewAttributeEncoder()
	ae.Uint32(attrIndex, 17)
	b, _ := ae.Encode()
	if i, err := replyIndex([]genetlink.Message{{Data: b}}); err != nil || i != 17 {
		t.Fatalf("index %d, %v", i, err)
	}
	if _, err := replyIndex(nil); err == nil {
		t.Fatal("no reply accepted")
	}
}

func socketpair(t *testing.T) (srv net.Conn, cli *os.File) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	sf := os.NewFile(uintptr(fds[0]), "nbd-server")
	srv, err = net.FileConn(sf)
	_ = sf.Close()
	if err != nil {
		t.Fatal(err)
	}
	cli = os.NewFile(uintptr(fds[1]), "nbd-client")
	t.Cleanup(func() { _ = srv.Close(); _ = cli.Close() })
	return srv, cli
}

// The handshake Connect runs on the client end of a socketpair against Serve.
func TestConnectHandshakeOnSocketpair(t *testing.T) {
	t.Parallel()
	srv, cli := socketpair(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = Serve(ctx, srv, newMem(testSize), defaultOpts) }()
	hctx, hcancel := context.WithTimeout(ctx, 5*time.Second)
	defer hcancel()
	e, err := handshakeFile(hctx, cli)
	if err != nil {
		t.Fatal(err)
	}
	if e.size != testSize || e.flags != FlagHasFlags|FlagSendFlush|FlagSendFUA|FlagSendTrim || e.blockSize != 4096 {
		t.Fatalf("export %+v", e)
	}
}

func TestConnectReturnsUnderDeadlineWhenServerIsSilent(t *testing.T) {
	t.Parallel()
	_, cli := socketpair(t) // nothing serves the other end
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := Connect(ctx, cli, ConnectOptions{Index: -1, Size: testSize, BlockSize: 4096})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Connect: %v, want DeadlineExceeded", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("Connect took %v", d)
	}
}

// earlyDeadline reports a deadline before the moment its Done channel
// closes, as a context does in the instant between its deadline passing and
// its timer firing; the socket deadline Connect sets from it fires first.
type earlyDeadline struct {
	context.Context
	at time.Time
}

func (e earlyDeadline) Deadline() (time.Time, bool) { return e.at, true }

func TestConnectReportsTheContextsDeadlineWhenTheSocketsFiresFirst(t *testing.T) {
	t.Parallel()
	_, cli := socketpair(t) // nothing serves the other end
	inner, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ctx := earlyDeadline{Context: inner, at: time.Now().Add(50 * time.Millisecond)}
	_, err := handshakeFile(ctx, cli)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("handshake: %v, want DeadlineExceeded", err)
	}
	if inner.Err() != nil {
		t.Fatal("the test did not provoke the race: the context ended before the socket's deadline")
	}
}

func TestCtxErrMapping(t *testing.T) {
	t.Parallel()
	other := errors.New("other")
	live := context.Background()
	done, cancel := context.WithCancel(context.Background())
	cancel()
	past, cancel2 := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel2()
	for _, c := range []struct {
		name string
		ctx  context.Context
		err  error
		want error
	}{
		{"live, other error", live, other, other},
		{"live, socket timeout", live, os.ErrDeadlineExceeded, os.ErrDeadlineExceeded},
		{"cancelled, other error", done, other, context.Canceled},
		{"cancelled, socket timeout", done, os.ErrDeadlineExceeded, context.Canceled},
		{"deadline passed, socket timeout", past, os.ErrDeadlineExceeded, context.DeadlineExceeded},
		{"deadline passed, success", past, nil, context.DeadlineExceeded},
		{"live, success", live, nil, nil},
	} {
		if got := ctxErr(c.ctx, c.err); !errors.Is(got, c.want) || (c.want == nil && got != nil) {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

func TestConnectRefusesAnExportSmallerThanTheDevice(t *testing.T) {
	t.Parallel()
	srv, cli := socketpair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = Serve(ctx, srv, newMem(testSize), defaultOpts) }()
	if _, err := Connect(ctx, cli, ConnectOptions{Index: -1, Size: 2 * testSize, BlockSize: 4096}); err == nil {
		t.Fatal("Connect accepted a device larger than its export")
	}
}

func TestNetlinkArgumentChecks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, o := range []ConnectOptions{
		{Size: 0, BlockSize: 4096},
		{Size: 4096, BlockSize: 1000},
		{Size: 6000, BlockSize: 4096},
	} {
		if _, err := Connect(ctx, nil, o); err == nil {
			t.Errorf("Connect accepted %+v", o)
		}
	}
	if err := Reconfigure(ctx, -1, 4096); err == nil {
		t.Error("Reconfigure accepted index -1")
	}
	if err := Disconnect(ctx, -1); err == nil {
		t.Error("Disconnect accepted index -1")
	}
}

func TestReconfigureAttrsNameTheBackend(t *testing.T) {
	t.Parallel()
	for _, backend := range []string{"", "pvc-00000000-0000-0000-0000-000000000001"} {
		b, err := reconfigureAttrs(4, 8192, backend)
		if err != nil {
			t.Fatal(err)
		}
		ad, err := netlink.NewAttributeDecoder(b)
		if err != nil {
			t.Fatal(err)
		}
		var index uint32
		var size uint64
		got := ""
		for ad.Next() {
			switch ad.Type() {
			case attrIndex:
				index = ad.Uint32()
			case attrSizeBytes:
				size = ad.Uint64()
			case attrBackendIdentifier:
				got = ad.String()
			default:
				t.Errorf("unexpected attribute %d", ad.Type())
			}
		}
		if index != 4 || size != 8192 || got != backend {
			t.Errorf("index %d, size %d, backend %q; want 4, 8192, %q", index, size, got, backend)
		}
	}
}

func TestSeconds(t *testing.T) {
	t.Parallel()
	for d, want := range map[time.Duration]uint64{0: 0, -time.Second: 0, time.Nanosecond: 1, time.Second: 1, 61 * time.Second / 2: 31} {
		if got := seconds(d); got != want {
			t.Errorf("seconds(%v) = %d, want %d", d, got, want)
		}
	}
}

func TestBoundedAbandonsAHungCall(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	release := make(chan struct{})
	defer close(release)
	_, err := bounded(ctx, func(context.Context) (int, error) { <-release; return 1, nil })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bounded: %v", err)
	}
}

// A reconnect names the device, its backend and the new socket, and nothing
// else: a size attribute that differs from the device's would notify a uevent.
func TestReconnectAttrsCarrySocketAndBackendButNoSize(t *testing.T) {
	t.Parallel()
	for _, backend := range []string{"", "pvc-00000000-0000-0000-0000-000000000001"} {
		b, err := reconnectAttrs(4, backend, 9)
		if err != nil {
			t.Fatal(err)
		}
		ad, err := netlink.NewAttributeDecoder(b)
		if err != nil {
			t.Fatal(err)
		}
		var index, fd uint32
		got := ""
		for ad.Next() {
			switch ad.Type() {
			case attrIndex:
				index = ad.Uint32()
			case attrBackendIdentifier:
				got = ad.String()
			case attrSockets:
				ad.Nested(func(nad *netlink.AttributeDecoder) error {
					for nad.Next() {
						if nad.Type() != sockItem {
							t.Errorf("socket list item type %d", nad.Type())
						}
						nad.Nested(func(sad *netlink.AttributeDecoder) error {
							for sad.Next() {
								if sad.Type() == sockFD {
									fd = sad.Uint32()
								}
							}
							return nil
						})
					}
					return nil
				})
			default:
				t.Errorf("unexpected attribute %d (a size or timeout must not be sent)", ad.Type())
			}
		}
		if err := ad.Err(); err != nil {
			t.Fatal(err)
		}
		if index != 4 || fd != 9 || got != backend {
			t.Errorf("index %d, fd %d, backend %q; want 4, 9, %q", index, fd, got, backend)
		}
	}
}

func TestReconnectSocketArgumentChecks(t *testing.T) {
	t.Parallel()
	if err := ReconnectSocket(context.Background(), -1, os.Stdin); err == nil {
		t.Error("a negative index was accepted")
	}
	if err := ReconnectSocket(context.Background(), 0, nil); err == nil {
		t.Error("a nil socket was accepted")
	}
}

func TestHandshakeRefusesAnExportSmallerThanWanted(t *testing.T) {
	t.Parallel()
	srv, cli := socketpair(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = Serve(ctx, srv, newMem(testSize), defaultOpts) }()
	hctx, hcancel := context.WithTimeout(ctx, 5*time.Second)
	defer hcancel()
	if err := Handshake(hctx, cli, testSize+4096); err == nil {
		t.Fatal("an export smaller than wanted was accepted")
	}
}
