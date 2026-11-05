package blockd

import (
	"context"
	"errors"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/nbd"
)

// fakeKernel is the kernel's nbd as Run uses it. A reconnect "drops" its
// socket (the kernel's silent no-dead-slot success) for the first drops
// calls; touch plays the kernel resending a held request, which reaches the
// backend only once a reconnect has been kept.
type fakeKernel struct {
	mu           sync.Mutex
	devs         []nbd.Device
	connectIdx   int
	connects     int
	reconnects   int
	disconnects  []int
	drops        int
	reconnectErr error
	onTouch      func() // runs in touch before the request is sent
	resizes      []int64
	held         []net.Conn
}

// hold keeps a duplicate of sock, as the kernel keeps its own reference, so the
// serving end sees no EOF when Run closes its descriptor.
func (k *fakeKernel) hold(sock *os.File) error {
	c, err := net.FileConn(sock)
	if err != nil {
		return err
	}
	k.held = append(k.held, c)
	return nil
}

func (k *fakeKernel) close() {
	k.mu.Lock()
	defer k.mu.Unlock()
	for _, c := range k.held {
		_ = c.Close()
	}
}

func (k *fakeKernel) devices() ([]nbd.Device, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([]nbd.Device(nil), k.devs...), nil
}

func (k *fakeKernel) connect(ctx context.Context, cfg Config, sock *os.File) (int, error) {
	if err := nbd.Handshake(ctx, sock, cfg.Size); err != nil {
		return -1, err
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	k.connects++
	return k.connectIdx, k.hold(sock)
}

func (k *fakeKernel) reconnect(_ context.Context, _ int, sock *os.File) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.reconnects++
	if k.reconnectErr != nil {
		return k.reconnectErr
	}
	return k.hold(sock)
}

func (k *fakeKernel) disconnect(_ context.Context, idx int) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.disconnects = append(k.disconnects, idx)
	return nil
}

func (k *fakeKernel) resize(_ context.Context, _ int, size int64) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.resizes = append(k.resizes, size)
	return nil
}

func (k *fakeKernel) setReconnectErr(err error) {
	k.mu.Lock()
	k.reconnectErr = err
	k.mu.Unlock()
}

func (k *fakeKernel) touch(ctx context.Context, _ int, b nbd.Backend) error {
	if k.onTouch != nil {
		k.onTouch()
	}
	for {
		k.mu.Lock()
		kept := k.reconnects > k.drops
		k.mu.Unlock()
		if kept {
			_, err := b.ReadAt(make([]byte, 4096), 0)
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (k *fakeKernel) counts() (connects, reconnects int, disconnects []int) {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.connects, k.reconnects, append([]int(nil), k.disconnects...)
}

// shortConfirm shrinks the confirmation bounds so a failing confirmation
// takes milliseconds.
func shortConfirm(t *testing.T) {
	t.Helper()
	a, b := confirmAttempt, confirmBudget
	confirmAttempt, confirmBudget = 40*time.Millisecond, 250*time.Millisecond
	t.Cleanup(func() { confirmAttempt, confirmBudget = a, b })
}

func (k *fakeKernel) cleanup(t *testing.T) *fakeKernel {
	t.Helper()
	t.Cleanup(k.close)
	return k
}

// seedCache leaves a cache for cfg's volume behind, as a server that crashed
// would: a device in use is only ever served from one.
func seedCache(t *testing.T, cfg Config) {
	t.Helper()
	s, err := newServer(context.Background(), cfg, testLog())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
}

func ourDevice(idx int, size int64) nbd.Device {
	return nbd.Device{Index: idx, Pid: 4242, Backend: testVol, Size: size}
}

// runAsync starts run and returns its result channel.
func runAsync(ctx context.Context, cfg Config, k kernelAPI) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- runOn(ctx, cfg, testLog(), k) }()
	return ch
}

// waitIndex polls the control socket until Status reports want.
func waitIndex(t *testing.T, socket string, want int) Status {
	t.Helper()
	var last any
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		cli, err := Dial(ctx, socket)
		if err == nil {
			st, serr := cli.Status(ctx)
			_ = cli.Close()
			last = []any{st.Index, serr}
			if serr == nil && st.Index == want {
				cancel()
				return st
			}
		}
		cancel()
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("status never reported index %d (last %v)", want, last)
	return Status{}
}

func stopRun(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runOn: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return")
	}
}

func TestRunConnectsWhenNoDeviceMatches(t *testing.T) {
	cfg, _ := testConfig(t)
	k := &fakeKernel{connectIdx: 5, devs: []nbd.Device{
		{Index: 0, Pid: 0, Backend: testVol, Size: testSize}, // unconfigured
		ourDevice(1, testSize),
	}}
	k.cleanup(t)
	k.devs[1].Backend = "pvc-other"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runAsync(ctx, cfg, k)
	waitIndex(t, cfg.ControlSocket, 5)
	stopRun(t, cancel, done)
	c, r, d := k.counts()
	if c != 1 || r != 0 || len(d) != 0 {
		t.Fatalf("connects %d, reconnects %d, disconnects %v", c, r, d)
	}
}

func TestRunReconfiguresTheMatchingDevice(t *testing.T) {
	cfg, _ := testConfig(t)
	seedCache(t, cfg)
	k := &fakeKernel{connectIdx: 5, devs: []nbd.Device{ourDevice(3, testSize)}}
	k.cleanup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runAsync(ctx, cfg, k)
	st := waitIndex(t, cfg.ControlSocket, 3)
	if st.Size != testSize {
		t.Errorf("size %d, want %d", st.Size, testSize)
	}
	stopRun(t, cancel, done)
	c, r, d := k.counts()
	if c != 0 || r != 1 {
		t.Fatalf("connects %d, reconnects %d; want 0, 1", c, r)
	}
	// A signal leaves the device, its mapping and its mount for the next server.
	if len(d) != 0 {
		t.Fatalf("disconnects %v after a signal, want none", d)
	}
}

func TestRunReportsNoDeviceUntilConfirmed(t *testing.T) {
	cfg, _ := testConfig(t)
	seedCache(t, cfg)
	k := &fakeKernel{devs: []nbd.Device{ourDevice(3, testSize)}}
	k.cleanup(t)
	seen := make(chan int, 1)
	k.onTouch = func() {
		// The reconfigure has been issued but nothing has reached the
		// backend: the control service answers, and says -1.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		cli, err := Dial(ctx, cfg.ControlSocket)
		if err != nil {
			seen <- -2
			return
		}
		defer func() { _ = cli.Close() }()
		st, err := cli.Status(ctx)
		if err != nil {
			seen <- -3
			return
		}
		seen <- st.Index
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runAsync(ctx, cfg, k)
	if got := <-seen; got != -1 {
		t.Fatalf("status index before confirmation = %d, want -1", got)
	}
	waitIndex(t, cfg.ControlSocket, 3)
	stopRun(t, cancel, done)
}

func TestRunRetriesAReconfigureTheKernelDropped(t *testing.T) {
	shortConfirm(t)
	cfg, _ := testConfig(t)
	seedCache(t, cfg)
	k := &fakeKernel{devs: []nbd.Device{ourDevice(3, testSize)}, drops: 2}
	k.cleanup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runAsync(ctx, cfg, k)
	waitIndex(t, cfg.ControlSocket, 3)
	stopRun(t, cancel, done)
	if _, r, _ := k.counts(); r != 3 {
		t.Fatalf("reconnects %d, want 3 (two dropped, one kept)", r)
	}
}

func TestRunFailsWithoutDisconnectingWhenNeverConfirmed(t *testing.T) {
	shortConfirm(t)
	cfg, _ := testConfig(t)
	seedCache(t, cfg)
	k := &fakeKernel{devs: []nbd.Device{ourDevice(3, testSize)}, drops: 1 << 30}
	k.cleanup(t)
	err := runOn(context.Background(), cfg, testLog(), k)
	if err == nil {
		t.Fatal("run succeeded though the kernel never used the socket")
	}
	c, r, d := k.counts()
	if c != 0 || r < 2 {
		t.Errorf("connects %d, reconnects %d; want 0 and several", c, r)
	}
	if len(d) != 0 {
		t.Fatalf("disconnected %v: the device carries a live mount", d)
	}
}

func TestRunFailsWithoutDisconnectingWhenReconfigureErrors(t *testing.T) {
	shortConfirm(t)
	cfg, _ := testConfig(t)
	seedCache(t, cfg)
	k := &fakeKernel{devs: []nbd.Device{ourDevice(3, testSize)}, reconnectErr: errors.New("EINVAL")}
	k.cleanup(t)
	if err := runOn(context.Background(), cfg, testLog(), k); err == nil {
		t.Fatal("run succeeded though the reconfigure failed")
	}
	if _, _, d := k.counts(); len(d) != 0 {
		t.Fatalf("disconnected %v", d)
	}
}

func TestRunRefusesTwoMatchingDevices(t *testing.T) {
	cfg, _ := testConfig(t)
	k := &fakeKernel{devs: []nbd.Device{ourDevice(3, testSize), ourDevice(4, testSize)}}
	k.cleanup(t)
	if err := runOn(context.Background(), cfg, testLog(), k); err == nil {
		t.Fatal("run succeeded with two matching devices")
	}
	c, r, d := k.counts()
	if c != 0 || r != 0 || len(d) != 0 {
		t.Fatalf("touched the kernel: connects %d, reconnects %d, disconnects %v", c, r, d)
	}
	if _, err := os.Stat(cfg.CacheDir + "/index.json"); err == nil {
		t.Error("the cache was created before the refusal")
	}
}

func TestRunServesAGrownDevicesSize(t *testing.T) {
	cfg, _ := testConfig(t) // size 2 chunks
	seedCache(t, cfg)
	k := &fakeKernel{devs: []nbd.Device{ourDevice(3, 2*testSize)}}
	k.cleanup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runAsync(ctx, cfg, k)
	st := waitIndex(t, cfg.ControlSocket, 3)
	if st.Size != 2*testSize {
		t.Errorf("served size %d, want the device's %d", st.Size, 2*testSize)
	}
	stopRun(t, cancel, done)
}

// Arguments raised for a grow the device never got: finish it, once the
// device is confirmed to be ours.
func TestRunFinishesAnInterruptedGrow(t *testing.T) {
	cfg, _ := testConfig(t)
	seedCache(t, cfg)
	cfg.Size = 2 * testSize
	k := &fakeKernel{devs: []nbd.Device{ourDevice(3, testSize)}}
	k.cleanup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runAsync(ctx, cfg, k)
	st := waitIndex(t, cfg.ControlSocket, 3)
	if st.Size != 2*testSize {
		t.Errorf("served size %d, want %d", st.Size, 2*testSize)
	}
	stopRun(t, cancel, done)
	k.mu.Lock()
	defer k.mu.Unlock()
	if len(k.resizes) != 1 || k.resizes[0] != 2*testSize {
		t.Fatalf("resizes %v, want [%d]", k.resizes, 2*testSize)
	}
}

// A cache index larger than both the arguments and the device (a grow cut off
// by an older crash shape) is served at the index's size, and the device is
// brought up to it.
func TestRunServesTheLargestOfArgsDeviceAndIndex(t *testing.T) {
	cfg, _ := testConfig(t)
	big := cfg
	big.Size = 2 * testSize
	seedCache(t, big)
	k := &fakeKernel{devs: []nbd.Device{ourDevice(3, testSize)}}
	k.cleanup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runAsync(ctx, cfg, k) // args and device are both the smaller size
	st := waitIndex(t, cfg.ControlSocket, 3)
	if st.Size != 2*testSize {
		t.Errorf("served size %d, want %d", st.Size, 2*testSize)
	}
	stopRun(t, cancel, done)
	if len(k.resizes) != 1 || k.resizes[0] != 2*testSize {
		t.Fatalf("resizes %v", k.resizes)
	}
}

func TestRunDoesNotResizeAnEqualDevice(t *testing.T) {
	cfg, _ := testConfig(t)
	seedCache(t, cfg)
	k := &fakeKernel{devs: []nbd.Device{ourDevice(3, testSize)}}
	k.cleanup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runAsync(ctx, cfg, k)
	waitIndex(t, cfg.ControlSocket, 3)
	stopRun(t, cancel, done)
	if len(k.resizes) != 0 {
		t.Fatalf("resizes %v, want none", k.resizes)
	}
}

func TestRunRefusesADeviceThatIsNotWholeChunks(t *testing.T) {
	cfg, _ := testConfig(t)
	seedCache(t, cfg)
	k := &fakeKernel{devs: []nbd.Device{ourDevice(3, testSize+4096)}}
	if err := runOn(context.Background(), cfg, testLog(), k); err == nil {
		t.Fatal("run accepted a device that is not whole chunks")
	}
	if _, r, d := k.counts(); r != 0 || len(d) != 0 {
		t.Fatalf("touched the device: reconnects %d, disconnects %v", r, d)
	}
}

func TestAdoptDeviceSize(t *testing.T) {
	cfg, _ := testConfig(t)
	for _, tc := range []struct {
		name string
		dev  int64
		want int64
		bad  bool
	}{
		{"equal", testSize, testSize, false},
		{"grown", 3 * testSize, 3 * testSize, false},
		{"smaller keeps the arguments", testSize - testChunk, testSize, false},
		{"not a whole chunk", testSize + 4096, 0, true},
	} {
		got, err := adoptDeviceSize(cfg, nbd.Device{Index: 1, Size: tc.dev})
		if tc.bad {
			if err == nil {
				t.Errorf("%s: accepted", tc.name)
			}
			continue
		}
		if err != nil || got.Size != tc.want {
			t.Errorf("%s: size %d, err %v; want %d", tc.name, got.Size, err, tc.want)
		}
	}
}

// Critical 1: the kernel shutting our socket down (a request timed out) is
// answered by re-pointing the device in process, not by disconnecting it.
func TestRunRepointsAfterTheServeLoopDies(t *testing.T) {
	cfg, _ := testConfig(t)
	k := &fakeKernel{devs: []nbd.Device{ourDevice(3, testSize)}}
	seedCache(t, cfg)
	k.cleanup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runAsync(ctx, cfg, k)
	waitIndex(t, cfg.ControlSocket, 3)
	k.close() // the kernel's kernel_sock_shutdown on our socket
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, r, _ := k.counts(); r >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the device was never re-pointed")
		}
		time.Sleep(5 * time.Millisecond)
	}
	select {
	case err := <-done:
		t.Fatalf("run returned %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	st := waitIndex(t, cfg.ControlSocket, 3) // Index never changed
	_ = st
	stopRun(t, cancel, done)
	if _, _, d := k.counts(); len(d) != 0 {
		t.Fatalf("disconnected %v", d)
	}
}

func TestRunExitsNonZeroWithoutDisconnectingWhenRepointFails(t *testing.T) {
	shortConfirm(t)
	cfg, _ := testConfig(t)
	seedCache(t, cfg)
	k := &fakeKernel{devs: []nbd.Device{ourDevice(3, testSize)}}
	k.cleanup(t)
	done := runAsync(context.Background(), cfg, k)
	waitIndex(t, cfg.ControlSocket, 3)
	k.setReconnectErr(errors.New("EINVAL"))
	k.close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("run exited zero though it could not take the device back")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return")
	}
	if _, _, d := k.counts(); len(d) != 0 {
		t.Fatalf("disconnected %v, which still carries the mount", d)
	}
}

func TestRunDisconnectsOnlyOnStopRequest(t *testing.T) {
	cfg, _ := testConfig(t)
	k := &fakeKernel{connectIdx: 5}
	k.cleanup(t)
	done := runAsync(context.Background(), cfg, k)
	waitIndex(t, cfg.ControlSocket, 5)
	dctx, dcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dcancel()
	cli, err := Dial(dctx, cfg.ControlSocket)
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Stop(dctx); err != nil {
		t.Fatal(err)
	}
	_ = cli.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, _, d := k.counts(); len(d) != 1 || d[0] != 5 {
		t.Fatalf("disconnects %v, want [5]", d)
	}
}

// Important 3: a device in use is never re-pointed at a cache built from the
// store.
func TestRunRefusesALeftDeviceWithNoCacheIndex(t *testing.T) {
	cfg, _ := testConfig(t) // no cache yet
	k := &fakeKernel{devs: []nbd.Device{ourDevice(3, testSize)}}
	err := runOn(context.Background(), cfg, testLog(), k)
	if err == nil {
		t.Fatal("a left device with no cache index was re-pointed at a cache built from the store")
	}
	if _, err := os.Stat(cfg.CacheDir + "/index.json"); err == nil {
		t.Error("a cache index was created")
	}
	if _, r, d := k.counts(); r != 0 || len(d) != 0 {
		t.Fatalf("touched the device: reconnects %d, disconnects %v", r, d)
	}
}

func TestRunRefusesALeftDeviceWhoseCacheGeometryDiffers(t *testing.T) {
	cfg, _ := testConfig(t)
	other := cfg
	other.Chunk = 2 * testChunk
	other.CacheBound = testBound
	seedCache(t, other)
	k := &fakeKernel{devs: []nbd.Device{ourDevice(3, testSize)}}
	if err := runOn(context.Background(), cfg, testLog(), k); err == nil {
		t.Fatal("served a device from a cache of another geometry")
	}
	if _, r, d := k.counts(); r != 0 || len(d) != 0 {
		t.Fatalf("touched the device: reconnects %d, disconnects %v", r, d)
	}
}

// O1: a second server on the same cache directory is refused before it looks
// at the cache or the device.
func TestRunRefusesACacheHeldByAnotherServer(t *testing.T) {
	cfg, _ := testConfig(t)
	seedCache(t, cfg)
	held, err := lockCache(cfg.CacheDir)
	if err != nil {
		t.Fatal(err)
	}
	k := &fakeKernel{devs: []nbd.Device{ourDevice(3, testSize)}}
	if err := runOn(context.Background(), cfg, testLog(), k); err == nil {
		t.Fatal("run started on a locked cache")
	}
	if c, r, d := k.counts(); c != 0 || r != 0 || len(d) != 0 {
		t.Fatalf("touched the kernel: %d %d %v", c, r, d)
	}
	held.release(nil)
	k.cleanup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runAsync(ctx, cfg, k)
	waitIndex(t, cfg.ControlSocket, 3)
	stopRun(t, cancel, done)
}

func TestLockCacheIsExclusiveUntilReleased(t *testing.T) {
	dir := t.TempDir()
	a, err := lockCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockCache(dir); err == nil {
		t.Fatal("a second lock on the same directory succeeded")
	}
	a.release(nil)
	b, err := lockCache(dir)
	if err != nil {
		t.Fatalf("lock after release: %v", err)
	}
	b.release(nil)
}

// The lock goes last: by then the serve loop has returned, the control service
// is closed and the cache is closed, on every way out.
func TestLockIsReleasedAfterServeAndCacheAreDone(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(t *testing.T) error
	}{
		{"signal", func(t *testing.T) error {
			cfg, _ := testConfig(t)
			seedCache(t, cfg)
			k := &fakeKernel{devs: []nbd.Device{ourDevice(3, testSize)}}
			k.cleanup(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := runAsync(ctx, cfg, k)
			waitIndex(t, cfg.ControlSocket, 3)
			cancel()
			return <-done
		}},
		{"never confirmed", func(t *testing.T) error {
			shortConfirm(t)
			cfg, _ := testConfig(t)
			seedCache(t, cfg)
			k := &fakeKernel{devs: []nbd.Device{ourDevice(3, testSize)}, drops: 1 << 30}
			k.cleanup(t)
			return runOn(context.Background(), cfg, testLog(), k)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls int
			var closedErr error
			var ctlGone bool
			onUnlock = func(s *server) {
				calls++
				if s == nil {
					return
				}
				closedErr = s.back.cache.Grow(s.back.Size()) // ErrClosed once closed
				c, err := net.Dial("unix", s.cfg.ControlSocket)
				if err == nil {
					_ = c.Close()
				}
				ctlGone = err != nil
			}
			t.Cleanup(func() { onUnlock = nil })
			_ = tc.run(t)
			if calls != 1 {
				t.Fatalf("lock released %d times", calls)
			}
			if !errors.Is(closedErr, os.ErrClosed) {
				t.Errorf("the cache was still open when the lock was released (%v)", closedErr)
			}
			if !ctlGone {
				t.Error("the control service was still up when the lock was released")
			}
		})
	}
}

// Important 2: the kernel grows first, so a failure leaves the cache as it was.
func TestGrowReconfiguresTheKernelBeforeTheCache(t *testing.T) {
	cfg, _ := testConfig(t)
	s := newTestServer(t, cfg)
	s.setDevice(&fakeDevice{idx: 7, reconfigErr: errors.New("netlink failed")})
	if err := s.growTo(context.Background(), 2*testSize); err == nil {
		t.Fatal("grow succeeded though the kernel refused")
	}
	if s.back.Size() != testSize || s.back.cache.Index().Size != testSize {
		t.Fatalf("the cache grew to %d (index %d) though the kernel did not", s.back.Size(), s.back.cache.Index().Size)
	}
}

// A cache index larger than the arguments is adopted, not refused.
func TestResumeAdoptsALargerIndex(t *testing.T) {
	cfg, _ := testConfig(t)
	big := cfg
	big.Size = 2 * testSize
	seedCache(t, big)
	s, err := newServer(context.Background(), cfg, testLog())
	if err != nil {
		t.Fatalf("restart refused after a grow the arguments predate: %v", err)
	}
	defer func() { _ = s.close() }()
	if s.back.Size() != 2*testSize || s.cfg.Size != 2*testSize {
		t.Fatalf("size %d, want %d", s.back.Size(), 2*testSize)
	}
}

func TestGrowRefusedBeforeTheDeviceIsServed(t *testing.T) {
	cfg, _ := testConfig(t)
	s, err := newServer(context.Background(), cfg, testLog())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.close() })
	if err := s.growTo(context.Background(), 2*testSize); err == nil {
		t.Fatal("grow accepted before a device was confirmed")
	}
	if s.back.Size() != testSize {
		t.Fatalf("cache size changed to %d", s.back.Size())
	}
}
