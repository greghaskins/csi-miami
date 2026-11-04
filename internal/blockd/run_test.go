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
	if c != 1 || r != 0 || len(d) != 1 || d[0] != 5 {
		t.Fatalf("connects %d, reconnects %d, disconnects %v", c, r, d)
	}
}

func TestRunReconfiguresTheMatchingDevice(t *testing.T) {
	cfg, _ := testConfig(t)
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
	// A stop on request or a signal is an unstage: it disconnects.
	if len(d) != 1 || d[0] != 3 {
		t.Fatalf("disconnects %v, want [3]", d)
	}
}

func TestRunReportsNoDeviceUntilConfirmed(t *testing.T) {
	cfg, _ := testConfig(t)
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
	if _, err := os.Stat(cfg.CacheDir); err == nil {
		t.Error("the cache was created before the refusal")
	}
}

func TestRunServesAGrownDevicesSize(t *testing.T) {
	cfg, _ := testConfig(t) // size 2 chunks
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

func TestRunRefusesAShrunkDevice(t *testing.T) {
	cfg, _ := testConfig(t)
	k := &fakeKernel{devs: []nbd.Device{ourDevice(3, testSize/2)}}
	k.cleanup(t)
	if err := runOn(context.Background(), cfg, testLog(), k); err == nil {
		t.Fatal("run accepted a device smaller than the configured size")
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
		{"smaller", testSize - testChunk, 0, true},
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
