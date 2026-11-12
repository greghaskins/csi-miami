package blockd

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/chunk"
	"github.com/greghaskins/csi-miami/internal/nbd"
)

// Decision 67: a request of a dead connection that is still in the backend when
// the server would re-point must not be allowed to write after the new
// connection's requests. Either it leaves the backend within fenceWait and the
// server re-points in process, or the server exits and the next process
// re-points. These tests cover the decision; the data outcome of the exit is
// proved by the smoke test, because only a dead process cannot write.

// stallBase makes the first write that needs the base of chunk 1 wait for
// release, signalling entered when it gets there. The hook is cleared on cleanup.
func stallBase(t *testing.T) (entered <-chan struct{}, release func()) {
	t.Helper()
	in := make(chan struct{})
	rel := make(chan struct{})
	var once, relOnce sync.Once
	baseFetchHook = func(i uint32) {
		if i != 1 { // chunk 0 is read by the re-point's confirmation, so is resident
			return
		}
		first := false
		once.Do(func() { first = true })
		if first {
			close(in)
			<-rel
		}
	}
	release = func() { relOnce.Do(func() { close(rel) }) }
	t.Cleanup(func() { release(); baseFetchHook = nil })
	return in, release
}

// fenceConfig is a left device whose chunk 1 has a non-zero base that is not in
// the cache, so a sub-chunk write to it fetches the base.
func fenceConfig(t *testing.T) (Config, *fakeKernel) {
	t.Helper()
	cfg, _ := testConfig(t)
	cfg.CacheBound = testChunk // less than the size: no prefetch makes the chunk resident
	store := openStore(t, cfg)
	h0 := putChunk(t, store, pattern(1, testChunk))
	h1 := putChunk(t, store, pattern(2, testChunk))
	writeCurrent(t, store, 1, 1, []chunk.Hash{h0, h1})
	seedCache(t, cfg)
	k := &fakeKernel{devs: []nbd.Device{ourDevice(3, testSize)}}
	k.cleanup(t)
	return cfg, k
}

func (k *fakeKernel) lastHeld(t *testing.T) net.Conn {
	t.Helper()
	k.mu.Lock()
	defer k.mu.Unlock()
	if len(k.held) == 0 {
		t.Fatal("the kernel holds no socket")
	}
	return k.held[len(k.held)-1]
}

// nbdSend writes one request (type 0 read, 1 write) to the server end of conn.
func nbdSend(t *testing.T, c net.Conn, typ uint16, cookie, off uint64, n uint32, payload []byte) {
	t.Helper()
	var h [28]byte
	binary.BigEndian.PutUint32(h[0:4], 0x25609513)
	binary.BigEndian.PutUint16(h[6:8], typ)
	binary.BigEndian.PutUint64(h[8:16], cookie)
	binary.BigEndian.PutUint64(h[16:24], off)
	binary.BigEndian.PutUint32(h[24:28], n)
	if _, err := c.Write(append(h[:], payload...)); err != nil {
		t.Fatalf("send request: %v", err)
	}
}

// nbdRecv reads one reply and the n bytes of data that follow a successful read.
func nbdRecv(t *testing.T, c net.Conn, n int) []byte {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	var h [16]byte
	if _, err := io.ReadFull(c, h[:]); err != nil {
		t.Fatalf("read reply: %v", err)
	}
	if e := binary.BigEndian.Uint32(h[4:8]); e != 0 {
		t.Fatalf("reply errno %d", e)
	}
	data := make([]byte, n)
	if _, err := io.ReadFull(c, data); err != nil {
		t.Fatalf("read reply data: %v", err)
	}
	return data
}

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func shortFence(t *testing.T, d time.Duration) {
	t.Helper()
	old := fenceWait
	fenceWait = d
	t.Cleanup(func() { fenceWait = old })
}

// P1a/P3: a request stuck in the backend when the connection dies turns the
// re-point into an exit, within fenceWait, without disconnecting the device.
func TestRunExitsInsteadOfRepointingWhileADeadConnectionsWriteIsInFlight(t *testing.T) {
	shortFence(t, 300*time.Millisecond)
	cfg, k := fenceConfig(t)
	entered, _ := stallBase(t)
	logs := &syncBuf{}
	log := slog.New(slog.NewTextHandler(logs, nil))
	done := make(chan error, 1)
	go func() { done <- runOn(context.Background(), cfg, log, k) }()
	waitIndex(t, cfg.ControlSocket, 3)

	nbdSend(t, k.lastHeld(t), 1, 1, testChunk+4096, 4096, pattern(9, 4096)) // W1, a partial chunk
	<-entered
	began := time.Now()
	k.close() // the kernel shuts the socket down
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("run returned nil with a write of the dead connection still in flight")
		}
		if took := time.Since(began); took > fenceWait+2*time.Second {
			t.Fatalf("run took %s to give up, bound is %s", took, fenceWait)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not exit while a write was stuck")
	}
	if _, r, d := k.counts(); len(d) != 0 || r != 1 {
		t.Fatalf("disconnects %v, reconnects %d: the device must be left alone and not re-pointed", d, r)
	}
	if !strings.Contains(logs.String(), "stuck=1") {
		t.Fatalf("the exit did not log the stuck request count:\n%s", logs.String())
	}
}

// P2: the stall ends inside the bound; the old write has landed before anything
// is re-pointed, so the new connection's W1' and W2 apply after it.
func TestRunRepointsInProcessWhenTheDeadConnectionsWriteFinishesInTime(t *testing.T) {
	shortFence(t, 5*time.Second)
	cfg, k := fenceConfig(t)
	entered, release := stallBase(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runAsync(ctx, cfg, k)
	waitIndex(t, cfg.ControlSocket, 3)

	w1 := pattern(9, 4096)
	nbdSend(t, k.lastHeld(t), 1, 1, testChunk+4096, 4096, w1)
	<-entered
	k.close()
	time.Sleep(100 * time.Millisecond) // the serve loop is dead, W1 is stuck
	release()

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
	waitIndex(t, cfg.ControlSocket, 3)
	c := k.lastHeld(t)
	w2 := pattern(77, 4096)
	nbdSend(t, c, 1, 2, testChunk+4096, 4096, w1) // W1', the kernel's resend
	nbdRecv(t, c, 0)
	nbdSend(t, c, 1, 3, testChunk+4096, 4096, w2) // W2
	nbdRecv(t, c, 0)
	nbdSend(t, c, 0, 4, testChunk, testChunk, nil)
	got := nbdRecv(t, c, testChunk)
	want := pattern(2, testChunk)
	copy(want[4096:], w2)
	if !bytes.Equal(got, want) {
		t.Fatal("chunk 1 does not hold W2 over its base")
	}
	select {
	case err := <-done:
		t.Fatalf("run returned %v", err)
	default:
	}
	stopRun(t, cancel, done)
	if _, _, d := k.counts(); len(d) != 0 {
		t.Fatalf("disconnected %v", d)
	}
}
