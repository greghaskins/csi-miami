package blockd

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/chunk"
	"github.com/greghaskins/csi-miami/internal/nbd"
)

// Decision 67: a request of a dead connection that is still in the backend when
// the server re-points in process must not write after the new connection's
// requests. A write holds the stripes of every chunk it touches from before its
// first segment to after its last, so the kernel's resend of it, and anything
// after that, queues behind it on the same stripes.

// stallBase makes the first write that needs the base of chunk n wait for
// release, signalling entered when it gets there. The hook is cleared on cleanup.
func stallBase(t *testing.T, n uint32) (entered <-chan struct{}, release func()) {
	t.Helper()
	in := make(chan struct{})
	rel := make(chan struct{})
	var once, relOnce sync.Once
	baseFetchHook = func(i uint32) {
		if i != n {
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

func waitEntered(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the write never reached its base fetch")
	}
}

// fenceConfig is a left device whose chunk 1 has a non-zero base that is not in
// the cache, so a sub-chunk write to it fetches the base.
func fenceConfig(t *testing.T) (Config, *fakeKernel) {
	t.Helper()
	cfg, _ := testConfig(t)
	cfg.Size = 4 * testChunk
	cfg.CacheBound = 3 * testChunk // less than the size: no prefetch makes the chunks resident
	store := openStore(t, cfg)
	hs := make([]chunk.Hash, 4)
	for i := range hs {
		hs[i] = putChunk(t, store, pattern(byte(i+1), testChunk))
	}
	writeCurrent(t, store, 1, 1, hs)
	seedCache(t, cfg)
	k := &fakeKernel{devs: []nbd.Device{ourDevice(3, cfg.Size)}}
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

// nbdRecv reads the reply to cookie and the n bytes of data that follow a
// successful read.
func nbdRecv(t *testing.T, c net.Conn, cookie uint64, n int) []byte {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	var h [16]byte
	if _, err := io.ReadFull(c, h[:]); err != nil {
		t.Fatalf("read reply: %v", err)
	}
	if got := binary.BigEndian.Uint64(h[8:16]); got != cookie {
		t.Fatalf("reply to request %d, want %d", got, cookie)
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

// nbdSilent fails if anything arrives on c within d.
func nbdSilent(t *testing.T, c net.Conn, d time.Duration) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(d))
	var b [1]byte
	if n, err := c.Read(b[:]); n != 0 || err == nil {
		t.Fatalf("a reply arrived while the stalled write held its chunks (%d bytes, %v)", n, err)
	}
}

// waitRepoint waits for the device to be re-pointed in process.
func waitRepoint(t *testing.T, k *fakeKernel, cfg Config, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, r, _ := k.counts(); r >= 2 {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("run returned %v instead of re-pointing", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("the device was never re-pointed")
		}
		time.Sleep(5 * time.Millisecond)
	}
	waitIndex(t, cfg.ControlSocket, 3)
}

// P2: the re-point does not wait for a stuck write. The kernel's resend of it
// queues behind it on its chunk, so it applies after the original, and the
// write after that applies last.
func TestRunRepointsInProcessWhileAWriteIsStuckAndOrdersTheResend(t *testing.T) {
	cfg, k := fenceConfig(t)
	entered, release := stallBase(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runAsync(ctx, cfg, k)
	waitIndex(t, cfg.ControlSocket, 3)

	w1 := pattern(9, 4096)
	nbdSend(t, k.lastHeld(t), 1, 1, testChunk+4096, 4096, w1)
	waitEntered(t, entered)
	k.close()
	waitRepoint(t, k, cfg, done) // with W1 still stuck in the store
	c := k.lastHeld(t)
	nbdSend(t, c, 1, 2, testChunk+4096, 4096, w1) // W1', the kernel's resend
	nbdSilent(t, c, 300*time.Millisecond)
	release()
	nbdRecv(t, c, 2, 0)
	w2 := pattern(77, 4096)
	nbdSend(t, c, 1, 3, testChunk+4096, 4096, w2) // W2
	nbdRecv(t, c, 3, 0)
	nbdSend(t, c, 0, 4, testChunk, testChunk, nil)
	got := nbdRecv(t, c, 4, testChunk)
	want := pattern(2, testChunk)
	copy(want[4096:], w2)
	if !bytes.Equal(got, want) {
		t.Fatal("chunk 1 does not hold W2 over its base")
	}
	stopRun(t, cancel, done)
	if _, _, d := k.counts(); len(d) != 0 {
		t.Fatalf("disconnected %v", d)
	}
}

// Q1: a write over two chunks whose second chunk's base fetch stalls holds the
// first chunk too, so the resend and a later write to the first chunk both wait
// for it. Without that, the later write would be acked while the original still
// had a stale write in front of it.
func TestAStalledTwoChunkWriteHoldsItsFirstChunkAgainstTheResendAndLaterWrites(t *testing.T) {
	cfg, k := fenceConfig(t)
	entered, release := stallBase(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runAsync(ctx, cfg, k)
	waitIndex(t, cfg.ControlSocket, 3)

	const off = testChunk - 4096 // the last block of chunk 0, then the first of chunk 1
	w1 := pattern(9, 8192)
	nbdSend(t, k.lastHeld(t), 1, 1, off, 8192, w1)
	waitEntered(t, entered)
	k.close()
	waitRepoint(t, k, cfg, done)
	c := k.lastHeld(t)
	w2 := pattern(77, 4096)
	nbdSend(t, c, 1, 2, off, 8192, w1) // W1'
	time.Sleep(100 * time.Millisecond) // W1' is queued first
	nbdSend(t, c, 1, 3, off, 4096, w2) // W2, to the first chunk only
	nbdSilent(t, c, 300*time.Millisecond)
	release()
	nbdRecv(t, c, 2, 0)
	nbdRecv(t, c, 3, 0)
	nbdSend(t, c, 0, 4, 0, 2*testChunk, nil)
	got := nbdRecv(t, c, 4, 2*testChunk)
	want := append(pattern(1, testChunk), pattern(2, testChunk)...)
	copy(want[off:], w1)
	copy(want[off:], w2)
	if !bytes.Equal(got, want) {
		t.Fatal("the chunks do not hold W1' then W2")
	}
	select {
	case err := <-done:
		t.Fatalf("run exited: %v", err)
	default:
	}
	stopRun(t, cancel, done)
}

// The first request on a re-pointed socket confirms it even while a stuck write
// holds chunk 0's stripe: the backend is entered before any stripe is taken.
func TestConfirmationDoesNotWaitForAStuckWritesChunk(t *testing.T) {
	cfg, k := fenceConfig(t)
	entered, release := stallBase(t, 0)
	nbdBlocked := make(chan struct{})
	// Chunk 0 is not resident until something reads it; the write below, to
	// the left device's first socket, stalls there before the confirmation.
	k.onTouch = func() { <-nbdBlocked }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runAsync(ctx, cfg, k)
	deadline := time.Now().Add(10 * time.Second)
	for {
		k.mu.Lock()
		n := len(k.held)
		k.mu.Unlock()
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no socket")
		}
		time.Sleep(5 * time.Millisecond)
	}
	nbdSend(t, k.lastHeld(t), 1, 1, 4096, 4096, pattern(9, 4096))
	waitEntered(t, entered) // the write holds chunk 0 and waits for its base
	close(nbdBlocked)
	waitIndex(t, cfg.ControlSocket, 3) // the touch read of chunk 0 still confirms
	release()
	stopRun(t, cancel, done)
}
