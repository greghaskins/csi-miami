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
	cfg := fenceStore(t)
	seedCache(t, cfg)
	k := &fakeKernel{devs: []nbd.Device{ourDevice(3, cfg.Size)}}
	k.cleanup(t)
	return cfg, k
}

// fenceStore is a store of four chunks, chunk i holding pattern(i+1), and a
// config for a cache of three that is not prefetched.
func fenceStore(t *testing.T) Config {
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
	return cfg
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

// servings records the servings Run starts, oldest first.
type servings struct {
	mu  sync.Mutex
	all []*serving
}

func recordServings(t *testing.T) *servings {
	t.Helper()
	r := &servings{}
	onServing = func(sv *serving) {
		r.mu.Lock()
		r.all = append(r.all, sv)
		r.mu.Unlock()
	}
	t.Cleanup(func() { onServing = nil })
	return r
}

// waitIdle waits for every request of the oldest serving to leave the backend.
func (r *servings) waitIdle(t *testing.T) {
	t.Helper()
	r.mu.Lock()
	old := r.all[0]
	r.mu.Unlock()
	deadline := time.Now().Add(10 * time.Second)
	for old.inflight.Count() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d requests of the old connection never left the backend", old.inflight.Count())
		}
		time.Sleep(time.Millisecond)
	}
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

// Q1: a write over two chunks whose second chunk's base fetch stalls blocks
// only that chunk. The resend waits for the stall, but a later write to the
// first chunk is served at once (a stuck chunk does not hold its neighbours),
// and when the stall ends the data is ordered: the first chunk holds the later
// write, the second the write's own second half.
func TestAStalledTwoChunkWriteBlocksOnlyItsStuckChunk(t *testing.T) {
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
	nbdSend(t, c, 1, 2, off, 8192, w1) // W1': its first half lands, its second waits
	time.Sleep(100 * time.Millisecond)
	nbdSend(t, c, 1, 3, off, 4096, w2) // a later write to the first chunk only
	nbdRecv(t, c, 3, 0)                // is not held up by the stuck second chunk
	nbdSilent(t, c, 200*time.Millisecond)
	release()
	nbdRecv(t, c, 2, 0)
	nbdSend(t, c, 0, 4, 0, 2*testChunk, nil)
	got := nbdRecv(t, c, 4, 2*testChunk)
	want := append(pattern(1, testChunk), pattern(2, testChunk)...)
	copy(want[off:], w1)
	copy(want[off:], w2)
	if !bytes.Equal(got, want) {
		t.Fatal("the chunks do not hold W2 over W1's first half and W1's second half")
	}
	select {
	case err := <-done:
		t.Fatalf("run exited: %v", err)
	default:
	}
	stopRun(t, cancel, done)
}

// The first request on a re-pointed socket confirms it even while a stuck write
// holds the chunk it reads. The stuck write goes to the first serving, which
// awaits no confirmation, so only the re-pointed socket's read can confirm.
func TestConfirmationDoesNotWaitForAStuckWritesChunk(t *testing.T) {
	cfg := fenceStore(t)
	k := &fakeKernel{connectIdx: 3, viaSocket: true}
	k.cleanup(t)
	entered, release := stallBase(t, 0)
	logs := &syncBuf{}
	log := slog.New(slog.NewTextHandler(logs, nil))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runOn(ctx, cfg, log, k) }()
	waitIndex(t, cfg.ControlSocket, 3)

	nbdSend(t, k.lastHeld(t), 1, 1, 4096, 4096, pattern(9, 4096)) // chunk 0, not resident
	waitEntered(t, entered)
	k.close()
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(logs.String(), "path=repoint") { // logged once the re-point is confirmed
		if time.Now().After(deadline) {
			t.Fatalf("the re-point was never confirmed while the write held chunk 0:\n%s", logs.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
	release()
	stopRun(t, cancel, done)
}

// Q3: an old write that reaches its chunk after the device was re-pointed,
// the resend and the guest's next write having run first, does nothing. W0
// holds chunk 2 (stuck in its base fetch); the old connection's W1 to the chunk
// is held back before taking its stripe; the device is re-pointed; the resend
// W1' and then W2 run after W0 releases; only then does the old W1 get the
// chunk. Whatever order the stripe is handed over in, it must find its
// connection dead and leave W2 alone.
func TestAnOldWriteThatTakesItsChunkAfterTheResendIsDropped(t *testing.T) {
	cfg, k := fenceConfig(t)
	entered, release := stallBase(t, 2)
	w1 := pattern(5, 4096)
	gate := make(chan struct{})
	atHook := make(chan struct{}, 1)
	var seen sync.Once
	beforeSegmentHook = func(_ uint32, src []byte) {
		if !bytes.Equal(src, w1) {
			return
		}
		first := false
		seen.Do(func() { first = true })
		if first { // the old W1: held back until the test lets it go
			atHook <- struct{}{}
			<-gate
		}
	}
	t.Cleanup(func() { beforeSegmentHook = nil })
	svs := recordServings(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runAsync(ctx, cfg, k)
	waitIndex(t, cfg.ControlSocket, 3)

	c0 := k.lastHeld(t)
	const base = 2 * testChunk
	w0 := pattern(3, 4096)
	nbdSend(t, c0, 1, 1, base, 4096, w0) // W0 stalls in its base fetch, holding chunk 2
	waitEntered(t, entered)
	nbdSend(t, c0, 1, 2, base+8192, 4096, w1) // the old W1, held before its stripe
	select {
	case <-atHook:
	case <-time.After(10 * time.Second):
		t.Fatal("the old W1 never reached the backend")
	}
	k.close()
	waitRepoint(t, k, cfg, done)
	c := k.lastHeld(t)
	nbdSend(t, c, 1, 3, base+8192, 4096, w1) // W1', the resend
	nbdSilent(t, c, 200*time.Millisecond)
	release() // W0 lets go; W1' takes the chunk
	nbdRecv(t, c, 3, 0)
	w2 := pattern(77, 4096)
	nbdSend(t, c, 1, 4, base+8192, 4096, w2) // W2, after W1' was acked
	nbdRecv(t, c, 4, 0)
	close(gate) // now the old W1 takes the chunk: its connection is dead
	svs.waitIdle(t)
	nbdSend(t, c, 0, 5, base, testChunk, nil)
	got := nbdRecv(t, c, 5, testChunk)
	want := pattern(3, testChunk)
	copy(want, w0)
	copy(want[8192:], w2)
	if !bytes.Equal(got, want) {
		t.Fatal("the old W1 landed after W2")
	}
	stopRun(t, cancel, done)
}

// Q3, multi-segment: an old write whose first segment held its chunk across the
// death finishes that segment ahead of its resend, and its second segment,
// reaching the next chunk only after the resend and the next write have run,
// is refused. W1 spans the end of chunk 2 and the start of chunk 3; chunk 2's
// base fetch stalls with W1 holding it; its segment for chunk 3 is held back
// until W1' and W2 are done.
func TestAnOldWritesLaterSegmentIsRefusedAfterTheResend(t *testing.T) {
	cfg, k := fenceConfig(t)
	entered, release := stallBase(t, 2)
	const off = 3*testChunk - 4096
	w1 := pattern(5, 8192)
	gate := make(chan struct{})
	atHook := make(chan struct{}, 1)
	var seen sync.Once
	beforeSegmentHook = func(i uint32, src []byte) {
		if i != 3 || !bytes.Equal(src, w1[4096:]) {
			return
		}
		first := false
		seen.Do(func() { first = true })
		if first { // the old W1's second segment: held back until the test lets it go
			atHook <- struct{}{}
			<-gate
		}
	}
	t.Cleanup(func() { beforeSegmentHook = nil })
	svs := recordServings(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runAsync(ctx, cfg, k)
	waitIndex(t, cfg.ControlSocket, 3)

	nbdSend(t, k.lastHeld(t), 1, 1, off, 8192, w1) // segment 0 stalls in chunk 2's base fetch
	waitEntered(t, entered)
	k.close()
	waitRepoint(t, k, cfg, done)
	c := k.lastHeld(t)
	nbdSend(t, c, 1, 2, off, 8192, w1) // W1', the resend, waits on chunk 2
	nbdSilent(t, c, 200*time.Millisecond)
	release() // the old segment 0 finishes ahead of W1'; its segment 1 is held back
	select {
	case <-atHook:
	case <-time.After(10 * time.Second):
		t.Fatal("the old W1 never reached its second segment")
	}
	nbdRecv(t, c, 2, 0)
	w2 := pattern(77, 4096)
	nbdSend(t, c, 1, 3, 3*testChunk, 4096, w2) // W2, to the second chunk, after W1' was acked
	nbdRecv(t, c, 3, 0)
	close(gate) // the old segment 1 takes chunk 3 now: its connection is dead
	svs.waitIdle(t)
	nbdSend(t, c, 0, 4, 2*testChunk, 2*testChunk, nil)
	got := nbdRecv(t, c, 4, 2*testChunk)
	want := append(pattern(3, testChunk), pattern(4, testChunk)...)
	copy(want[testChunk-4096:], w1[:4096])
	copy(want[testChunk:], w2)
	if !bytes.Equal(got, want) {
		t.Fatal("the old W1's second segment landed after W2")
	}
	stopRun(t, cancel, done)
}
