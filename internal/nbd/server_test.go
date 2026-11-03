package nbd

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

const testSize = 1 << 20

var defaultOpts = ServeOptions{BlockSize: 4096, MaxRequest: 32 << 20}

// served is a Serve running on one end of a net.Pipe; c is the other end.
type served struct {
	c      net.Conn
	cancel context.CancelFunc
	done   chan struct{} // closed when Serve returns, err set
	err    error
}

func startServe(t testing.TB, b Backend, o ServeOptions) *served {
	t.Helper()
	srv, cli := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	s := &served{c: cli, cancel: cancel, done: make(chan struct{})}
	go func() { s.err = Serve(ctx, srv, b, o); close(s.done) }()
	t.Cleanup(func() {
		cancel()
		_ = cli.Close()
		select {
		case <-s.done:
		case <-time.After(5 * time.Second):
			t.Error("Serve did not return after cancel")
		}
	})
	_ = cli.SetDeadline(time.Now().Add(10 * time.Second))
	return s
}

// negotiated is startServe plus the client handshake.
func negotiated(t testing.TB, b Backend, o ServeOptions) *served {
	t.Helper()
	s := startServe(t, b, o)
	if _, err := clientHandshake(s.c); err != nil {
		t.Fatalf("handshake: %v", err)
	}
	return s
}

func (s *served) send(t testing.TB, typ, flags uint16, cookie, off uint64, n uint32, payload []byte) {
	t.Helper()
	var h [requestLen]byte
	putRequest(&h, request{flags: flags, typ: typ, cookie: cookie, offset: off, length: n})
	if _, err := s.c.Write(h[:]); err != nil {
		t.Fatalf("write request: %v", err)
	}
	if payload != nil {
		if _, err := s.c.Write(payload); err != nil {
			t.Fatalf("write payload: %v", err)
		}
	}
}

// reply reads one simple reply, and dataLen bytes of data if it succeeded.
func (s *served) reply(t testing.TB, dataLen int) (errno uint32, cookie uint64, data []byte) {
	t.Helper()
	var h [replyLen]byte
	if _, err := io.ReadFull(s.c, h[:]); err != nil {
		t.Fatalf("read reply: %v", err)
	}
	if m := binary.BigEndian.Uint32(h[0:4]); m != magicReplySimp {
		t.Fatalf("reply magic %#x", m)
	}
	errno = binary.BigEndian.Uint32(h[4:8])
	cookie = binary.BigEndian.Uint64(h[8:16])
	if errno == 0 && dataLen > 0 {
		data = make([]byte, dataLen)
		if _, err := io.ReadFull(s.c, data); err != nil {
			t.Fatalf("read data: %v", err)
		}
	}
	return errno, cookie, data
}

func (s *served) do(t testing.TB, typ, flags uint16, off uint64, n uint32, payload []byte) (uint32, []byte) {
	t.Helper()
	s.send(t, typ, flags, 42, off, n, payload)
	dataLen := 0
	if typ == cmdRead {
		dataLen = int(n)
	}
	errno, cookie, data := s.reply(t, dataLen)
	if cookie != 42 {
		t.Fatalf("cookie %d, want 42", cookie)
	}
	return errno, data
}

func readFull(t *testing.T, r io.Reader, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		t.Fatalf("read %d bytes: %v", n, err)
	}
	return b
}

func writeOpt(t *testing.T, w io.Writer, opt uint32, data []byte) {
	t.Helper()
	b := make([]byte, 16+len(data))
	binary.BigEndian.PutUint64(b[0:8], magicOpt)
	binary.BigEndian.PutUint32(b[8:12], opt)
	binary.BigEndian.PutUint32(b[12:16], uint32(len(data))) //nolint:gosec // small test values
	copy(b[16:], data)
	if _, err := w.Write(b); err != nil {
		t.Fatal(err)
	}
}

// readOptReply reads one option reply and checks its option and type.
func readOptReply(t *testing.T, r io.Reader, opt, typ uint32) []byte {
	t.Helper()
	h := readFull(t, r, 20)
	if m := binary.BigEndian.Uint64(h[0:8]); m != magicOptReply {
		t.Fatalf("option reply magic %#x", m)
	}
	if o := binary.BigEndian.Uint32(h[8:12]); o != opt {
		t.Fatalf("reply for option %d, want %d", o, opt)
	}
	if ty := binary.BigEndian.Uint32(h[12:16]); ty != typ {
		t.Fatalf("reply type %#x, want %#x", ty, typ)
	}
	return readFull(t, r, int(binary.BigEndian.Uint32(h[16:20])))
}

func infoData(name string, infos ...uint16) []byte {
	b := binary.BigEndian.AppendUint32(nil, uint32(len(name))) //nolint:gosec // small test values
	b = append(b, name...)
	b = binary.BigEndian.AppendUint16(b, uint16(len(infos))) //nolint:gosec // small test values
	for _, i := range infos {
		b = binary.BigEndian.AppendUint16(b, i)
	}
	return b
}

func checkInfos(t *testing.T, r io.Reader, opt uint32, wantFlags uint16) {
	t.Helper()
	ex := readOptReply(t, r, opt, repInfo)
	if len(ex) != 12 || binary.BigEndian.Uint16(ex[0:2]) != infoExport {
		t.Fatalf("export info %x", ex)
	}
	if got := binary.BigEndian.Uint64(ex[2:10]); got != testSize {
		t.Errorf("export size %d, want %d", got, testSize)
	}
	if got := binary.BigEndian.Uint16(ex[10:12]); got != wantFlags {
		t.Errorf("transmission flags %#x, want %#x", got, wantFlags)
	}
	bs := readOptReply(t, r, opt, repInfo)
	if len(bs) != 14 || binary.BigEndian.Uint16(bs[0:2]) != infoBlockSize {
		t.Fatalf("block size info %x", bs)
	}
	minB, pref, maxB := binary.BigEndian.Uint32(bs[2:6]), binary.BigEndian.Uint32(bs[6:10]), binary.BigEndian.Uint32(bs[10:14])
	if minB != 4096 || pref != 4096 || maxB != 32<<20 {
		t.Errorf("block sizes %d/%d/%d, want 4096/4096/%d", minB, pref, maxB, 32<<20)
	}
	readOptReply(t, r, opt, repAck)
}

func TestHandshakeFixedNewstyle(t *testing.T) {
	t.Parallel()
	s := startServe(t, newMem(testSize), defaultOpts)
	hello := readFull(t, s.c, 18)
	if binary.BigEndian.Uint64(hello[0:8]) != magicNBD || binary.BigEndian.Uint64(hello[8:16]) != magicOpt {
		t.Fatalf("greeting %x", hello)
	}
	if hf := binary.BigEndian.Uint16(hello[16:18]); hf != flagFixedNewstyle|flagNoZeroes {
		t.Fatalf("handshake flags %#x", hf)
	}
	if _, err := s.c.Write([]byte{0, 0, 0, cflagFixedNewstyle | cflagNoZeroes}); err != nil {
		t.Fatal(err)
	}

	writeOpt(t, s.c, optStructuredReply, nil)
	readOptReply(t, s.c, optStructuredReply, repErrUnsup)

	writeOpt(t, s.c, optInfo, []byte{1, 2})
	readOptReply(t, s.c, optInfo, repErrInvalid)

	want := uint16(FlagHasFlags | FlagSendFlush | FlagSendFUA | FlagSendTrim)
	writeOpt(t, s.c, optInfo, infoData("", infoBlockSize))
	checkInfos(t, s.c, optInfo, want)

	writeOpt(t, s.c, optGo, infoData("pvc-00000000-0000-0000-0000-000000000001", infoBlockSize))
	checkInfos(t, s.c, optGo, want)

	// Transmission: the next bytes are a reply, no zero padding before it.
	if errno, _ := s.do(t, cmdFlush, 0, 0, 0, nil); errno != 0 {
		t.Fatalf("flush errno %d", errno)
	}
}

func TestHandshakeAbortEndsServe(t *testing.T) {
	t.Parallel()
	s := startServe(t, newMem(testSize), defaultOpts)
	readFull(t, s.c, 18)
	if _, err := s.c.Write([]byte{0, 0, 0, cflagFixedNewstyle | cflagNoZeroes}); err != nil {
		t.Fatal(err)
	}
	writeOpt(t, s.c, optAbort, nil)
	readOptReply(t, s.c, optAbort, repAck)
	select {
	case <-s.done:
		if s.err != nil {
			t.Fatalf("Serve after abort: %v", s.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after NBD_OPT_ABORT")
	}
}

func TestHandshakeReadOnlyFlag(t *testing.T) {
	t.Parallel()
	o := defaultOpts
	o.ReadOnly = true
	s := startServe(t, newMem(testSize), o)
	e, err := clientHandshake(s.c)
	if err != nil {
		t.Fatal(err)
	}
	if e.flags&FlagReadOnly == 0 || e.size != testSize || e.blockSize != 4096 || e.maxReq != 32<<20 {
		t.Fatalf("export %+v", e)
	}
	if errno, _ := s.do(t, cmdWrite, 0, 0, 4096, make([]byte, 4096)); errno != errPerm {
		t.Fatalf("write to read-only export: errno %d, want EPERM", errno)
	}
	if errno, _ := s.do(t, cmdTrim, 0, 0, 4096, nil); errno != errPerm {
		t.Fatalf("trim of read-only export: errno %d, want EPERM", errno)
	}
	if errno, _ := s.do(t, cmdRead, 0, 0, 4096, nil); errno != 0 {
		t.Fatalf("read of read-only export: errno %d", errno)
	}
}

func TestReadWriteFlushTrim(t *testing.T) {
	t.Parallel()
	m := newMem(testSize)
	s := negotiated(t, m, defaultOpts)

	pattern := bytes.Repeat([]byte("miami-nbd"), 8192/9+1)[:8192]
	if errno, _ := s.do(t, cmdWrite, 0, 4096, 8192, pattern); errno != 0 {
		t.Fatalf("write errno %d", errno)
	}
	errno, got := s.do(t, cmdRead, 0, 4096, 8192, nil)
	if errno != 0 || !bytes.Equal(got, pattern) {
		t.Fatalf("read back errno %d, equal %v", errno, bytes.Equal(got, pattern))
	}
	if errno, _ := s.do(t, cmdFlush, 0, 0, 0, nil); errno != 0 || m.flushes != 1 {
		t.Fatalf("flush errno %d, backend flushes %d", errno, m.flushes)
	}
	if errno, _ := s.do(t, cmdTrim, 0, 4096, 4096, nil); errno != 0 {
		t.Fatalf("trim errno %d", errno)
	}
	_, got = s.do(t, cmdRead, 0, 4096, 8192, nil)
	if !bytes.Equal(got[:4096], make([]byte, 4096)) || !bytes.Equal(got[4096:], pattern[4096:]) {
		t.Fatal("trim did not zero exactly its range")
	}

	// Out of range: a read past the end is EINVAL, a write ENOSPC, a trim
	// EINVAL, and none reaches the backend.
	reads, writes := m.reads, m.writes
	if errno, _ := s.do(t, cmdRead, 0, testSize-4096, 8192, nil); errno != errInval {
		t.Fatalf("read past end errno %d", errno)
	}
	if errno, _ := s.do(t, cmdWrite, 0, testSize, 4096, make([]byte, 4096)); errno != errNoSpc {
		t.Fatalf("write past end errno %d", errno)
	}
	if errno, _ := s.do(t, cmdTrim, 0, 1<<62, 4096, nil); errno != errInval {
		t.Fatalf("trim past end errno %d", errno)
	}
	if m.reads != reads || m.writes != writes {
		t.Fatal("an out-of-range request reached the backend")
	}
}

func TestFUAWrite(t *testing.T) {
	t.Parallel()
	m := newMem(testSize)
	s := negotiated(t, m, defaultOpts)
	if errno, _ := s.do(t, cmdWrite, 0, 0, 4096, make([]byte, 4096)); errno != 0 || m.fuaWrites != 0 {
		t.Fatalf("plain write: errno %d, fua writes %d", errno, m.fuaWrites)
	}
	if errno, _ := s.do(t, cmdWrite, cmdFlagFUA, 0, 4096, make([]byte, 4096)); errno != 0 || m.fuaWrites != 1 {
		t.Fatalf("FUA write: errno %d, fua writes %d", errno, m.fuaWrites)
	}
}

func TestOversizedRequestIsRejectedNotServed(t *testing.T) {
	t.Parallel()
	m := newMem(testSize)
	o := defaultOpts
	o.MaxRequest = 64 << 10
	s := negotiated(t, m, o)

	// The payload must still be consumed, or the stream loses its framing.
	big := bytes.Repeat([]byte{0xab}, 128<<10)
	if errno, _ := s.do(t, cmdWrite, 0, 0, uint32(len(big)), big); errno != errInval { //nolint:gosec // small test values
		t.Fatalf("oversized write errno %d, want EINVAL", errno)
	}
	if errno, data := s.do(t, cmdRead, 0, 0, 128<<10, nil); errno != errInval || data != nil {
		t.Fatalf("oversized read errno %d, %d bytes", errno, len(data))
	}
	if m.writes != 0 || m.reads != 0 {
		t.Fatalf("backend saw %d writes, %d reads", m.writes, m.reads)
	}
	// The stream is still in step.
	if errno, got := s.do(t, cmdRead, 0, 0, 4096, nil); errno != 0 || !bytes.Equal(got, make([]byte, 4096)) {
		t.Fatalf("read after oversized: errno %d", errno)
	}
}

func TestUnknownCommandIsEINVAL(t *testing.T) {
	t.Parallel()
	s := negotiated(t, newMem(testSize), defaultOpts)
	if errno, _ := s.do(t, 6 /* WRITE_ZEROES, not advertised */, 0, 0, 4096, nil); errno != errInval {
		t.Fatalf("errno %d, want EINVAL", errno)
	}
	if errno, _ := s.do(t, cmdFlush, 0, 0, 0, nil); errno != 0 {
		t.Fatalf("flush after unknown: errno %d", errno)
	}
}

func TestDiscEndsServe(t *testing.T) {
	t.Parallel()
	s := negotiated(t, newMem(testSize), defaultOpts)
	s.send(t, cmdDisc, 0, 1, 0, 0, nil)
	select {
	case <-s.done:
		if s.err != nil {
			t.Fatalf("Serve after DISC: %v", s.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after NBD_CMD_DISC")
	}
}

func TestBackendErrorMapsToEIO(t *testing.T) {
	t.Parallel()
	m := newMem(testSize)
	m.failRead[8192] = true
	m.failWrite[4096] = true
	s := negotiated(t, m, defaultOpts)
	if errno, data := s.do(t, cmdRead, 0, 8192, 4096, nil); errno != errIO || data != nil {
		t.Fatalf("failing read: errno %d", errno)
	}
	if errno, _ := s.do(t, cmdRead, 0, 8192, 4096, nil); errno != 0 {
		t.Fatalf("next read: errno %d", errno)
	}
	if errno, _ := s.do(t, cmdWrite, 0, 4096, 4096, make([]byte, 4096)); errno != errIO {
		t.Fatalf("failing write: errno %d", errno)
	}
	if errno, _ := s.do(t, cmdWrite, 0, 4096, 4096, make([]byte, 4096)); errno != 0 {
		t.Fatalf("next write: errno %d", errno)
	}
}

// blockingBackend parks every read until released, counting how many are
// parked at once.
type blockingBackend struct {
	*memBackend
	mu      sync.Mutex
	inside  int
	peak    int
	release chan struct{}
	entered chan struct{}
}

func (b *blockingBackend) ReadAt(p []byte, off int64) (int, error) {
	b.mu.Lock()
	b.inside++
	b.peak = max(b.peak, b.inside)
	b.mu.Unlock()
	b.entered <- struct{}{}
	<-b.release
	b.mu.Lock()
	b.inside--
	b.mu.Unlock()
	return b.memBackend.ReadAt(p, off)
}

func TestRequestsAreServedConcurrently(t *testing.T) {
	t.Parallel()
	b := &blockingBackend{memBackend: newMem(testSize), release: make(chan struct{}), entered: make(chan struct{}, 64)}
	s := negotiated(t, b, defaultOpts)
	const n = workers + 2
	go func() {
		for i := range n {
			var h [requestLen]byte
			putRequest(&h, request{typ: cmdRead, cookie: uint64(i), offset: uint64(i) * 4096, length: 4096})
			if _, err := s.c.Write(h[:]); err != nil {
				return
			}
		}
	}()
	for range workers {
		select {
		case <-b.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("requests were not dispatched concurrently")
		}
	}
	select {
	case <-b.entered:
		t.Fatalf("more than %d requests in the backend at once", workers)
	case <-time.After(100 * time.Millisecond):
	}
	close(b.release)
	seen := map[uint64]bool{}
	for range n {
		errno, cookie, _ := s.reply(t, 4096)
		if errno != 0 {
			t.Fatalf("errno %d", errno)
		}
		seen[cookie] = true
	}
	b.mu.Lock()
	peak := b.peak
	b.mu.Unlock()
	if len(seen) != n || peak != workers {
		t.Fatalf("cookies %v, peak %d", seen, peak)
	}
}

// hangingBackend never returns from a read.
type hangingBackend struct{ *memBackend }

func (hangingBackend) ReadAt([]byte, int64) (int, error) { select {} }

func TestServeReturnsWhenContextEndsWithBackendHung(t *testing.T) {
	t.Parallel()
	srv, cli := net.Pipe()
	t.Cleanup(func() { _ = cli.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, srv, hangingBackend{newMem(testSize)}, defaultOpts) }()
	_ = cli.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := clientHandshake(cli); err != nil {
		t.Fatal(err)
	}
	var h [requestLen]byte
	putRequest(&h, request{typ: cmdRead, cookie: 1, length: 4096})
	if _, err := cli.Write(h[:]); err != nil {
		t.Fatal(err)
	}
	// Then a DISC, which must wait for the hung read, so only ctx ends it.
	putRequest(&h, request{typ: cmdDisc, cookie: 2})
	if _, err := cli.Write(h[:]); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Serve: %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after its context ended")
	}
}

func TestServeReturnsWhenContextEndsDuringHandshake(t *testing.T) {
	t.Parallel()
	srv, cli := net.Pipe()
	t.Cleanup(func() { _ = cli.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, srv, newMem(testSize), defaultOpts) }()
	cancel() // the client never reads the greeting
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return")
	}
}

func TestServeRejectsBadOptions(t *testing.T) {
	t.Parallel()
	for _, o := range []ServeOptions{
		{BlockSize: 0, MaxRequest: 32 << 20},
		{BlockSize: 4096, MaxRequest: 1000},
		{BlockSize: 3000, MaxRequest: 32 << 20},
	} {
		srv, cli := net.Pipe()
		err := Serve(context.Background(), srv, newMem(testSize), o)
		_ = cli.Close()
		if err == nil {
			t.Errorf("Serve accepted %+v", o)
		}
	}
}

func BenchmarkServeRead(b *testing.B) {
	s := negotiated(b, newMem(testSize), defaultOpts)
	_ = s.c.SetDeadline(time.Time{})
	var h [requestLen]byte
	putRequest(&h, request{typ: cmdRead, cookie: 7, offset: 65536, length: 65536})
	var rh [replyLen]byte
	data := make([]byte, 65536)
	b.SetBytes(65536)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := s.c.Write(h[:]); err != nil {
			b.Fatal(err)
		}
		if _, err := io.ReadFull(s.c, rh[:]); err != nil {
			b.Fatal(err)
		}
		if _, err := io.ReadFull(s.c, data); err != nil {
			b.Fatal(err)
		}
	}
}
