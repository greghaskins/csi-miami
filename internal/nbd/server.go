package nbd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"net"
	"sync"
)

// Backend is what the server serves. Offsets and lengths are byte values the
// kernel already bounded by the advertised block sizes; the server checks
// them against Size before calling, so a Backend never sees a request that
// runs past its end.
type Backend interface {
	ReadAt(p []byte, off int64) (int, error)
	WriteAt(p []byte, off int64, fua bool) (int, error)
	Flush() error
	Trim(off, n int64) error
	Size() int64
}

// ServeOptions shape the export. BlockSize is 4096 and MaxRequest 32 MiB in
// the driver; BlockSize must be a power of two from 512, MaxRequest a
// multiple of it.
type ServeOptions struct {
	ReadOnly   bool
	BlockSize  uint32
	MaxRequest uint32
}

// workers is how many requests are in the backend at once.
const workers = 8

// maxMaxRequest bounds MaxRequest (the kernel sends at most 32 MiB).
const maxMaxRequest = 1 << 30

// discardChunk bounds the reader's buffer for skipping a rejected payload.
const discardChunk = 64 << 10

// Serve speaks the fixed-newstyle handshake then transmission until
// NBD_CMD_DISC or ctx ends. It owns conn and closes it when it returns.
//
// It returns nil after NBD_CMD_DISC (once the requests before it are
// answered) or NBD_OPT_ABORT, ctx's error when ctx ends, and an error when
// the peer breaks the protocol or the connection fails. When ctx ends it
// closes conn and returns at once, even if the backend is stuck: a request
// still in the backend is abandoned, and its reply goes nowhere.
//
// A backend error fails that request alone with EIO; the loop continues.
func Serve(ctx context.Context, conn net.Conn, b Backend, o ServeOptions) error {
	defer func() { _ = conn.Close() }()
	if o.BlockSize < 512 || o.BlockSize&(o.BlockSize-1) != 0 {
		return fmt.Errorf("nbd: block size %d is not a power of two from 512", o.BlockSize)
	}
	if o.MaxRequest < o.BlockSize || o.MaxRequest%o.BlockSize != 0 || o.MaxRequest > maxMaxRequest {
		return fmt.Errorf("nbd: max request %d is not a multiple of the block size %d up to %d", o.MaxRequest, o.BlockSize, maxMaxRequest)
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	flags := uint16(FlagHasFlags | FlagSendFlush | FlagSendFUA | FlagSendTrim)
	if o.ReadOnly {
		flags |= FlagReadOnly
	}
	size := b.Size()
	if size < 0 {
		return fmt.Errorf("nbd: backend size %d", size)
	}
	enter, err := serverHandshake(conn, export{size: uint64(size), flags: flags, blockSize: o.BlockSize, maxReq: o.MaxRequest})
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		return fmt.Errorf("nbd: handshake: %w", err)
	}
	if !enter {
		return nil
	}
	s := &session{conn: conn, b: b, o: o, free: make(chan *job, workers), work: make(chan *job, workers)}
	for i := range s.jobs {
		s.free <- &s.jobs[i]
	}
	for range workers {
		go s.worker()
	}
	defer close(s.work)
	err = s.read(ctx)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if ferr := s.failure(); ferr != nil {
		return ferr
	}
	return err
}

type job struct {
	req request
	buf *[]byte // the write payload, from the pool
	hdr [replyLen]byte
}

type session struct {
	conn net.Conn
	b    Backend
	o    ServeOptions

	wmu  sync.Mutex // serializes replies on conn
	free chan *job
	work chan *job
	jobs [workers]job

	fmu    sync.Mutex
	failed error

	// The reader's own buffers.
	rhdr    [requestLen]byte
	ihdr    [replyLen]byte
	discard []byte
}

func (s *session) fail(err error) {
	s.fmu.Lock()
	if s.failed == nil {
		s.failed = fmt.Errorf("nbd: write reply: %w", err)
	}
	s.fmu.Unlock()
	_ = s.conn.Close()
}

func (s *session) failure() error {
	s.fmu.Lock()
	defer s.fmu.Unlock()
	return s.failed
}

// read is the request loop: one reader, requests handed to the workers.
func (s *session) read(ctx context.Context) error {
	for {
		if _, err := io.ReadFull(s.conn, s.rhdr[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return fmt.Errorf("nbd: connection closed without NBD_CMD_DISC: %w", io.ErrUnexpectedEOF)
			}
			return fmt.Errorf("nbd: read request: %w", err)
		}
		r, err := parseRequest(&s.rhdr)
		if err != nil {
			return err
		}
		switch r.typ {
		case cmdDisc:
			return s.drain(ctx)
		case cmdRead, cmdWrite:
			if r.length > s.o.MaxRequest {
				if r.typ == cmdWrite {
					if err := s.skip(r.length); err != nil {
						return err
					}
				}
				s.inline(errInval, r.cookie)
				continue
			}
		case cmdFlush, cmdTrim:
		default:
			s.inline(errInval, r.cookie)
			continue
		}
		var j *job
		select {
		case j = <-s.free:
		case <-ctx.Done():
			return ctx.Err()
		}
		j.req = r
		if r.typ == cmdWrite {
			j.buf = getBuf(r.length)
			if _, err := io.ReadFull(s.conn, (*j.buf)[:r.length]); err != nil {
				putBuf(j.buf)
				j.buf = nil
				return fmt.Errorf("nbd: read write payload: %w", err)
			}
		}
		s.work <- j // never blocks: there are only `workers` jobs
	}
}

// skip consumes a rejected payload in bounded pieces.
func (s *session) skip(n uint32) error {
	if s.discard == nil {
		s.discard = make([]byte, discardChunk)
	}
	for n > 0 {
		k := min(n, discardChunk)
		if _, err := io.ReadFull(s.conn, s.discard[:k]); err != nil {
			return fmt.Errorf("nbd: skip payload: %w", err)
		}
		n -= k
	}
	return nil
}

// inline answers from the reader, for requests that never reach a worker.
func (s *session) inline(errno uint32, cookie uint64) {
	putReply(&s.ihdr, errno, cookie)
	s.wmu.Lock()
	_, err := s.conn.Write(s.ihdr[:])
	s.wmu.Unlock()
	if err != nil {
		s.fail(err)
	}
}

// drain waits for every request before NBD_CMD_DISC to be answered.
func (s *session) drain(ctx context.Context) error {
	for range workers {
		select {
		case <-s.free:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (s *session) worker() {
	for j := range s.work {
		s.handle(j)
		s.free <- j // never blocks: free holds every job
	}
}

// inRange reports whether [off, off+n) lies within size.
func inRange(off uint64, n uint32, size int64) bool {
	return size >= 0 && off <= uint64(size) && uint64(n) <= uint64(size)-off
}

func (s *session) handle(j *job) {
	r := j.req
	size := s.b.Size()
	var errno uint32
	switch r.typ {
	case cmdRead:
		if !inRange(r.offset, r.length, size) {
			errno = errInval
			break
		}
		bp := getBuf(r.length)
		data := (*bp)[:r.length]
		n, err := s.b.ReadAt(data, int64(r.offset)) //nolint:gosec // bounded by inRange
		if n == len(data) && (err == nil || errors.Is(err, io.EOF)) {
			s.reply(j, 0, data)
			putBuf(bp)
			return
		}
		putBuf(bp)
		errno = errIO
	case cmdWrite:
		data := (*j.buf)[:r.length]
		switch {
		case s.o.ReadOnly:
			errno = errPerm
		case !inRange(r.offset, r.length, size):
			errno = errNoSpc
		default:
			n, err := s.b.WriteAt(data, int64(r.offset), r.flags&cmdFlagFUA != 0) //nolint:gosec // bounded by inRange
			if err != nil || n != len(data) {
				errno = errIO
			}
		}
		putBuf(j.buf)
		j.buf = nil
	case cmdFlush:
		if s.b.Flush() != nil {
			errno = errIO
		}
	case cmdTrim:
		switch {
		case s.o.ReadOnly:
			errno = errPerm
		case !inRange(r.offset, r.length, size):
			errno = errInval
		case s.b.Trim(int64(r.offset), int64(r.length)) != nil: //nolint:gosec // bounded by inRange
			errno = errIO
		}
	}
	s.reply(j, errno, nil)
}

func (s *session) reply(j *job, errno uint32, data []byte) {
	putReply(&j.hdr, errno, j.req.cookie)
	s.wmu.Lock()
	_, err := s.conn.Write(j.hdr[:])
	if err == nil && data != nil {
		_, err = s.conn.Write(data)
	}
	s.wmu.Unlock()
	if err != nil {
		s.fail(err)
	}
}

// Payload buffers come from one pool per power-of-two size class from 4 KiB,
// so the steady state allocates nothing and a small request does not pin a
// MaxRequest-sized buffer.
const minClass = 12

var pools [31]sync.Pool

func class(n uint32) int {
	if n <= 1<<minClass {
		return minClass
	}
	return bits.Len32(n - 1)
}

func getBuf(n uint32) *[]byte {
	c := class(n)
	if v := pools[c].Get(); v != nil {
		return v.(*[]byte)
	}
	b := make([]byte, 1<<c)
	return &b
}

func putBuf(b *[]byte) {
	pools[class(uint32(len(*b)))].Put(b) //nolint:gosec // len is a size class, at most maxMaxRequest
}
