package nbd

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Wire values of the NBD protocol (the upstream proto.md), fixed-newstyle
// negotiation and simple replies only.
const (
	magicNBD       = 0x4e42444d41474943 // "NBDMAGIC"
	magicOpt       = 0x49484156454f5054 // "IHAVEOPT"
	magicOptReply  = 0x0003e889045565a9
	magicRequest   = 0x25609513
	magicReplySimp = 0x67446698

	// Handshake flags (server) and client flags.
	flagFixedNewstyle  = 1 << 0
	flagNoZeroes       = 1 << 1
	cflagFixedNewstyle = 1 << 0
	cflagNoZeroes      = 1 << 1

	optExportName      = 1
	optAbort           = 2
	optInfo            = 6
	optGo              = 7
	optStructuredReply = 8

	repAck        = 1
	repInfo       = 3
	repErrUnsup   = 1<<31 + 1
	repErrInvalid = 1<<31 + 3

	infoExport    = 0
	infoBlockSize = 3

	// FlagHasFlags must accompany the other transmission flags.
	FlagHasFlags = 1 << 0
	// FlagReadOnly marks a read-only export.
	FlagReadOnly = 1 << 1

	cmdRead  = 0
	cmdWrite = 1
	cmdDisc  = 2
	cmdFlush = 3
	cmdTrim  = 4

	cmdFlagFUA = 1 << 0

	errPerm  = 1
	errIO    = 5
	errInval = 22
	errNoSpc = 28

	requestLen = 28
	replyLen   = 16

	// maxOptionLen bounds an option's payload during negotiation; nothing we
	// accept is longer than a name and a few info requests.
	maxOptionLen = 64 << 10
)

// Transmission flags the server advertises (besides FlagHasFlags).
const (
	FlagSendFlush = 1 << 2
	FlagSendFUA   = 1 << 3
	FlagSendTrim  = 1 << 5
)

// errProtocol marks a peer that broke the protocol.
var errProtocol = errors.New("nbd: protocol violation")

// request is a transmission request header.
type request struct {
	flags  uint16
	typ    uint16
	cookie uint64
	offset uint64
	length uint32
}

func parseRequest(b *[requestLen]byte) (request, error) {
	if binary.BigEndian.Uint32(b[0:4]) != magicRequest {
		return request{}, fmt.Errorf("%w: bad request magic", errProtocol)
	}
	return request{
		flags:  binary.BigEndian.Uint16(b[4:6]),
		typ:    binary.BigEndian.Uint16(b[6:8]),
		cookie: binary.BigEndian.Uint64(b[8:16]),
		offset: binary.BigEndian.Uint64(b[16:24]),
		length: binary.BigEndian.Uint32(b[24:28]),
	}, nil
}

func putRequest(b *[requestLen]byte, r request) {
	binary.BigEndian.PutUint32(b[0:4], magicRequest)
	binary.BigEndian.PutUint16(b[4:6], r.flags)
	binary.BigEndian.PutUint16(b[6:8], r.typ)
	binary.BigEndian.PutUint64(b[8:16], r.cookie)
	binary.BigEndian.PutUint64(b[16:24], r.offset)
	binary.BigEndian.PutUint32(b[24:28], r.length)
}

func putReply(b *[replyLen]byte, errno uint32, cookie uint64) {
	binary.BigEndian.PutUint32(b[0:4], magicReplySimp)
	binary.BigEndian.PutUint32(b[4:8], errno)
	binary.BigEndian.PutUint64(b[8:16], cookie)
}

// export is what negotiation tells the client.
type export struct {
	size      uint64
	flags     uint16
	blockSize uint32
	maxReq    uint32
}

// serverHandshake runs fixed-newstyle negotiation until NBD_OPT_GO or
// NBD_OPT_EXPORT_NAME (go is true) or NBD_OPT_ABORT (go is false). Any export
// name is accepted: a connection serves exactly one export, and the name the
// client asks for (the default "" or the volume's) selects nothing else.
func serverHandshake(rw io.ReadWriter, e export) (enter bool, err error) {
	var hello [18]byte
	binary.BigEndian.PutUint64(hello[0:8], magicNBD)
	binary.BigEndian.PutUint64(hello[8:16], magicOpt)
	binary.BigEndian.PutUint16(hello[16:18], flagFixedNewstyle|flagNoZeroes)
	if _, err := rw.Write(hello[:]); err != nil {
		return false, err
	}
	var cf [4]byte
	if _, err := io.ReadFull(rw, cf[:]); err != nil {
		return false, err
	}
	cflags := binary.BigEndian.Uint32(cf[:])
	if cflags&^(cflagFixedNewstyle|cflagNoZeroes) != 0 || cflags&cflagFixedNewstyle == 0 {
		return false, fmt.Errorf("%w: client flags %#x", errProtocol, cflags)
	}
	noZeroes := cflags&cflagNoZeroes != 0

	for {
		var hdr [16]byte
		if _, err := io.ReadFull(rw, hdr[:]); err != nil {
			return false, err
		}
		if binary.BigEndian.Uint64(hdr[0:8]) != magicOpt {
			return false, fmt.Errorf("%w: bad option magic", errProtocol)
		}
		opt := binary.BigEndian.Uint32(hdr[8:12])
		n := binary.BigEndian.Uint32(hdr[12:16])
		if n > maxOptionLen {
			return false, fmt.Errorf("%w: option %d of %d bytes", errProtocol, opt, n)
		}
		data := make([]byte, n)
		if _, err := io.ReadFull(rw, data); err != nil {
			return false, err
		}
		switch opt {
		case optAbort:
			// The reply is a courtesy; the client may already have gone.
			_ = writeOptReply(rw, opt, repAck, nil)
			return false, nil
		case optExportName:
			var b [10 + 124]byte
			binary.BigEndian.PutUint64(b[0:8], e.size)
			binary.BigEndian.PutUint16(b[8:10], e.flags)
			out := b[:]
			if noZeroes {
				out = b[:10]
			}
			_, err := rw.Write(out)
			return err == nil, err
		case optInfo, optGo:
			if !validInfoRequest(data) {
				if err := writeOptReply(rw, opt, repErrInvalid, nil); err != nil {
					return false, err
				}
				continue
			}
			var ex [12]byte
			binary.BigEndian.PutUint16(ex[0:2], infoExport)
			binary.BigEndian.PutUint64(ex[2:10], e.size)
			binary.BigEndian.PutUint16(ex[10:12], e.flags)
			if err := writeOptReply(rw, opt, repInfo, ex[:]); err != nil {
				return false, err
			}
			var bs [14]byte
			binary.BigEndian.PutUint16(bs[0:2], infoBlockSize)
			binary.BigEndian.PutUint32(bs[2:6], e.blockSize)
			binary.BigEndian.PutUint32(bs[6:10], e.blockSize)
			binary.BigEndian.PutUint32(bs[10:14], e.maxReq)
			if err := writeOptReply(rw, opt, repInfo, bs[:]); err != nil {
				return false, err
			}
			if err := writeOptReply(rw, opt, repAck, nil); err != nil {
				return false, err
			}
			if opt == optGo {
				return true, nil
			}
		default:
			// Structured replies, TLS, listing, meta contexts: unsupported.
			if err := writeOptReply(rw, opt, repErrUnsup, nil); err != nil {
				return false, err
			}
		}
	}
}

// validInfoRequest checks the layout of NBD_OPT_INFO/GO data: a 32-bit name
// length, the name, a 16-bit count and that many 16-bit info types.
func validInfoRequest(d []byte) bool {
	if len(d) < 4 {
		return false
	}
	n := uint64(binary.BigEndian.Uint32(d[0:4]))
	if uint64(len(d)) < 4+n+2 {
		return false
	}
	rest := d[4+n:]
	k := uint64(binary.BigEndian.Uint16(rest[0:2]))
	return uint64(len(rest)) == 2+2*k
}

func writeOptReply(w io.Writer, opt, typ uint32, data []byte) error {
	b := make([]byte, 20+len(data))
	binary.BigEndian.PutUint64(b[0:8], magicOptReply)
	binary.BigEndian.PutUint32(b[8:12], opt)
	binary.BigEndian.PutUint32(b[12:16], typ)
	binary.BigEndian.PutUint32(b[16:20], uint32(len(data))) //nolint:gosec // data is at most a few bytes
	copy(b[20:], data)
	_, err := w.Write(b)
	return err
}

// clientHandshake is the client half: fixed newstyle, NBD_OPT_GO for the
// default export with the block-size info requested. It is what Connect runs
// on the socket before handing it to the kernel, which only speaks the
// transmission phase.
func clientHandshake(rw io.ReadWriter) (export, error) {
	var hello [18]byte
	if _, err := io.ReadFull(rw, hello[:]); err != nil {
		return export{}, err
	}
	if binary.BigEndian.Uint64(hello[0:8]) != magicNBD || binary.BigEndian.Uint64(hello[8:16]) != magicOpt {
		return export{}, fmt.Errorf("%w: not a newstyle server", errProtocol)
	}
	hf := binary.BigEndian.Uint16(hello[16:18])
	if hf&flagFixedNewstyle == 0 || hf&flagNoZeroes == 0 {
		return export{}, fmt.Errorf("%w: server handshake flags %#x", errProtocol, hf)
	}
	var req [4 + 16 + 4 + 2 + 2]byte
	binary.BigEndian.PutUint32(req[0:4], cflagFixedNewstyle|cflagNoZeroes)
	binary.BigEndian.PutUint64(req[4:12], magicOpt)
	binary.BigEndian.PutUint32(req[12:16], optGo)
	binary.BigEndian.PutUint32(req[16:20], 8) // name length 0, one info request
	binary.BigEndian.PutUint32(req[20:24], 0)
	binary.BigEndian.PutUint16(req[24:26], 1)
	binary.BigEndian.PutUint16(req[26:28], infoBlockSize)
	if _, err := rw.Write(req[:]); err != nil {
		return export{}, err
	}
	var e export
	haveExport := false
	for {
		var hdr [20]byte
		if _, err := io.ReadFull(rw, hdr[:]); err != nil {
			return export{}, err
		}
		if binary.BigEndian.Uint64(hdr[0:8]) != magicOptReply || binary.BigEndian.Uint32(hdr[8:12]) != optGo {
			return export{}, fmt.Errorf("%w: bad option reply", errProtocol)
		}
		typ := binary.BigEndian.Uint32(hdr[12:16])
		n := binary.BigEndian.Uint32(hdr[16:20])
		if n > maxOptionLen {
			return export{}, fmt.Errorf("%w: option reply of %d bytes", errProtocol, n)
		}
		data := make([]byte, n)
		if _, err := io.ReadFull(rw, data); err != nil {
			return export{}, err
		}
		switch {
		case typ == repAck:
			if !haveExport {
				return export{}, fmt.Errorf("%w: no export info before ack", errProtocol)
			}
			return e, nil
		case typ == repInfo && len(data) >= 2:
			switch binary.BigEndian.Uint16(data[0:2]) {
			case infoExport:
				if len(data) != 12 {
					return export{}, fmt.Errorf("%w: export info of %d bytes", errProtocol, len(data))
				}
				e.size = binary.BigEndian.Uint64(data[2:10])
				e.flags = binary.BigEndian.Uint16(data[10:12])
				haveExport = true
			case infoBlockSize:
				if len(data) != 14 {
					return export{}, fmt.Errorf("%w: block size info of %d bytes", errProtocol, len(data))
				}
				e.blockSize = binary.BigEndian.Uint32(data[6:10])
				e.maxReq = binary.BigEndian.Uint32(data[10:14])
			}
		case typ&(1<<31) != 0:
			return export{}, fmt.Errorf("nbd: server refused NBD_OPT_GO: reply %#x", typ)
		}
	}
}
