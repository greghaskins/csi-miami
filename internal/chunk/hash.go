// Package chunk is the on-transport format of a chunk-backed volume: a
// directory holding chunks/<aa>/<sha256> (fixed-size pieces of a ciphertext
// device, named by their hash), manifests/<epoch>.<sequence> (text: header
// lines, then one chunk hash per line in device order) and current (the name
// of the latest complete manifest, replaced by rename).
//
// The store lives on an NFSv3 mount, so every write goes to a temporary name
// beside its destination, is fsynced, and is renamed (or linked) into place;
// current is opened fresh on every read; and nothing assumes a conditional
// rename. The package depends on no other driver package.
package chunk

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
)

const (
	// MinChunkSize is the smallest chunk size, a power of two.
	MinChunkSize = 1 << 20
	// MaxChunkSize is the largest chunk size, a power of two.
	MaxChunkSize = 256 << 20
	// DefaultMinChunk is the smallest chunk size DefaultChunkSize returns.
	DefaultMinChunk = 4 << 20
	// MaxManifestLines is the most hash lines a manifest may hold.
	MaxManifestLines = 65536
	// FormatVersion is the manifest format this package writes and reads.
	FormatVersion = 1
)

// Hash is a SHA-256 digest.
type Hash [32]byte

// String is the digest as 64 lower-case hexadecimal characters.
func (h Hash) String() string { return hex.EncodeToString(h[:]) }

// ParseHash parses the form String produces; upper case is rejected so that
// a name has one spelling.
func ParseHash(s string) (Hash, error) {
	var h Hash
	if len(s) != 2*len(h) {
		return h, fmt.Errorf("chunk: hash %q is not 64 hex characters", s)
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return Hash{}, fmt.Errorf("chunk: hash %q is not lower-case hex", s)
		}
	}
	if _, err := hex.Decode(h[:], []byte(s)); err != nil {
		return Hash{}, fmt.Errorf("chunk: hash %q: %w", s, err)
	}
	return h, nil
}

// Sum is the hash of b.
func Sum(b []byte) Hash { return sha256.Sum256(b) }

var zeroHashes sync.Map // int64 chunk size -> Hash

// ZeroHash is the hash of chunkSize zero bytes: the one well-known name of
// the all-zero chunk. It is memoised per size.
func ZeroHash(chunkSize int64) Hash {
	if v, ok := zeroHashes.Load(chunkSize); ok {
		h, _ := v.(Hash)
		return h
	}
	d := sha256.New()
	var block [64 << 10]byte
	for left := chunkSize; left > 0; {
		n := min(left, int64(len(block)))
		d.Write(block[:n])
		left -= n
	}
	var h Hash
	d.Sum(h[:0])
	zeroHashes.Store(chunkSize, h)
	return h
}

func isPow2(n int64) bool { return n > 0 && n&(n-1) == 0 }

// RoundUp rounds size up to a whole number of chunks.
func RoundUp(size, chunk int64) int64 {
	if chunk <= 0 {
		return size
	}
	return (size + chunk - 1) / chunk * chunk
}

func lines(size, chunk int64) int64 { return (size + chunk - 1) / chunk }

// DefaultChunkSize is the smallest power of two of at least DefaultMinChunk
// that keeps the manifest to MaxManifestLines lines, capped at MaxChunkSize
// (a size that still exceeds the cap fails CheckChunkSize).
func DefaultChunkSize(size int64) int64 {
	c := int64(DefaultMinChunk)
	for c < MaxChunkSize && lines(size, c) > MaxManifestLines {
		c <<= 1
	}
	return c
}

// ErrChunkSize is wrapped by every CheckChunkSize failure.
var ErrChunkSize = errors.New("chunk: invalid chunk size")

// CheckChunkSize reports whether chunk is a power of two in
// [MinChunkSize, MaxChunkSize] and keeps size, rounded up, to at most
// MaxManifestLines chunks.
func CheckChunkSize(size, chunk int64) error {
	switch {
	case !isPow2(chunk) || chunk < MinChunkSize || chunk > MaxChunkSize:
		return fmt.Errorf("%w: %d is not a power of two from %d to %d", ErrChunkSize, chunk, MinChunkSize, MaxChunkSize)
	case size <= 0:
		return fmt.Errorf("%w: size %d is not positive", ErrChunkSize, size)
	case lines(size, chunk) > MaxManifestLines:
		return fmt.Errorf("%w: %d bytes in %d byte chunks exceeds %d manifest lines", ErrChunkSize, size, chunk, MaxManifestLines)
	}
	return nil
}
