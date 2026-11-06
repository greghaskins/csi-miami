package chunk

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// ErrManifestFormat is wrapped by every manifest, manifest-name and current
// parse failure.
var ErrManifestFormat = errors.New("chunk: malformed manifest")

// maxManifestBytes bounds what ParseManifest reads: the line cap at 65 bytes a
// line, plus room for the header.
const maxManifestBytes = MaxManifestLines*65 + 4096

// Manifest is one generation of a volume: the hash of every chunk of the
// device, in order.
type Manifest struct {
	Volume   string
	Epoch    uint64
	Sequence uint64
	Size     int64 // device size in bytes, a whole number of chunks
	Chunk    int64 // chunk size in bytes
	Time     time.Time
	Chunks   []Hash
}

// Name is the manifest's file name, "<epoch>.<sequence>".
func (m Manifest) Name() string {
	return strconv.FormatUint(m.Epoch, 10) + "." + strconv.FormatUint(m.Sequence, 10)
}

func parseCanonicalUint(s string) (uint64, error) {
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil || strconv.FormatUint(v, 10) != s {
		return 0, fmt.Errorf("%w: a %d byte value is not a canonical decimal number", ErrManifestFormat, len(s))
	}
	return v, nil
}

// parseCanonicalInt is parseCanonicalUint for a signed value: no sign, no
// leading zeros, so a manifest has one spelling and Marshal reproduces it.
func parseCanonicalInt(s string) (int64, error) {
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || strconv.FormatInt(v, 10) != s {
		return 0, fmt.Errorf("%w: a %d byte value is not a canonical decimal number", ErrManifestFormat, len(s))
	}
	return v, nil
}

// ParseManifestName splits "<epoch>.<sequence>", both canonical decimal.
func ParseManifestName(s string) (epoch, seq uint64, err error) {
	e, q, ok := strings.Cut(s, ".")
	if !ok {
		return 0, 0, fmt.Errorf("%w: a %d byte name is not <epoch>.<sequence>", ErrManifestFormat, len(s))
	}
	if epoch, err = parseCanonicalUint(e); err != nil {
		return 0, 0, err
	}
	if seq, err = parseCanonicalUint(q); err != nil {
		return 0, 0, err
	}
	return epoch, seq, nil
}

// Marshal renders the manifest: header lines, then one hash per line. The
// volume name must not contain a line break (ParseManifest would reject the
// result). Time is written in UTC at second resolution.
func (m Manifest) Marshal() []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "# format %d\n", FormatVersion)
	fmt.Fprintf(&b, "# volume %s\n", m.Volume)
	fmt.Fprintf(&b, "# epoch %d\n", m.Epoch)
	fmt.Fprintf(&b, "# sequence %d\n", m.Sequence)
	fmt.Fprintf(&b, "# size %d\n", m.Size)
	fmt.Fprintf(&b, "# chunk %d\n", m.Chunk)
	fmt.Fprintf(&b, "# time %s\n", m.Time.UTC().Format(time.RFC3339))
	for _, h := range m.Chunks {
		b.WriteString(h.String())
		b.WriteByte('\n')
	}
	return b.Bytes()
}

func bad(format string, a ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrManifestFormat}, a...)...)
}

// ParseManifest reads a manifest. The format line must come first; every
// header key must appear exactly once; unknown header keys are ignored; the
// hash lines must number exactly Size/Chunk, each a lower-case SHA-256.
func ParseManifest(r io.Reader) (Manifest, error) {
	var m Manifest
	sc := bufio.NewScanner(io.LimitReader(r, maxManifestBytes+1))
	sc.Buffer(make([]byte, 0, 4096), 1<<16)
	seen := map[string]bool{}
	first := true
	var read int
	for sc.Scan() {
		line := sc.Text()
		read += len(line) + 1
		if read > maxManifestBytes {
			return Manifest{}, bad("too large")
		}
		if rest, ok := strings.CutPrefix(line, "#"); ok {
			key, val, _ := strings.Cut(strings.TrimPrefix(rest, " "), " ")
			if first && (key != "format" || val != strconv.Itoa(FormatVersion)) {
				return Manifest{}, bad("the first line is not the format line this driver reads")
			}
			first = false
			if len(m.Chunks) > 0 {
				return Manifest{}, bad("header line after hash lines")
			}
			if err := m.setHeader(seen, key, val); err != nil {
				return Manifest{}, err
			}
			continue
		}
		if first {
			return Manifest{}, bad("missing format line")
		}
		if len(m.Chunks) >= MaxManifestLines {
			return Manifest{}, bad("more than %d hash lines", MaxManifestLines)
		}
		h, err := ParseHash(line)
		if err != nil {
			return Manifest{}, bad("%w", err)
		}
		m.Chunks = append(m.Chunks, h)
	}
	if err := sc.Err(); err != nil {
		return Manifest{}, bad("%w", err)
	}
	if first {
		return Manifest{}, bad("empty")
	}
	for _, k := range []string{"volume", "epoch", "sequence", "size", "chunk", "time"} {
		if !seen[k] {
			return Manifest{}, bad("missing header %q", k)
		}
	}
	if err := CheckChunkSize(m.Size, m.Chunk); err != nil {
		return Manifest{}, bad("%w", err)
	}
	if m.Size%m.Chunk != 0 {
		return Manifest{}, bad("size %d is not a whole number of %d byte chunks", m.Size, m.Chunk)
	}
	if want := m.Size / m.Chunk; int64(len(m.Chunks)) != want {
		return Manifest{}, bad("%d hash lines, size and chunk say %d", len(m.Chunks), want)
	}
	return m, nil
}

func (m *Manifest) setHeader(seen map[string]bool, key, val string) error {
	switch key {
	case "format":
		if seen[key] || val != strconv.Itoa(FormatVersion) {
			return bad("unknown format")
		}
	case "volume", "epoch", "sequence", "size", "chunk", "time":
		if seen[key] {
			return bad("duplicate header %q", key)
		}
	default:
		return nil
	}
	seen[key] = true
	var err error
	switch key {
	case "volume":
		m.Volume = val
	case "epoch":
		m.Epoch, err = parseCanonicalUint(val)
	case "sequence":
		m.Sequence, err = parseCanonicalUint(val)
	case "size":
		m.Size, err = parseCanonicalInt(val)
	case "chunk":
		m.Chunk, err = parseCanonicalInt(val)
	case "time":
		m.Time, err = time.Parse(time.RFC3339, val)
	}
	if err != nil {
		return bad("header %q has an unusable value (%d bytes)", key, len(val))
	}
	return nil
}
