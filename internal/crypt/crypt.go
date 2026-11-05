// Package crypt drives cryptsetup for the LUKS2 layer of a volume's device
// stack: format once on the raw device, open and close the mapping, grow it,
// and rotate the keyslot.
//
// Keys travel only as descriptors: each call writes the key to a pipe and
// hands the read end to the Runner, which makes it fd 3 (and fd 4) in the
// child, so cryptsetup is given /dev/fd/N. A key is never on a command line,
// never in an error and never logged.
package crypt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/greghaskins/csi-miami/internal/tool"
)

// Defaults for Params fields left zero: Adiantum for CPUs without AES
// instructions, a 256-bit key and the 4096-byte sectors of the nbd device.
const (
	DefaultCipher     = "xchacha12,aes-adiantum-plain64"
	DefaultKeySize    = 256
	DefaultSectorSize = 4096
)

// pbkdfIterations is the PBKDF2 cost. The key is a high-entropy secret from
// the node-stage Secret, not a passphrase, so stretching only slows stage.
const pbkdfIterations = "1000"

// ErrBadKey is returned when cryptsetup rejects the key (exit code 2: no
// keyslot matched).
var ErrBadKey = errors.New("crypt: no keyslot matches the key")

// Params are the class parameters of a LUKS2 format. A zero field takes its
// default.
type Params struct {
	Cipher     string
	KeySize    int // bits
	SectorSize int // bytes
}

func (p Params) withDefaults() Params {
	if p.Cipher == "" {
		p.Cipher = DefaultCipher
	}
	if p.KeySize == 0 {
		p.KeySize = DefaultKeySize
	}
	if p.SectorSize == 0 {
		p.SectorSize = DefaultSectorSize
	}
	return p
}

// Name is the device-mapper name for a volume.
func Name(volume string) string { return "miami-" + volume }

// MapperPath is the node of the mapping called name.
func MapperPath(name string) string { return "/dev/mapper/" + name }

// Format writes a LUKS2 header with key into dev.
func Format(ctx context.Context, r tool.Runner, dev string, key []byte, p Params) error {
	p = p.withDefaults()
	argv := []string{
		"cryptsetup", "luksFormat", "--type", "luks2",
		"--cipher", p.Cipher,
		"--key-size", strconv.Itoa(p.KeySize),
		"--sector-size", strconv.Itoa(p.SectorSize),
		"--pbkdf", "pbkdf2", "--pbkdf-force-iterations", pbkdfIterations,
		"--batch-mode", "--key-file", "/dev/fd/3", dev,
	}
	return run(ctx, r, "luksFormat", argv, key)
}

// Open maps dev as name using key. A wrong key is ErrBadKey.
//
// The volume key is stored in the dm-crypt table rather than in the kernel
// keyring (--disable-keyring). With a keyring key, cryptsetup resize asks for
// a passphrase to reload it, and a node-side expand has none; with the key in
// the table, resize reuses it, which is what Resize relies on.
func Open(ctx context.Context, r tool.Runner, dev, name string, key []byte) error {
	argv := []string{"cryptsetup", "open", "--type", "luks2", "--disable-keyring", "--key-file", "/dev/fd/3", dev, name}
	return run(ctx, r, "open", argv, key)
}

// Close removes the mapping called name.
func Close(ctx context.Context, r tool.Runner, name string) error {
	return run(ctx, r, "close", []string{"cryptsetup", "close", name})
}

// Resize grows the mapping to its device's size. The kernel holds the key, so
// none is needed.
func Resize(ctx context.Context, r tool.Runner, name string) error {
	return run(ctx, r, "resize", []string{"cryptsetup", "resize", name})
}

// Rotate replaces the keyslot that oldKey opens with newKey. A wrong oldKey
// is ErrBadKey.
func Rotate(ctx context.Context, r tool.Runner, dev string, oldKey, newKey []byte) error {
	argv := []string{"cryptsetup", "luksChangeKey", "--key-file", "/dev/fd/3", dev, "/dev/fd/4"}
	return run(ctx, r, "luksChangeKey", argv, oldKey, newKey)
}

// IsLUKS reports whether dev carries a LUKS header. Exit code 1 is "no", but
// cryptsetup exits 1 for a header it could not read as well (an I/O error on
// the device), and the answer "no" is what leads to a luksFormat. So the device
// is looked at first (it must exist, be a block device and read), and "no"
// stands only if the start of the device also reads as zeros: a device with
// data and no header is an error, left for a person.
func IsLUKS(ctx context.Context, r tool.Runner, dev string) (bool, error) {
	d, err := tool.ProbeDevice(r, dev, 0)
	if err != nil {
		return false, fmt.Errorf("crypt: isLuks: %w", err)
	}
	stdout, stderr, code, err := r.Run(ctx, []string{"cryptsetup", "isLuks", dev}, nil)
	if err != nil {
		return false, fmt.Errorf("crypt: isLuks: %w", err)
	}
	switch code {
	case 0:
		return true, nil
	case 1:
		if !d.Blank {
			return false, fmt.Errorf("crypt: isLuks: %s holds data but no LUKS header; not treating it as blank", dev)
		}
		return false, nil
	}
	return false, fmt.Errorf("crypt: isLuks: exit %d: %s", code, output(stdout, stderr))
}

// output is what a failed cryptsetup said: its standard error, or its standard
// output when the error stream is empty (cryptsetup reports some failures on
// the latter). Neither carries a key: keys travel on file descriptors.
func output(stdout, stderr []byte) string {
	if s := strings.TrimSpace(string(stderr)); s != "" {
		return s
	}
	return strings.TrimSpace(string(stdout))
}

// PayloadOffset is where the data of the LUKS2 volume on dev starts, in bytes:
// the offset of its segment in the header's JSON metadata. A format writes the
// header and the key slots below it and nothing above, so the first bytes of
// the payload are zero until something is written through the mapping, and then
// they are ciphertext, never zeros. The offset is checked: positive, a whole
// number of sectors, and leaving room on the device to read from.
func PayloadOffset(ctx context.Context, r tool.Runner, dev string) (int64, error) {
	stdout, stderr, code, err := r.Run(ctx, []string{"cryptsetup", "luksDump", "--dump-json-metadata", dev}, nil)
	if err != nil {
		return 0, fmt.Errorf("crypt: luksDump: %w", err)
	}
	if code != 0 {
		return 0, fmt.Errorf("crypt: luksDump: exit %d: %s", code, output(stdout, stderr))
	}
	var meta struct {
		Segments map[string]struct {
			Offset string `json:"offset"`
		} `json:"segments"`
	}
	if err := json.Unmarshal(stdout, &meta); err != nil {
		return 0, fmt.Errorf("crypt: luksDump: the metadata does not decode: %w", err)
	}
	seg, ok := meta.Segments["0"]
	if !ok {
		return 0, errors.New("crypt: luksDump: the metadata has no data segment")
	}
	off, err := strconv.ParseInt(seg.Offset, 10, 64)
	if err != nil || off <= 0 || off%512 != 0 {
		return 0, fmt.Errorf("crypt: luksDump: the data segment's offset %q is not a positive whole number of sectors", seg.Offset)
	}
	d, err := tool.ProbeDevice(r, dev, 0)
	if err != nil {
		return 0, fmt.Errorf("crypt: %w", err)
	}
	if off+tool.ProbeSize > d.Size {
		return 0, fmt.Errorf("crypt: the data segment's offset %d leaves no room on %s, which is %d bytes", off, dev, d.Size)
	}
	return off, nil
}

func run(ctx context.Context, r tool.Runner, op string, argv []string, keys ...[]byte) error {
	files, closeAll, err := keyFiles(keys)
	if err != nil {
		return fmt.Errorf("crypt: %s: %w", op, err)
	}
	defer closeAll()
	stdout, stderr, code, err := r.Run(ctx, argv, files)
	if err != nil {
		return fmt.Errorf("crypt: %s: %w", op, err)
	}
	switch code {
	case 0:
		return nil
	case 2:
		return fmt.Errorf("crypt: %s: %w", op, ErrBadKey)
	}
	return fmt.Errorf("crypt: %s: exit %d: %s", op, code, output(stdout, stderr))
}

// keyFiles makes one pipe per key, already holding the key with its write end
// closed, so the child reads it to EOF. A key is far below the pipe's
// capacity, so the write never blocks.
func keyFiles(keys [][]byte) (files []*os.File, closeAll func(), err error) {
	closeAll = func() {
		for _, f := range files {
			_ = f.Close()
		}
	}
	for _, k := range keys {
		rd, wr, perr := os.Pipe()
		if perr != nil {
			closeAll()
			return nil, func() {}, perr
		}
		_, werr := wr.Write(k)
		_ = wr.Close()
		files = append(files, rd)
		if werr != nil {
			closeAll()
			return nil, func() {}, werr
		}
	}
	return files, closeAll, nil
}
