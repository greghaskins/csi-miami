package volume

import (
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/greghaskins/csi-miami/internal/transport"
)

// Volume context keys. Every key the driver owns starts with contextPrefix;
// Kubernetes adds keys of its own, which are ignored.
const (
	contextPrefix = "miami/"

	keyKind            = contextPrefix + "kind"
	keyTransportKind   = contextPrefix + "transport.kind"
	keyTransportServer = contextPrefix + "transport.server"
	keyTransportExport = contextPrefix + "transport.export"
	keyTransportPath   = contextPrefix + "transport.path"
	keySubdir          = contextPrefix + "subdir"
	keyMode            = contextPrefix + "mode"

	// Keys of the volume kind only.
	keyChunkSize         = contextPrefix + "chunkSize"
	keyCacheSize         = contextPrefix + "cacheSize"
	keyEncryption        = contextPrefix + "encryption"
	keyCipher            = contextPrefix + "cipher"
	keyKeySize           = contextPrefix + "keySize"
	keySize              = contextPrefix + "size"
	keyGenInterval       = contextPrefix + "generations.interval"
	keyGenKeep           = contextPrefix + "generations.keep"
	keyGenMaxAge         = contextPrefix + "generations.maxAge"
	keyGenDirtyWatermark = contextPrefix + "generations.dirtyWatermark"
	// keyTakeoverTimeout is carried by the context of a PV made before decision
	// 52 removed the timed takeover. It is accepted and means nothing, and
	// Encode no longer writes it.
	keyTakeoverTimeout = contextPrefix + "takeoverTimeout"
)

// volumeKeys are the context keys only a volume of kind volume carries.
var volumeKeys = []string{
	keyChunkSize, keyCacheSize, keyEncryption, keyCipher, keyKeySize, keySize,
	keyGenInterval, keyGenKeep, keyGenMaxAge, keyGenDirtyWatermark,
}

// Context is the volume context set by CreateVolume, stored on the PV and
// sent on every node call.
//
// The directory of a volume is always <transport mount>/<Subdir>/<name>,
// where name comes from the volume ID; Dir computes it.
type Context struct {
	Kind      Kind
	Transport transport.Spec
	Subdir    string // optional prefix, relative and cleaned; empty means none

	// Mode is the mode of the item directory when the driver creates it, as
	// four octal digits (0755); empty means DefaultMode. It applies to the
	// directory of the volume only, not to the subdir elements above it, and
	// only to a directory the driver makes: one that exists is adopted as it
	// is. DirMode gives it as a number.
	Mode string

	// Volume holds the parameters of a volume of kind volume; nil for a
	// directory. Compare Contexts with Equal, not ==.
	Volume *VolumeParams
}

// DirMode returns the mode of the item directory as a number: DefaultMode
// when Mode is empty.
func (c Context) DirMode() uint32 { return ModeBits(c.Mode) }

// Validate reports whether the Context is complete and exactly in the form
// NewContext produces. The error carries the gRPC code InvalidArgument.
func (c Context) Validate() error {
	if !c.Kind.Valid() {
		return invalid("volume context " + keyKind + " must be directory or volume")
	}
	if (c.Kind == KindVolume) != (c.Volume != nil) {
		return invalid("volume parameters belong to kind " + string(KindVolume) + " and to no other")
	}
	if c.Volume != nil {
		if c.Transport.Kind == transport.KindLocal {
			return invalid("kind " + string(KindVolume) + " needs a remote transport: transport.kind local is not supported")
		}
		if err := c.Volume.check(); err != nil {
			return err
		}
	}
	if err := c.Transport.Validate(); err != nil {
		return err
	}
	if err := CheckMode(c.Mode); err != nil {
		return err
	}
	if c.Subdir != "" {
		return validateSubdir(c.Subdir)
	}
	return nil
}

// MatchesID returns InvalidArgument unless id is a valid volume ID of the
// same kind as the Context.
func (c Context) MatchesID(id ID) error {
	if err := id.Validate(); err != nil {
		return err
	}
	if id.Kind != c.Kind {
		return invalid("volume ID kind does not match the volume context kind")
	}
	return nil
}

// Dir returns the directory of the volume: <transport mount>/<Subdir>/<name>
// with the transport mounted under root (see transport.Spec.MountPoint). It
// is the one place the directory is computed. Because the volume's own name is
// always the last element, no two volumes share a directory.
//
// It returns InvalidArgument if the Context or the ID is invalid, if their
// kinds differ, or if root is not an absolute, cleaned path.
func (c Context) Dir(root string, id ID) (string, error) {
	if err := c.Validate(); err != nil {
		return "", err
	}
	if err := c.MatchesID(id); err != nil {
		return "", err
	}
	mount, err := c.Transport.MountPoint(root)
	if err != nil {
		return "", err
	}
	return filepath.Join(mount, c.Subdir, id.Name), nil
}

// Encode returns the volume context map. Empty values are omitted.
func (c Context) Encode() map[string]string {
	m := make(map[string]string, 6)
	set := func(k, v string) {
		if v != "" {
			m[k] = v
		}
	}
	set(keyKind, string(c.Kind))
	set(keyTransportKind, c.Transport.Kind)
	set(keyTransportServer, c.Transport.Server)
	set(keyTransportExport, c.Transport.Export)
	set(keyTransportPath, c.Transport.Path)
	set(keySubdir, c.Subdir)
	set(keyMode, c.Mode)
	if v := c.Volume; v != nil {
		num := func(n int64) string {
			if n == 0 {
				return ""
			}
			return strconv.FormatInt(n, 10)
		}
		set(keyChunkSize, num(v.ChunkSize))
		set(keyCacheSize, num(v.CacheSize))
		set(keySize, num(v.Size))
		set(keyEncryption, map[bool]string{true: "on", false: "off"}[v.Encryption])
		set(keyCipher, v.Cipher)
		set(keyKeySize, strconv.Itoa(v.KeySize))
		set(keyGenInterval, v.Generations.Interval.String())
		set(keyGenKeep, strconv.Itoa(v.Generations.Keep))
		set(keyGenMaxAge, v.Generations.MaxAge.String())
		set(keyGenDirtyWatermark, num(v.Generations.DirtyWatermark))
	}
	return m
}

// DecodeContext reads a volume context. It returns InvalidArgument for a
// missing or malformed key, for any unknown key that starts with "miami/",
// and for anything NewContext would not have produced (it never cleans or
// repairs a value); keys without that prefix are ignored. The subdir and mode
// keys may be absent (an absent mode is DefaultMode).
func DecodeContext(m map[string]string) (Context, error) {
	var unknown []string
	for k := range m {
		if !strings.HasPrefix(k, contextPrefix) {
			continue
		}
		switch k {
		case keyKind, keyTransportKind, keyTransportServer, keyTransportExport, keyTransportPath, keySubdir, keyMode, keyTakeoverTimeout:
		default:
			if slices.Contains(volumeKeys, k) {
				continue
			}
			unknown = append(unknown, k)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return Context{}, invalid("unknown volume context key " + quote(unknown[0]))
	}

	for _, k := range []string{keyKind, keyTransportKind} {
		if m[k] == "" {
			return Context{}, invalid("volume context key " + k + " is required")
		}
	}

	c := Context{
		Kind: Kind(m[keyKind]),
		Transport: transport.Spec{
			Kind:   m[keyTransportKind],
			Server: m[keyTransportServer],
			Export: m[keyTransportExport],
			Path:   m[keyTransportPath],
		},
		Subdir: m[keySubdir],
		Mode:   m[keyMode],
	}
	if c.Kind == KindVolume {
		v, err := decodeVolume(m)
		if err != nil {
			return Context{}, err
		}
		c.Volume = &v
	} else {
		for _, k := range volumeKeys {
			if _, set := m[k]; set {
				return Context{}, invalid("volume context key " + k + " is only for kind " + string(KindVolume))
			}
		}
	}
	if err := c.Validate(); err != nil {
		return Context{}, err
	}
	return c, nil
}

// NewContext parses StorageClass parameters, resolves the subdir template
// and validates the result, for CreateVolume.
func NewContext(params map[string]string) (Context, error) {
	p, err := ParseParameters(params)
	if err != nil {
		return Context{}, err
	}
	subdir, err := ResolveSubdir(p.SubdirTemplate, p.Tokens)
	if err != nil {
		return Context{}, err
	}
	c := Context{Kind: p.Kind, Transport: p.Transport, Subdir: subdir, Mode: p.Mode, Volume: p.Volume}
	if err := c.Validate(); err != nil {
		return Context{}, err
	}
	return c, nil
}

// decodeVolume reads the volume keys of a context. Every one is required and
// must be in the form Encode writes.
func decodeVolume(m map[string]string) (VolumeParams, error) {
	var v VolumeParams
	for _, k := range volumeKeys {
		if m[k] == "" {
			return v, invalid("volume context key " + k + " is required")
		}
	}
	num := func(k string) (int64, error) {
		n, err := strconv.ParseInt(m[k], 10, 64)
		if err != nil || n <= 0 || strconv.FormatInt(n, 10) != m[k] {
			return 0, invalid("volume context key " + k + " must be a positive number")
		}
		return n, nil
	}
	dur := func(k string) (time.Duration, error) {
		d, err := time.ParseDuration(m[k])
		if err != nil || d <= 0 || d.String() != m[k] {
			return 0, invalid("volume context key " + k + " must be a positive duration")
		}
		return d, nil
	}
	var err error
	if v.ChunkSize, err = num(keyChunkSize); err != nil {
		return v, err
	}
	if v.CacheSize, err = num(keyCacheSize); err != nil {
		return v, err
	}
	if v.Size, err = num(keySize); err != nil {
		return v, err
	}
	if v.Generations.DirtyWatermark, err = num(keyGenDirtyWatermark); err != nil {
		return v, err
	}
	switch m[keyEncryption] {
	case "on":
		v.Encryption = true
	case "off":
	default:
		return v, invalid("volume context key " + keyEncryption + " must be on or off")
	}
	v.Cipher = m[keyCipher]
	ks, err := num(keyKeySize)
	if err != nil {
		return v, err
	}
	v.KeySize = int(ks)
	keep, err := num(keyGenKeep)
	if err != nil {
		return v, err
	}
	v.Generations.Keep = int(keep)
	if v.Generations.Interval, err = dur(keyGenInterval); err != nil {
		return v, err
	}
	if v.Generations.MaxAge, err = dur(keyGenMaxAge); err != nil {
		return v, err
	}
	if v.Size%v.ChunkSize != 0 {
		return v, invalid("volume context key " + keySize + " must be a whole number of chunks")
	}
	return v, nil
}
