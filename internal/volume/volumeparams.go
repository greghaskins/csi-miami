package volume

import (
	"strconv"
	"strings"
	"time"

	"github.com/greghaskins/csi-miami/internal/chunk"
)

// StorageClass parameters of the volume kind.
const (
	ParamChunkSize          = "chunkSize"
	ParamCacheSize          = "cacheSize"
	ParamEncryption         = "encryption"
	ParamCipher             = "cipher"
	ParamKeySize            = "keySize"
	ParamGenInterval        = "generations.interval"
	ParamGenKeep            = "generations.keep"
	ParamGenMaxAge          = "generations.maxAge"
	ParamGenDirtyWatermark  = "generations.dirtyWatermark"
	ParamTakeoverTimeout    = "takeoverTimeout"
	cipherAdiantum          = "xchacha12,aes-adiantum-plain64"
	cipherXTS               = "aes-xts-plain64"
	MinGenerationInterval   = time.Minute
	maxKeySizeBits          = 512
	defaultEncryption       = true
	DefaultCipher           = cipherAdiantum
	DefaultKeySize          = 256
	DefaultGenerationKeep   = 3
	DefaultGenerationEvery  = 5 * time.Minute
	DefaultGenerationMaxAge = time.Hour
	DefaultTakeoverTimeout  = 5 * time.Minute
	minTakeoverTimeout      = time.Minute
)

// volumeParamNames are the StorageClass parameters only the volume kind has.
var volumeParamNames = map[string]bool{
	ParamChunkSize: true, ParamCacheSize: true, ParamEncryption: true, ParamCipher: true,
	ParamKeySize: true, ParamGenInterval: true, ParamGenKeep: true, ParamGenMaxAge: true,
	ParamGenDirtyWatermark: true, ParamTakeoverTimeout: true,
}

// GenerationParams say when a volume publishes a generation and how many it
// keeps.
type GenerationParams struct {
	Interval       time.Duration
	MaxAge         time.Duration
	Keep           int
	DirtyWatermark int64 // bytes of dirty chunks that force a boundary; 0 until the cache size is known
}

// VolumeParams are the parameters of a volume of the volume kind. After
// NewContext, ChunkSize, CacheSize, Size and DirtyWatermark are zero when the
// StorageClass left them to be derived; Context.WithSize resolves them once
// the capacity is known, and every Context that DecodeContext returns is
// resolved.
type VolumeParams struct { //nolint:revive // the name is fixed by the design (3.8)
	ChunkSize, CacheSize, Size int64
	Encryption                 bool
	Cipher                     string
	KeySize                    int
	Generations                GenerationParams
	// TakeoverTimeout is how long a holder may go without renewing before it
	// counts as dead; zero means DefaultTakeoverTimeout.
	TakeoverTimeout time.Duration
}

// check validates the fields that are set; derived fields still zero are not
// an error here (see resolved).
func (v VolumeParams) check() error {
	if v.ChunkSize != 0 {
		if err := chunk.CheckChunkSize(max(v.Size, 1), v.ChunkSize); err != nil {
			return invalid("chunkSize: " + err.Error())
		}
	}
	if v.CacheSize != 0 && v.ChunkSize != 0 && v.CacheSize < v.ChunkSize {
		return invalid("cacheSize must be at least chunkSize")
	}
	if v.CacheSize < 0 || v.Size < 0 {
		return invalid("sizes must not be negative")
	}
	switch v.Cipher {
	case cipherAdiantum:
		if v.KeySize != 256 {
			return invalid("keySize must be 256 for cipher " + cipherAdiantum)
		}
	case cipherXTS:
		if v.KeySize != 256 && v.KeySize != maxKeySizeBits {
			return invalid("keySize must be 256 or 512 for cipher " + cipherXTS)
		}
	default:
		return invalid("cipher must be " + cipherAdiantum + " or " + cipherXTS)
	}
	g := v.Generations
	switch {
	case g.Keep < 1:
		return invalid("generations.keep must be at least 1")
	case g.Interval < MinGenerationInterval:
		return invalid("generations.interval must be at least 1m")
	case g.MaxAge < g.Interval:
		return invalid("generations.maxAge must be at least generations.interval")
	case g.DirtyWatermark < 0:
		return invalid("generations.dirtyWatermark must not be negative")
	case g.DirtyWatermark != 0 && v.CacheSize != 0 && g.DirtyWatermark > v.CacheSize:
		return invalid("generations.dirtyWatermark must not exceed cacheSize")
	}
	return nil
}

// parseVolumeParams reads the volume parameters from a StorageClass map,
// with the defaults of the design.
func parseVolumeParams(params map[string]string) (VolumeParams, error) {
	v := VolumeParams{
		Encryption: defaultEncryption,
		Cipher:     DefaultCipher,
		KeySize:    DefaultKeySize,
		Generations: GenerationParams{
			Interval: DefaultGenerationEvery, MaxAge: DefaultGenerationMaxAge, Keep: DefaultGenerationKeep,
		},
	}
	var err error
	if raw, ok := params[ParamChunkSize]; ok {
		if v.ChunkSize, err = parseBytes(ParamChunkSize, raw); err != nil {
			return v, err
		}
	}
	if raw, ok := params[ParamCacheSize]; ok {
		if v.CacheSize, err = parseBytes(ParamCacheSize, raw); err != nil {
			return v, err
		}
	}
	if raw, ok := params[ParamGenDirtyWatermark]; ok {
		if v.Generations.DirtyWatermark, err = parseBytes(ParamGenDirtyWatermark, raw); err != nil {
			return v, err
		}
	}
	if raw, ok := params[ParamEncryption]; ok {
		switch raw {
		case "on":
			v.Encryption = true
		case "off":
			v.Encryption = false
		default:
			return v, invalid("parameter " + ParamEncryption + " must be on or off")
		}
	}
	if raw, ok := params[ParamCipher]; ok {
		v.Cipher = raw
	}
	if raw, ok := params[ParamKeySize]; ok {
		n, err := strconv.Atoi(raw)
		if err != nil {
			return v, invalid("parameter " + ParamKeySize + " must be a number of bits")
		}
		v.KeySize = n
	}
	if raw, ok := params[ParamGenKeep]; ok {
		n, err := strconv.Atoi(raw)
		if err != nil {
			return v, invalid("parameter " + ParamGenKeep + " must be a number")
		}
		v.Generations.Keep = n
	}
	if raw, ok := params[ParamGenInterval]; ok {
		if v.Generations.Interval, err = parseDuration(ParamGenInterval, raw); err != nil {
			return v, err
		}
	}
	if raw, ok := params[ParamGenMaxAge]; ok {
		if v.Generations.MaxAge, err = parseDuration(ParamGenMaxAge, raw); err != nil {
			return v, err
		}
	}
	if v.TakeoverTimeout, err = parseTakeoverTimeout(params); err != nil {
		return v, err
	}
	if err := v.check(); err != nil {
		return v, err
	}
	return v, nil
}

// parseTakeoverTimeout reads the optional takeoverTimeout parameter; zero
// means the default.
func parseTakeoverTimeout(params map[string]string) (time.Duration, error) {
	raw, ok := params[ParamTakeoverTimeout]
	if !ok {
		return 0, nil
	}
	d, err := parseDuration(ParamTakeoverTimeout, raw)
	if err != nil {
		return 0, err
	}
	if d < minTakeoverTimeout {
		return 0, invalid("parameter " + ParamTakeoverTimeout + " must be at least 1m")
	}
	return d, nil
}

// parseBytes reads a size: decimal bytes, or a number with a Ki, Mi, Gi or Ti
// suffix.
func parseBytes(name, raw string) (int64, error) {
	mult := int64(1)
	num := raw
	for suffix, m := range map[string]int64{"Ki": 1 << 10, "Mi": 1 << 20, "Gi": 1 << 30, "Ti": 1 << 40} {
		if strings.HasSuffix(raw, suffix) {
			num, mult = strings.TrimSuffix(raw, suffix), m
			break
		}
	}
	n, err := strconv.ParseInt(num, 10, 64)
	if err != nil || n <= 0 || n > (1<<62)/mult {
		return 0, invalid("parameter " + name + " must be a positive size in bytes (or with a Ki, Mi, Gi or Ti suffix)")
	}
	return n * mult, nil
}

func parseDuration(name, raw string) (time.Duration, error) {
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, invalid("parameter " + name + " must be a positive duration such as 5m")
	}
	return d, nil
}

// Takeover returns the takeover timeout, the default when unset.
func (v VolumeParams) Takeover() time.Duration {
	if v.TakeoverTimeout > 0 {
		return v.TakeoverTimeout
	}
	return DefaultTakeoverTimeout
}

// WithSize resolves a volume Context for a capacity of required bytes: the
// chunk size is derived when unset (the smallest power of two of at least 4
// MiB that keeps the size to 65536 chunks), the size is rounded up to whole
// chunks, and the cache size and dirty watermark take their defaults (the
// volume size, and half the cache). It is InvalidArgument for a Context
// that is not a volume's, for a size that is not positive, and for
// parameters that cannot hold that size.
func (c Context) WithSize(required int64) (Context, error) {
	if c.Volume == nil {
		return c, invalid("only a volume of kind " + string(KindVolume) + " has a size")
	}
	if required <= 0 {
		return c, invalid("a volume of kind " + string(KindVolume) + " needs a capacity: set requests.storage")
	}
	v := *c.Volume
	if v.ChunkSize == 0 {
		v.ChunkSize = chunk.DefaultChunkSize(required)
	}
	if err := chunk.CheckChunkSize(required, v.ChunkSize); err != nil {
		return c, invalid("chunkSize: " + err.Error())
	}
	v.Size = chunk.RoundUp(required, v.ChunkSize)
	if v.CacheSize == 0 {
		v.CacheSize = v.Size
	}
	if v.Generations.DirtyWatermark == 0 {
		v.Generations.DirtyWatermark = max(v.CacheSize/2, 1)
	}
	c.Volume = &v
	if err := c.Validate(); err != nil {
		return c, err
	}
	return c, nil
}

// Equal reports whether two Contexts hold the same values (Volume is
// compared by value, not by pointer).
func (c Context) Equal(o Context) bool {
	if c.Kind != o.Kind || c.Transport != o.Transport || c.Subdir != o.Subdir || c.Mode != o.Mode {
		return false
	}
	if (c.Volume == nil) != (o.Volume == nil) {
		return false
	}
	return c.Volume == nil || *c.Volume == *o.Volume
}
