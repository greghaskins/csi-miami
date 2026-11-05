package volume

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func volParams(overrides map[string]string) map[string]string {
	o := map[string]string{"kind": "volume"}
	for k, v := range overrides {
		o[k] = v
	}
	return nfsParams(o)
}

func TestVolumeParamsDefaults(t *testing.T) {
	t.Parallel()
	c, err := NewContext(volParams(nil))
	if err != nil {
		t.Fatal(err)
	}
	want := &VolumeParams{
		Encryption: true, Cipher: "xchacha12,aes-adiantum-plain64", KeySize: 256,
		Generations: GenerationParams{Interval: 5 * time.Minute, MaxAge: time.Hour, Keep: 3},
	}
	if !reflect.DeepEqual(c.Volume, want) {
		t.Errorf("defaults = %+v, want %+v", c.Volume, want)
	}
	if _, has := mustSize(t, c).Encode()["miami/takeoverTimeout"]; has {
		t.Error("a new volume's context carries a takeoverTimeout")
	}

	sized, err := c.WithSize(1 << 30)
	if err != nil {
		t.Fatal(err)
	}
	v := sized.Volume
	if v.ChunkSize != 4<<20 || v.Size != 1<<30 || v.CacheSize != 1<<30 || v.Generations.DirtyWatermark != 512<<20 {
		t.Errorf("sized = %+v; want chunk 4Mi, size and cache 1Gi, watermark half the cache", v)
	}
	if c.Volume.ChunkSize != 0 {
		t.Error("WithSize changed its receiver")
	}
}

func TestChunkSizeDerivation(t *testing.T) {
	t.Parallel()
	const GiB, TiB = int64(1) << 30, int64(1) << 40
	tests := []struct{ size, want int64 }{
		{1 * GiB, 4 << 20},
		{256 * GiB, 4 << 20}, // exactly 65536 chunks of 4 MiB
		{256*GiB + 1, 8 << 20},
		{512 * GiB, 8 << 20},
		{4 * TiB, 64 << 20},
		{16 * TiB, 256 << 20},
	}
	base, err := NewContext(volParams(nil))
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range tests {
		c, err := base.WithSize(tt.size)
		if err != nil {
			t.Errorf("size %d: %v", tt.size, err)
			continue
		}
		if c.Volume.ChunkSize != tt.want {
			t.Errorf("size %d: chunk %d, want %d", tt.size, c.Volume.ChunkSize, tt.want)
		}
	}
	// Beyond 256 MiB x 65536 nothing fits.
	if _, err := base.WithSize(17 * TiB); code(err) != codes.InvalidArgument {
		t.Errorf("17 TiB: %v, want InvalidArgument", err)
	}
}

func TestSizeRoundsUpToWholeChunks(t *testing.T) {
	t.Parallel()
	base, _ := NewContext(volParams(nil))
	for _, tt := range []struct{ in, want int64 }{
		{1, 4 << 20}, {4 << 20, 4 << 20}, {4<<20 + 1, 8 << 20}, {17 << 20, 20 << 20},
	} {
		c, err := base.WithSize(tt.in)
		if err != nil || c.Volume.Size != tt.want {
			t.Errorf("size %d: got %+v, %v; want %d", tt.in, c.Volume, err, tt.want)
		}
	}
	explicit, _ := NewContext(volParams(map[string]string{"chunkSize": "16Mi"}))
	if c, err := explicit.WithSize(100 << 20); err != nil || c.Volume.Size != 112<<20 || c.Volume.ChunkSize != 16<<20 {
		t.Errorf("explicit chunk: %+v, %v", c.Volume, err)
	}
	if _, err := base.WithSize(0); code(err) != codes.InvalidArgument {
		t.Errorf("size 0: %v", err)
	}
}

func code(err error) codes.Code { return status.Code(err) }

func TestVolumeParamsRejects(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		params map[string]string
	}{
		{"chunk not a power of two", map[string]string{"chunkSize": "5Mi"}},
		{"chunk under 1 MiB", map[string]string{"chunkSize": "512Ki"}},
		{"chunk over 256 MiB", map[string]string{"chunkSize": "512Mi"}},
		{"chunk not a number", map[string]string{"chunkSize": "big"}},
		{"cache below chunk", map[string]string{"chunkSize": "8Mi", "cacheSize": "4Mi"}},
		{"keep 0", map[string]string{"generations.keep": "0"}},
		{"keep not a number", map[string]string{"generations.keep": "many"}},
		{"interval under 1m", map[string]string{"generations.interval": "30s"}},
		{"maxAge under interval", map[string]string{"generations.interval": "10m", "generations.maxAge": "5m"}},
		{"maxAge under the default interval", map[string]string{"generations.maxAge": "1m"}},
		{"cipher not allowed", map[string]string{"cipher": "aes-cbc-essiv"}},
		{"adiantum with 512 bits", map[string]string{"keySize": "512"}},
		{"xts with 128 bits", map[string]string{"cipher": "aes-xts-plain64", "keySize": "128"}},
		{"encryption maybe", map[string]string{"encryption": "maybe"}},
		{"watermark over cache", map[string]string{"cacheSize": "64Mi", "generations.dirtyWatermark": "128Mi"}},
		{"takeoverTimeout, whatever its value", map[string]string{"takeoverTimeout": "10m"}},
		{"takeoverTimeout under 1m", map[string]string{"takeoverTimeout": "10s"}},
		{"takeoverTimeout empty", map[string]string{"takeoverTimeout": ""}},
		{"local transport", map[string]string{"transport.kind": "local", "transport.path": "/mnt/data", "transport.server": del, "transport.export": del}},
		{"volume parameter on a directory", map[string]string{"kind": "directory", "chunkSize": "4Mi"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewContext(volParams(tt.params))
			if code(err) != codes.InvalidArgument {
				t.Errorf("NewContext = %v, want InvalidArgument", err)
			}
		})
	}
	// Valid alternatives.
	for _, ok := range []map[string]string{
		{"cipher": "aes-xts-plain64", "keySize": "512"},
		{"cipher": "aes-xts-plain64"},
		{"encryption": "off"},
		{"chunkSize": "1Mi", "cacheSize": "1Mi"},
		{"generations.interval": "1m", "generations.maxAge": "1m"},
	} {
		if _, err := NewContext(volParams(ok)); err != nil {
			t.Errorf("%v: %v", ok, err)
		}
	}
}

func TestContextRoundTripsVolume(t *testing.T) {
	t.Parallel()
	base, err := NewContext(volParams(map[string]string{"encryption": "off", "generations.keep": "5"}))
	if err != nil {
		t.Fatal(err)
	}
	c, err := base.WithSize(10 << 30)
	if err != nil {
		t.Fatal(err)
	}
	enc := c.Encode()
	for _, k := range []string{"miami/chunkSize", "miami/cacheSize", "miami/size", "miami/encryption", "miami/cipher", "miami/keySize",
		"miami/generations.interval", "miami/generations.keep", "miami/generations.maxAge", "miami/generations.dirtyWatermark"} {
		if enc[k] == "" {
			t.Errorf("Encode lacks %s: %v", k, enc)
		}
	}
	if enc["miami/encryption"] != "off" || enc["miami/kind"] != "volume" {
		t.Errorf("encoded %v", enc)
	}
	got, err := DecodeContext(enc)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(c) {
		t.Errorf("round trip\n got %+v\nwant %+v", got.Volume, c.Volume)
	}
	// An unresolved context (no size) is not a stored volume.
	delete(enc, "miami/size")
	if _, err := DecodeContext(enc); code(err) != codes.InvalidArgument {
		t.Errorf("decode without size: %v", err)
	}
}

func TestDecodeRejectsVolumeKeysOnDirectory(t *testing.T) {
	t.Parallel()
	dir, err := NewContext(nfsParams(nil))
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range volumeKeys {
		m := dir.Encode()
		m[k] = "1"
		if _, err := DecodeContext(m); code(err) != codes.InvalidArgument {
			t.Errorf("directory with %s: %v, want InvalidArgument", k, err)
		}
	}
	m := dir.Encode()
	m["miami/nonsense"] = "1"
	if _, err := DecodeContext(m); code(err) != codes.InvalidArgument {
		t.Errorf("unknown miami key: %v", err)
	}
	// And a directory's encoding carries none of them.
	for _, k := range volumeKeys {
		if _, set := dir.Encode()[k]; set {
			t.Errorf("a directory encodes %s", k)
		}
	}
}

func TestIDKindVolume(t *testing.T) {
	t.Parallel()
	id, err := ParseID("volume:" + pvUUID1)
	if err != nil || id.Kind != KindVolume {
		t.Errorf("ParseID = %+v, %v", id, err)
	}
	if !KindVolume.Valid() {
		t.Error("KindVolume is not valid")
	}
}

func mustSize(t *testing.T, c Context) Context {
	t.Helper()
	sz, err := c.WithSize(1 << 30)
	if err != nil {
		t.Fatal(err)
	}
	return sz
}

// Decision 52 removed the timed takeover: a new volume cannot ask for one, and
// the message says why. A PV made before that still carries the key in its
// context and must still stage; the key means nothing there.
func TestTakeoverTimeoutIsRejectedForNewVolumesAndIgnoredOnOld(t *testing.T) {
	t.Parallel()
	_, err := NewContext(volParams(map[string]string{"takeoverTimeout": "10m"}))
	if code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "decision 52") || !strings.Contains(err.Error(), "takeoverTimeout") {
		t.Fatalf("NewContext = %v, want InvalidArgument naming decision 52", err)
	}

	c, err := NewContext(volParams(nil))
	if err != nil {
		t.Fatal(err)
	}
	enc := mustSize(t, c).Encode()
	for name, val := range map[string]string{"a duration": "10m0s", "junk": "soon", "zero": "0s"} {
		old := map[string]string{}
		for k, v := range enc {
			old[k] = v
		}
		old["miami/takeoverTimeout"] = val
		got, err := DecodeContext(old)
		if err != nil {
			t.Errorf("%s: a PV carrying takeoverTimeout does not decode: %v", name, err)
			continue
		}
		if got.Volume == nil || got.Volume.Size != 1<<30 {
			t.Errorf("%s: decoded = %+v", name, got.Volume)
		}
		if _, has := got.Encode()["miami/takeoverTimeout"]; has {
			t.Errorf("%s: the ignored key is carried forward", name)
		}
	}
}
