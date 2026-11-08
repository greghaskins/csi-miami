package volume

import (
	"testing"
)

const (
	mi = int64(1) << 20
	gi = int64(1) << 30
	ti = int64(1) << 40
)

func TestCacheFootprintPinned(t *testing.T) {
	// This table is the pin: a change to it is a design change (decision 66).
	for _, tc := range []struct {
		name                       string
		size, cacheSize, watermark int64
		want                       int64
	}{
		{"cache equals size", 10 * gi, 10 * gi, 5 * gi, 15376 * mi},
		{"lazy volume", 100 * gi, 8 * gi, 4 * gi, 12304 * mi},
		{"small volume under a big cache", 1 * gi, 10 * gi, 5 * gi, 6160 * mi},
		{"tiny", 4 * mi, 4 * mi, 2 * mi, 22 * mi},
		{"rounds up", 1 * gi, 1 * gi, 1000000, 1041 * mi},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := VolumeParams{Size: tc.size, CacheSize: tc.cacheSize}
			v.Generations.DirtyWatermark = tc.watermark
			if got := CacheFootprint(v); got != tc.want {
				t.Fatalf("CacheFootprint = %d (%d MiB), want %d (%d MiB)", got, got/mi, tc.want, tc.want/mi)
			}
		})
	}
}

func TestCacheFootprintMatchesWithSize(t *testing.T) {
	for _, params := range []map[string]string{
		{},
		{"cacheSize": "8Gi"},
		{"cacheSize": "8Gi", "generations.dirtyWatermark": "1Gi"},
	} {
		for _, size := range []int64{1 * gi, 100 * gi, 1 * ti} {
			p := nfsParams(map[string]string{"kind": "volume"})
			for k, v := range params {
				p[k] = v
			}
			c, err := NewContext(p)
			if err != nil {
				t.Fatalf("NewContext(%v): %v", params, err)
			}
			ctx, err := c.WithSize(size)
			if err != nil {
				t.Fatalf("WithSize(%d) for %v: %v", size, params, err)
			}
			v := *ctx.Volume
			raw := min(v.Size, v.CacheSize) + v.Generations.DirtyWatermark + 16*mi
			want := (raw + mi - 1) / mi * mi
			if got := CacheFootprint(v); got != want {
				t.Errorf("params %v size %d: CacheFootprint = %d, want %d", params, size, got, want)
			}
		}
	}
}

func TestCacheFootprintUnresolvedIsZero(t *testing.T) {
	for _, v := range []VolumeParams{
		{},
		{CacheSize: 8 * gi},
		{Size: -1},
	} {
		if got := CacheFootprint(v); got != 0 {
			t.Errorf("CacheFootprint(%+v) = %d, want 0", v, got)
		}
	}
}

func TestCacheFootprintSaturates(t *testing.T) {
	v := VolumeParams{Size: 1 << 62, CacheSize: 1 << 62}
	v.Generations.DirtyWatermark = 1 << 62
	got := CacheFootprint(v)
	if got != MaxFootprint {
		t.Fatalf("CacheFootprint = %d, want MaxFootprint %d", got, MaxFootprint)
	}
	if MaxFootprint <= 0 || MaxFootprint%mi != 0 {
		t.Fatalf("MaxFootprint = %d, want a positive multiple of 1 MiB", MaxFootprint)
	}
	// Defaults derived from a huge size must not overflow either.
	if got := CacheFootprint(VolumeParams{Size: 1<<63 - 1}); got != MaxFootprint {
		t.Fatalf("CacheFootprint(max size) = %d, want MaxFootprint", got)
	}
}

func TestAddFootprintSaturates(t *testing.T) {
	if got := AddFootprint(MaxFootprint, 1<<20); got != MaxFootprint {
		t.Errorf("AddFootprint(Max, 1Mi) = %d, want MaxFootprint", got)
	}
	if got := AddFootprint(3<<20, 5<<20); got != 8<<20 {
		t.Errorf("AddFootprint(3Mi, 5Mi) = %d, want 8Mi", got)
	}
}

func TestServedFootprint(t *testing.T) {
	whole := VolumeParams{ChunkSize: 4 * mi, Size: 10 * gi, CacheSize: 10 * gi}
	whole.Generations.DirtyWatermark = 5 * gi
	lazy := VolumeParams{ChunkSize: 4 * mi, Size: 100 * gi, CacheSize: 8 * gi}
	lazy.Generations.DirtyWatermark = 4 * gi
	for _, tc := range []struct {
		name   string
		p      VolumeParams
		served int64
		want   int64
	}{
		{"not grown", whole, 0, CacheFootprint(whole)},
		{"smaller served size changes nothing", whole, 1 * gi, CacheFootprint(whole)},
		{"whole-volume cache follows the growth", whole, 20 * gi, CacheFootprint(VolumeParams{Size: 20 * gi, CacheSize: 20 * gi, Generations: whole.Generations})},
		{"growth rounds up to whole chunks", whole, 20*gi + 1, CacheFootprint(VolumeParams{Size: 20*gi + 4*mi, CacheSize: 20*gi + 4*mi, Generations: whole.Generations})},
		{"partial cache keeps its bound", lazy, 200 * gi, CacheFootprint(lazy)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ServedFootprint(tc.p, tc.served); got != tc.want {
				t.Fatalf("ServedFootprint = %d MiB, want %d MiB", got/mi, tc.want/mi)
			}
		})
	}
}
