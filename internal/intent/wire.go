package intent

import (
	"time"

	"github.com/greghaskins/csi-miami/internal/transport"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// wireIntent is the on-disk form of an Intent. Its tags are the format; the
// golden file under testdata/ pins it. Change it only under the rules in the
// documentation of Version.
type wireIntent struct {
	Version   int             `json:"version"`
	VolumeID  string          `json:"volumeID"`
	Kind      string          `json:"kind"`
	Transport transportRecord `json:"transport"`
	Subdir    string          `json:"subdir"`
	// Mode is informational for a version 1 reader, which ignores it; a
	// record without it reads as empty, the default mode.
	Mode       string    `json:"mode,omitempty"`
	StagePath  string    `json:"stagePath"`
	Publishes  []Publish `json:"publishes,omitempty"`
	MountFlags []string  `json:"mountFlags,omitempty"`
	AccessMode string    `json:"accessMode"`
	// EnsureItem is informational for a version 1 reader, which ignores it
	// (and then never creates a directory); a record without it reads false.
	EnsureItem bool `json:"ensureItem,omitempty"`
	// Generation is informational for a version 1 reader, which ignores it;
	// a record without it reads as 0.
	Generation uint64 `json:"generation"`
	// Volume and Epoch exist only for the volume kind; a version 1 reader
	// ignores them.
	Volume *volumeRecord `json:"volume,omitempty"`
	Epoch  uint64        `json:"epoch,omitempty"`
}

// volumeRecord is the on-disk form of a volume.VolumeParams, with its own
// explicit keys. Durations are nanoseconds.
type volumeRecord struct {
	ChunkSize      int64  `json:"chunkSize"`
	CacheSize      int64  `json:"cacheSize"`
	Size           int64  `json:"size"`
	Encryption     bool   `json:"encryption"`
	Cipher         string `json:"cipher"`
	KeySize        int    `json:"keySize"`
	Interval       int64  `json:"interval"`
	MaxAge         int64  `json:"maxAge"`
	Keep           int    `json:"keep"`
	DirtyWatermark int64  `json:"dirtyWatermark"`
	// TakeoverTimeout is nanoseconds; absent means the default.
	TakeoverTimeout int64 `json:"takeoverTimeout,omitempty"`
}

// transportRecord is the on-disk form of a transport.Spec, with its own
// explicit keys so that nothing about transport.Spec's field names or tags
// can change the format.
type transportRecord struct {
	Kind   string `json:"kind"`
	Server string `json:"server,omitempty"`
	Export string `json:"export,omitempty"`
	Path   string `json:"path,omitempty"`
}

func toWire(in Intent) wireIntent {
	var vr *volumeRecord
	if v := in.Volume; v != nil {
		vr = &volumeRecord{
			ChunkSize: v.ChunkSize, CacheSize: v.CacheSize, Size: v.Size, Encryption: v.Encryption,
			Cipher: v.Cipher, KeySize: v.KeySize, Interval: int64(v.Generations.Interval),
			MaxAge: int64(v.Generations.MaxAge), Keep: v.Generations.Keep, DirtyWatermark: v.Generations.DirtyWatermark,
			TakeoverTimeout: int64(v.TakeoverTimeout),
		}
	}
	return wireIntent{
		Volume:   vr,
		Epoch:    in.Epoch,
		Version:  in.Version,
		VolumeID: in.VolumeID,
		Kind:     string(in.Kind),
		Transport: transportRecord{
			Kind:   in.Transport.Kind,
			Server: in.Transport.Server,
			Export: in.Transport.Export,
			Path:   in.Transport.Path,
		},
		Subdir:     in.Subdir,
		Mode:       in.Mode,
		StagePath:  in.StagePath,
		Publishes:  in.Publishes,
		MountFlags: in.MountFlags,
		AccessMode: in.AccessMode,
		EnsureItem: in.EnsureItem,
		Generation: in.Generation,
	}
}

func fromWire(w wireIntent) Intent {
	var vp *volume.VolumeParams
	if r := w.Volume; r != nil {
		vp = &volume.VolumeParams{
			ChunkSize: r.ChunkSize, CacheSize: r.CacheSize, Size: r.Size, Encryption: r.Encryption,
			Cipher: r.Cipher, KeySize: r.KeySize, TakeoverTimeout: time.Duration(r.TakeoverTimeout),
			Generations: volume.GenerationParams{
				Interval: time.Duration(r.Interval), MaxAge: time.Duration(r.MaxAge),
				Keep: r.Keep, DirtyWatermark: r.DirtyWatermark,
			},
		}
	}
	return Intent{
		Volume:   vp,
		Epoch:    w.Epoch,
		Version:  w.Version,
		VolumeID: w.VolumeID,
		Kind:     volume.Kind(w.Kind),
		Transport: transport.Spec{
			Kind:   w.Transport.Kind,
			Server: w.Transport.Server,
			Export: w.Transport.Export,
			Path:   w.Transport.Path,
		},
		Subdir:     w.Subdir,
		Mode:       w.Mode,
		StagePath:  w.StagePath,
		Publishes:  w.Publishes,
		MountFlags: w.MountFlags,
		AccessMode: w.AccessMode,
		EnsureItem: w.EnsureItem,
		Generation: w.Generation,
	}
}
