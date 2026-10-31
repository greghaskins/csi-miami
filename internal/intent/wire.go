package intent

import (
	"github.com/greghaskins/csi-miami/internal/transport"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// wireIntent is the on-disk form of an Intent. Its tags are the format; the
// golden file under testdata/ pins it. Change it only under the rules in the
// documentation of Version.
type wireIntent struct {
	Version    int             `json:"version"`
	VolumeID   string          `json:"volumeID"`
	Kind       string          `json:"kind"`
	Transport  transportRecord `json:"transport"`
	Subdir     string          `json:"subdir"`
	StagePath  string          `json:"stagePath"`
	Publishes  []Publish       `json:"publishes,omitempty"`
	MountFlags []string        `json:"mountFlags,omitempty"`
	AccessMode string          `json:"accessMode"`
	// EnsureItem is informational for a version 1 reader, which ignores it
	// (and then never creates a directory); a record without it reads false.
	EnsureItem bool `json:"ensureItem,omitempty"`
	// Generation is informational for a version 1 reader, which ignores it;
	// a record without it reads as 0.
	Generation uint64 `json:"generation"`
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
	return wireIntent{
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
		StagePath:  in.StagePath,
		Publishes:  in.Publishes,
		MountFlags: in.MountFlags,
		AccessMode: in.AccessMode,
		EnsureItem: in.EnsureItem,
		Generation: in.Generation,
	}
}

func fromWire(w wireIntent) Intent {
	return Intent{
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
		StagePath:  w.StagePath,
		Publishes:  w.Publishes,
		MountFlags: w.MountFlags,
		AccessMode: w.AccessMode,
		EnsureItem: w.EnsureItem,
		Generation: w.Generation,
	}
}
