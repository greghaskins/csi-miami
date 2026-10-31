package intent

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/greghaskins/csi-miami/internal/transport"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// Version is the record format version written by Save and required by Load.
const Version = 1

// Publish is one target path a staged volume is bound to.
type Publish struct {
	TargetPath string `json:"targetPath"`
	ReadOnly   bool   `json:"readOnly"`
}

// Intent says what should be true on this node for one volume.
type Intent struct {
	// Version is always Version on write; a record with another version
	// fails to load.
	Version   int            `json:"version"`
	VolumeID  string         `json:"volumeID"`
	Kind      volume.Kind    `json:"kind"`
	Transport transport.Spec `json:"transport"`
	// Name is the item's name inside the transport: the subdirectory of a
	// directory volume.
	Name       string    `json:"name"`
	StagePath  string    `json:"stagePath"`
	Publishes  []Publish `json:"publishes,omitempty"`
	MountFlags []string  `json:"mountFlags,omitempty"`
	AccessMode string    `json:"accessMode"`
}

// Validate reports whether the Intent is a well-formed record: the version is
// Version, the volume ID parses and agrees with Kind, the transport and the
// name pass the rules of the transport and volume packages, the stage path is
// absolute and clean, and every publish target is absolute, clean and unique.
func (in Intent) Validate() error {
	if in.Version != Version {
		return fmt.Errorf("version %d is not supported (want %d)", in.Version, Version)
	}
	id, err := volume.ParseID(in.VolumeID)
	if err != nil {
		return fmt.Errorf("volumeID: %w", err)
	}
	if id.Kind != in.Kind {
		return fmt.Errorf("kind %q does not match the kind of volumeID %q", in.Kind, in.VolumeID)
	}
	// The volume context carries the same three fields under the same
	// rules; reuse its check rather than restate it.
	ctx := volume.Context{Kind: in.Kind, Transport: in.Transport, Subdir: in.Name}
	if err := ctx.Validate(); err != nil {
		return fmt.Errorf("kind, transport or name: %w", err)
	}
	if err := checkPath("stagePath", in.StagePath); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(in.Publishes))
	for i, p := range in.Publishes {
		field := fmt.Sprintf("publishes[%d].targetPath", i)
		if err := checkPath(field, p.TargetPath); err != nil {
			return err
		}
		if _, dup := seen[p.TargetPath]; dup {
			return fmt.Errorf("%s %q is listed twice", field, p.TargetPath)
		}
		seen[p.TargetPath] = struct{}{}
	}
	return nil
}

func checkPath(field, p string) error {
	if p == "" {
		return errors.New(field + " is required")
	}
	if !filepath.IsAbs(p) {
		return fmt.Errorf("%s %q must be an absolute path", field, p)
	}
	if filepath.Clean(p) != p {
		return fmt.Errorf("%s %q must be a clean path", field, p)
	}
	return nil
}
