package intent

import (
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
	"unicode"

	"github.com/greghaskins/csi-miami/internal/transport"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// Version is the record format version written by Save and required by Load.
//
// The format is owned by this package: records are written and read through
// private wire structs with explicit tags (see wire.go), never through the
// field names of types defined elsewhere. Changing the format follows three
// rules. Any field that a version 1 reader must not ignore needs a version
// bump. A purely informational field may be added without one, because
// readers tolerate unknown fields. (Generation was added that way: a reader
// that does not know it ignores it, and a record without it reads as 0.) And
// a change to the golden file under testdata/ requires a version decision in
// the same commit.
const Version = 1

// maxPathLen is the longest path accepted in a record, in bytes.
const maxPathLen = 4096

// maxEscapedIDLen bounds the escaped volume ID so that the record file name
// and its temp file name (a dot, ".json", ".tmp-" and 16 hex digits more)
// stay within the 255-byte file name limit.
const maxEscapedIDLen = 220

// Access modes a record may hold: the names of the CSI
// VolumeCapability.AccessMode.Mode enum.
var accessModes = map[string]struct{}{
	"SINGLE_NODE_WRITER":        {},
	"SINGLE_NODE_READER_ONLY":   {},
	"MULTI_NODE_READER_ONLY":    {},
	"MULTI_NODE_SINGLE_WRITER":  {},
	"MULTI_NODE_MULTI_WRITER":   {},
	"SINGLE_NODE_SINGLE_WRITER": {},
	"SINGLE_NODE_MULTI_WRITER":  {},
}

// Publish is one target path a staged volume is bound to.
type Publish struct {
	TargetPath string `json:"targetPath"`
	ReadOnly   bool   `json:"readOnly"`
}

// Intent says what should be true on this node for one volume. It mirrors
// volume.Context (Kind, Transport, Subdir) plus the volume ID.
//
// Its JSON form is fixed by this package, not by the field names here or in
// transport.Spec; see Version.
type Intent struct {
	// Version is always Version on write; a record with another version
	// fails to load.
	Version   int
	VolumeID  string
	Kind      volume.Kind
	Transport transport.Spec
	// Subdir is the optional prefix under the transport, empty for none;
	// the item's directory is <transport mount>/<Subdir>/<volume name>, and
	// volume.Context.Dir computes it.
	Subdir     string
	StagePath  string
	Publishes  []Publish
	MountFlags []string
	// AccessMode is the name of a CSI access mode, such as
	// "MULTI_NODE_MULTI_WRITER".
	AccessMode string

	// Generation counts the writes of the record: the store sets it to one
	// more than the record it replaces (1 for a new record, whatever the
	// caller put here), on every Save and Update. It tells a record that was
	// rewritten, even with identical content, from the one a caller read:
	// DeleteIf compares it, so a Stage repeated with the same parameters
	// while a teardown is running is not deleted along with it. A record
	// written before the field existed reads as 0. It is not part of what
	// the record says should be true.
	Generation uint64
}

// ID returns the volume ID in the record, split into kind and name. It
// checks only the structure (a kind, a colon and a name, no control
// characters), not the boundary rules of volume.ParseID, so a record that was
// accepted when it was written can always be identified.
func (in Intent) ID() (volume.ID, error) {
	kind, name, err := splitID(in.VolumeID)
	if err != nil {
		return volume.ID{}, err
	}
	return volume.ID{Kind: kind, Name: name}, nil
}

// Context returns the volume context the record mirrors: its kind, transport
// and subdir.
func (in Intent) Context() volume.Context {
	return volume.Context{Kind: in.Kind, Transport: in.Transport, Subdir: in.Subdir}
}

// Validate reports whether the Intent is a structurally sound record, which
// is what the reconciler needs in order to act on it safely. It is not the
// CSI boundary policy: rules such as the host name grammar, name shapes and
// length limits belong to the volume and transport packages and are applied
// when a request arrives, so tightening them never makes a record that is
// already on disk fail to load.
//
// A record is sound when: the version is Version; the volume ID has the form
// <kind>:<name> with Kind known and equal to the ID's kind; the transport
// kind is known, the fields it needs are set and the others empty; every
// path (export, local path, stage path, publish targets) is absolute,
// cleaned, free of control characters, shorter than 4096 bytes and not "/";
// the subdir is empty or relative, cleaned and free of ".." and control
// characters; publish targets are unique, none is the stage path or inside
// it, and none is inside another; the access mode is a CSI access mode name;
// and every mount flag is non-empty without commas or control characters.
//
// Error text names the field and the rule, never the value, so that a field
// which later holds a secret cannot leak through an error.
func (in Intent) Validate() error {
	if in.Version != Version {
		return fmt.Errorf("version %d is not supported (want %d)", in.Version, Version)
	}
	kind, _, err := splitID(in.VolumeID)
	if err != nil {
		return err
	}
	if !in.Kind.Valid() {
		return errors.New("kind is not a known kind")
	}
	if kind != in.Kind {
		return errors.New("kind does not match the kind of volumeID")
	}
	if len(Escape(in.VolumeID)) > maxEscapedIDLen {
		return errors.New("volumeID is too long to be a file name")
	}
	if err := validateTransport(in.Transport); err != nil {
		return err
	}
	if err := validateSubdir(in.Subdir); err != nil {
		return err
	}
	if err := checkAbsPath("stagePath", in.StagePath); err != nil {
		return err
	}
	if err := validatePublishes(in.StagePath, in.Publishes); err != nil {
		return err
	}
	if _, ok := accessModes[in.AccessMode]; !ok {
		return errors.New("accessMode is not a CSI access mode name")
	}
	for i, f := range in.MountFlags {
		switch {
		case f == "":
			return fmt.Errorf("mountFlags[%d] is empty", i)
		case strings.Contains(f, ","):
			return fmt.Errorf("mountFlags[%d] contains a comma", i)
		case hasControl(f):
			return fmt.Errorf("mountFlags[%d] contains a control character", i)
		}
	}
	return nil
}

// splitID splits "<kind>:<name>" without applying boundary rules.
func splitID(s string) (volume.Kind, string, error) {
	kind, name, ok := strings.Cut(s, ":")
	if !ok || kind == "" || name == "" {
		return "", "", errors.New("volumeID must have the form <kind>:<name>")
	}
	if hasControl(s) {
		return "", "", errors.New("volumeID contains a control character")
	}
	return volume.Kind(kind), name, nil
}

func validateTransport(t transport.Spec) error {
	if hasControl(t.Server) {
		return errors.New("transport.server contains a control character")
	}
	switch t.Kind {
	case transport.KindNFS3:
		if t.Server == "" {
			return errors.New("transport.server is required for nfs3")
		}
		if t.Path != "" {
			return errors.New("transport.path must be empty for nfs3")
		}
		return checkAbsPath("transport.export", t.Export)
	case transport.KindLocal:
		if t.Server != "" {
			return errors.New("transport.server must be empty for local")
		}
		if t.Export != "" {
			return errors.New("transport.export must be empty for local")
		}
		return checkAbsPath("transport.path", t.Path)
	default:
		return errors.New("transport.kind is not a known transport kind")
	}
}

func validateSubdir(s string) error {
	if s == "" {
		return nil
	}
	if hasControl(s) {
		return errors.New("subdir contains a control character")
	}
	if len(s) >= maxPathLen {
		return errors.New("subdir is too long")
	}
	if path.IsAbs(s) {
		return errors.New("subdir must be a relative path")
	}
	for _, elem := range strings.Split(s, "/") {
		if elem == ".." {
			return errors.New("subdir must not contain '..'")
		}
	}
	if s == "." || path.Clean(s) != s {
		return errors.New("subdir must be a cleaned path (empty means none)")
	}
	return nil
}

func validatePublishes(stage string, ps []Publish) error {
	for i, p := range ps {
		field := fmt.Sprintf("publishes[%d].targetPath", i)
		if err := checkAbsPath(field, p.TargetPath); err != nil {
			return err
		}
		if p.TargetPath == stage || isUnder(p.TargetPath, stage) {
			return fmt.Errorf("%s must not be the stage path or inside it", field)
		}
		for j := range i {
			other := ps[j].TargetPath
			switch {
			case p.TargetPath == other:
				return fmt.Errorf("%s repeats publishes[%d].targetPath", field, j)
			case isUnder(p.TargetPath, other):
				return fmt.Errorf("%s is inside publishes[%d].targetPath", field, j)
			case isUnder(other, p.TargetPath):
				return fmt.Errorf("publishes[%d].targetPath is inside %s", j, field)
			}
		}
	}
	return nil
}

// isUnder reports whether p is strictly inside dir. Both must be cleaned
// absolute paths.
func isUnder(p, dir string) bool {
	return strings.HasPrefix(p, strings.TrimSuffix(dir, "/")+"/")
}

// checkAbsPath checks the structural rules of a path in a record.
func checkAbsPath(field, p string) error {
	switch {
	case p == "":
		return errors.New(field + " is required")
	case hasControl(p):
		return errors.New(field + " contains a control character")
	case len(p) >= maxPathLen:
		return errors.New(field + " is too long")
	case !path.IsAbs(p):
		return errors.New(field + " must be an absolute path")
	case p == "/":
		return errors.New(field + " must not be the root")
	}
	for _, elem := range strings.Split(p, "/") {
		if elem == ".." {
			return errors.New(field + " must not contain '..'")
		}
	}
	if path.Clean(p) != p {
		return errors.New(field + " must be a cleaned path")
	}
	return nil
}

// hasControl reports whether s contains NUL or any other control character.
func hasControl(s string) bool {
	for _, r := range s {
		if unicode.IsControl(r) {
			return true
		}
	}
	return false
}

// MarshalJSON encodes the record in the format Version 1 pins.
func (in Intent) MarshalJSON() ([]byte, error) {
	return json.Marshal(toWire(in))
}

// UnmarshalJSON decodes a record. Unknown fields are ignored, so that a
// downgrade does not turn every record into a load failure. It does not
// validate; Load and Update do.
func (in *Intent) UnmarshalJSON(data []byte) error {
	var w wireIntent
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	*in = fromWire(w)
	return nil
}
