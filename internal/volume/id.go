package volume

import (
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Kind is the kind of item a volume is. Adding a kind means adding a
// constant here and listing it in Valid.
type Kind string

// KindDirectory is a directory on a transport.
const KindDirectory Kind = "directory"

const (
	// maxNameLen is the room reserved for a volume name, in bytes, when
	// sizing a subdir (see maxSubdirLen) and the ceiling checked against a
	// kind by TestNameLimitFitsEveryKind: with the longest kind, "directory:",
	// the ID is then exactly maxIDLen. The names actually accepted are the 40
	// bytes of pvc-<uuid>.
	maxNameLen = 118

	// maxIDLen is the longest volume ID CSI allows, in bytes.
	maxIDLen = 128
)

// IsPVName reports whether name has the shape the external-provisioner gives
// a PV, pvc-<uuid> with the UUID in lower-case hex. It is the one shape a
// volume name may have, and the one shape a subdir element may not.
func IsPVName(name string) bool { return pvNameShape.MatchString(name) }

// Valid reports whether k is a kind this driver knows.
func (k Kind) Valid() bool {
	switch k {
	case KindDirectory:
		return true
	default:
		return false
	}
}

func invalid(msg string) error {
	return status.Error(codes.InvalidArgument, msg)
}

// ID identifies a volume. Its string form is "<kind>:<name>".
type ID struct {
	Kind Kind
	Name string // what the provisioner passed as the CSI name, e.g. pvc-<uuid>
}

// String returns the volume ID as sent over CSI.
func (id ID) String() string {
	return string(id.Kind) + ":" + id.Name
}

// Validate reports whether the ID is well formed. The error carries the
// gRPC code InvalidArgument.
func (id ID) Validate() error {
	if !id.Kind.Valid() {
		return invalid("volume ID kind is not a known kind (directory)")
	}
	return validateName(id.Name)
}

// validateName requires exactly the name the external-provisioner gives a
// volume, pvc-<uuid> with the UUID in lower-case hex (see IsPVName). Every
// boundary that makes or reads a volume ID goes through here, so a volume's
// name is always of that shape and never one a subdir element can have: no
// directory a subdir template can produce is a volume, and no volume's
// directory can nest inside another's. A static PV (migration, adoption) must
// keep or be given such a name.
func validateName(name string) error {
	if name == "" {
		return invalid("volume ID name must not be empty")
	}
	if !IsPVName(name) {
		return invalid("volume ID name must be a provisioner name: pvc- and a lower-case UUID")
	}
	return nil
}

// NewID returns the ID for a kind and a name, or InvalidArgument if either
// is not acceptable.
func NewID(kind Kind, name string) (ID, error) {
	id := ID{Kind: kind, Name: name}
	if err := id.Validate(); err != nil {
		return ID{}, err
	}
	return id, nil
}

// ParseID is the inverse of ID.String. It returns InvalidArgument for
// anything malformed.
func ParseID(s string) (ID, error) {
	kind, name, ok := strings.Cut(s, ":")
	if !ok {
		return ID{}, invalid("volume ID must have the form <kind>:<name>")
	}
	return NewID(Kind(kind), name)
}
