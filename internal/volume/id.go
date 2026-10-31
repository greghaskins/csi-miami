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

// maxNameLen is the longest volume name, in bytes.
const maxNameLen = 200

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

// validateName allows [A-Za-z0-9._-], 1 to 200 bytes. "." and ".." are
// refused as well so that a name is never a path traversal if it is ever
// used as a file name.
func validateName(name string) error {
	if name == "" {
		return invalid("volume ID name must not be empty")
	}
	if len(name) > maxNameLen {
		return invalid("volume ID name must be at most 200 bytes")
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '.', c == '_', c == '-':
		default:
			return invalid("volume ID name may contain only A-Z, a-z, 0-9, '.', '_' and '-'")
		}
	}
	if name == "." || name == ".." {
		return invalid("volume ID name must not be '.' or '..'")
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
