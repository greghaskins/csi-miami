package transport

import (
	"crypto/sha256"
	"encoding/hex"
	"net/netip"
	"path"
	"path/filepath"
	"strings"
	"unicode"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Transport kinds.
const (
	// KindNFS3 is an NFSv3 export mounted over TCP.
	KindNFS3 = "nfs3"
	// KindLocal is a directory on the host, bind-mounted.
	KindLocal = "local"
)

// StorageClass parameter names that ParseSpec reads.
const (
	ParamKind   = "transport.kind"
	ParamServer = "transport.server"
	ParamExport = "transport.export"
	ParamPath   = "transport.path"
)

const (
	// idLen is the number of hex characters of the digest kept in an ID.
	idLen = 12

	// Limits of the Linux path interface.
	maxElementLen = 255
	maxPathLen    = 4096

	// Limits of a host name (RFC 1035, RFC 1123).
	maxHostLen  = 253
	maxLabelLen = 63
)

// Spec is what a StorageClass describes: where a transport's directory comes
// from.
//
// A valid Spec is in normal form: paths are cleaned, a host name is lower
// case ASCII. Validate rejects anything else, so the strings a Spec carries (and a volume context stores)
// can be compared byte for byte. Normalize and ParseSpec produce the normal
// form from a looser spelling.
type Spec struct {
	Kind   string // "nfs3" | "local"
	Server string // nfs3: ASCII host name or dotted IPv4 literal; IPv6 is not supported
	Export string // nfs3: absolute export path, not "/"
	Path   string // local: absolute host path, not "/"
}

func invalid(msg string) error {
	return status.Error(codes.InvalidArgument, msg)
}

// Validate reports whether the Spec is well formed and in normal form. The
// error carries the gRPC code InvalidArgument.
func (s Spec) Validate() error {
	switch s.Kind {
	case KindNFS3:
		if err := validateServer(s.Server); err != nil {
			return err
		}
		if err := validatePath(ParamExport, s.Export); err != nil {
			return err
		}
		if s.Path != "" {
			return invalid(ParamPath + " must be empty for " + ParamKind + " nfs3")
		}
	case KindLocal:
		if err := validatePath(ParamPath, s.Path); err != nil {
			return err
		}
		if s.Server != "" {
			return invalid(ParamServer + " must be empty for " + ParamKind + " local")
		}
		if s.Export != "" {
			return invalid(ParamExport + " must be empty for " + ParamKind + " local")
		}
	case "":
		return invalid(ParamKind + " is required (nfs3 or local)")
	default:
		return invalid(ParamKind + " must be nfs3 or local")
	}
	return nil
}

func validateServer(server string) error {
	if server == "" {
		return invalid(ParamServer + " is required for " + ParamKind + " nfs3")
	}
	for _, r := range server {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return invalid(ParamServer + " must not contain whitespace or control characters")
		}
		if r == '/' {
			return invalid(ParamServer + " must not contain '/'")
		}
	}
	switch {
	case strings.HasPrefix(server, "["):
		return invalid(ParamServer + ": IPv6 is not supported; use an IPv4 address or a host name")
	case strings.Contains(server, ":"):
		return invalid(ParamServer + " must not contain ':' (a port is not allowed, and IPv6 is not supported; use an IPv4 address or a host name)")
	case strings.ContainsAny(server, "[]"):
		return invalid(ParamServer + " must not contain '[' or ']'")
	}
	return validateHostname(server)
}

// validateHostname accepts an ASCII host name (RFC 1123 labels of letters,
// digits and hyphens) or a dotted IPv4 literal, lower case only.
func validateHostname(h string) error {
	if len(h) > maxHostLen {
		return invalid(ParamServer + " must be at most 253 bytes")
	}
	labels := strings.Split(h, ".")
	for _, l := range labels {
		if l == "" || len(l) > maxLabelLen {
			return invalid(ParamServer + " labels must be 1 to 63 bytes")
		}
		if l[0] == '-' || l[len(l)-1] == '-' {
			return invalid(ParamServer + " labels must not start or end with '-'")
		}
		for i := 0; i < len(l); i++ {
			c := l[i]
			switch {
			case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-':
			case c >= 'A' && c <= 'Z':
				return invalid(ParamServer + " must be lower case")
			default:
				return invalid(ParamServer + " may contain only a-z, 0-9, '-' and '.'")
			}
		}
	}
	// A name whose last label is all digits can only be an IPv4 literal.
	if last := labels[len(labels)-1]; strings.Trim(last, "0123456789") == "" {
		addr, err := netip.ParseAddr(h)
		if err != nil || !addr.Is4() || addr.String() != h {
			return invalid(ParamServer + " looks like an IPv4 address but is not a valid dotted-decimal one")
		}
	}
	return nil
}

func validatePath(field, p string) error {
	if p == "" {
		return invalid(field + " is required")
	}
	for i := 0; i < len(p); i++ {
		if p[i] < 0x20 || p[i] == 0x7f {
			return invalid(field + " must not contain control characters")
		}
	}
	if !path.IsAbs(p) {
		return invalid(field + " must be an absolute path")
	}
	if len(p) > maxPathLen {
		return invalid(field + " must be at most 4096 bytes")
	}
	for _, elem := range strings.Split(p, "/") {
		if elem == ".." {
			return invalid(field + " must not contain '..'")
		}
		if len(elem) > maxElementLen {
			return invalid(field + " must not have a path element longer than 255 bytes")
		}
	}
	if p == "/" {
		return invalid(field + " must not be the filesystem root")
	}
	if path.Clean(p) != p {
		return invalid(field + " must be a cleaned path (no trailing, doubled or '.' elements)")
	}
	return nil
}

// asciiLower lower-cases ASCII letters only, so that no Unicode case folding
// can make two different names collide.
func asciiLower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + ('a' - 'A')
		}
	}
	return string(b)
}

// normalizePath cleans p unless it has a ".." element: cleaning would
// resolve the element away and hide it from Validate.
func normalizePath(p string) string {
	if p == "" {
		return ""
	}
	for _, elem := range strings.Split(p, "/") {
		if elem == ".." {
			return p
		}
	}
	return path.Clean(p)
}

// Normalize returns the Spec with its server lower-cased (ASCII only) and
// its paths cleaned. It does
// not make an invalid Spec valid: a path with a ".." element is left alone.
func (s Spec) Normalize() Spec {
	s.Server = asciiLower(s.Server)
	s.Export = normalizePath(s.Export)
	s.Path = normalizePath(s.Path)
	return s
}

// Canonical returns one deterministic line for the Spec:
// "nfs3://<server><export>" or "local://<path>", from the normal form. It is
// meaningful only after Validate succeeds; for a Spec of unknown kind it
// returns the empty string.
func (s Spec) Canonical() string {
	n := s.Normalize()
	switch n.Kind {
	case KindNFS3:
		return n.Kind + "://" + n.Server + n.Export
	case KindLocal:
		return n.Kind + "://" + n.Path
	default:
		return ""
	}
}

// ID returns the first 12 hex characters of the SHA-256 of Canonical. Two
// Specs that differ only by a trailing slash or by the case of the server
// have the same ID. It is meaningful only after Validate succeeds.
func (s Spec) ID() string {
	sum := sha256.Sum256([]byte(s.Canonical()))
	return hex.EncodeToString(sum[:])[:idLen]
}

// MountPoint is where the transport is mounted under root:
// <root>/transports/<ID>. It returns InvalidArgument if the Spec is invalid
// or root is not an absolute, cleaned path.
func (s Spec) MountPoint(root string) (string, error) {
	if err := s.Validate(); err != nil {
		return "", err
	}
	if !path.IsAbs(root) || path.Clean(root) != root {
		return "", invalid("transports root must be an absolute, cleaned path")
	}
	return filepath.Join(root, "transports", s.ID()), nil
}

// ParseSpec builds a validated Spec in normal form from StorageClass
// parameters: paths are cleaned (a trailing slash is accepted), the server
// is lower-cased before validation. It
// reads only the transport.* keys and ignores the rest; rejecting unknown
// keys is the caller's job, since the other keys belong to the caller.
func ParseSpec(params map[string]string) (Spec, error) {
	s := Spec{
		Kind:   params[ParamKind],
		Server: params[ParamServer],
		Export: params[ParamExport],
		Path:   params[ParamPath],
	}.Normalize()
	if err := s.Validate(); err != nil {
		return Spec{}, err
	}
	return s, nil
}
