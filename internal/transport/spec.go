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

// idLen is the number of hex characters of the digest kept in an ID.
const idLen = 12

// Spec is what a StorageClass describes: where a transport's directory comes
// from.
type Spec struct {
	Kind   string // "nfs3" | "local"
	Server string // nfs3: host name or IP literal
	Export string // nfs3: absolute export path
	Path   string // local: absolute host path
}

func invalid(msg string) error {
	return status.Error(codes.InvalidArgument, msg)
}

// Validate reports whether the Spec is well formed. The error carries the
// gRPC code InvalidArgument.
//
// Paths need not be cleaned: a trailing slash or a doubled slash is accepted
// and normalised by Canonical. A ".." element is always rejected.
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
	if strings.HasPrefix(server, "[") {
		if _, ok := parseIPv6Literal(server); !ok {
			return invalid(ParamServer + ": a bracketed address must be an IPv6 literal such as [2001:db8::1]")
		}
		return nil
	}
	if strings.ContainsAny(server, ":[]") {
		return invalid(ParamServer + " must not contain ':' unless it is a bracketed IPv6 literal")
	}
	return nil
}

// parseIPv6Literal parses "[addr]" where addr is an IPv6 address without a
// zone.
func parseIPv6Literal(s string) (netip.Addr, bool) {
	if len(s) < 3 || s[0] != '[' || s[len(s)-1] != ']' {
		return netip.Addr{}, false
	}
	addr, err := netip.ParseAddr(s[1 : len(s)-1])
	if err != nil || !addr.Is6() || addr.Zone() != "" {
		return netip.Addr{}, false
	}
	return addr, true
}

func validatePath(field, p string) error {
	if p == "" {
		return invalid(field + " is required")
	}
	if strings.ContainsRune(p, 0) {
		return invalid(field + " must not contain NUL")
	}
	if !path.IsAbs(p) {
		return invalid(field + " must be an absolute path")
	}
	for _, elem := range strings.Split(p, "/") {
		if elem == ".." {
			return invalid(field + " must not contain '..'")
		}
	}
	return nil
}

// canonicalServer lower-cases a host name and writes an IPv6 literal in its
// shortest form, so spellings of one address agree.
func canonicalServer(server string) string {
	if addr, ok := parseIPv6Literal(server); ok {
		return "[" + addr.String() + "]"
	}
	return strings.ToLower(server)
}

func cleanPath(p string) string {
	if p == "" {
		return ""
	}
	return path.Clean(p)
}

// Canonical returns one deterministic line for the Spec:
// "nfs3://<server><export>" or "local://<path>", with paths cleaned and the
// server lower-cased. It is only meaningful for a Spec that passes Validate.
func (s Spec) Canonical() string {
	switch s.Kind {
	case KindNFS3:
		return s.Kind + "://" + canonicalServer(s.Server) + cleanPath(s.Export)
	case KindLocal:
		return s.Kind + "://" + cleanPath(s.Path)
	default:
		return s.Kind + "://" + canonicalServer(s.Server) + cleanPath(s.Export) + cleanPath(s.Path)
	}
}

// ID returns the first 12 hex characters of the SHA-256 of Canonical. Two
// Specs that differ only by a trailing slash or by the case of the server
// have the same ID.
func (s Spec) ID() string {
	sum := sha256.Sum256([]byte(s.Canonical()))
	return hex.EncodeToString(sum[:])[:idLen]
}

// MountPoint is where the transport is mounted under root:
// <root>/transports/<ID>.
func (s Spec) MountPoint(root string) string {
	return filepath.Join(root, "transports", s.ID())
}

// ParseSpec builds a validated Spec from StorageClass parameters. It reads
// only the transport.* keys and ignores the rest; rejecting unknown keys is
// the caller's job, since the other keys belong to the caller.
func ParseSpec(params map[string]string) (Spec, error) {
	s := Spec{
		Kind:   params[ParamKind],
		Server: params[ParamServer],
		Export: params[ParamExport],
		Path:   params[ParamPath],
	}
	if err := s.Validate(); err != nil {
		return Spec{}, err
	}
	return s, nil
}
