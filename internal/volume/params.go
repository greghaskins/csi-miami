package volume

import (
	"sort"
	"strings"
	"unicode"

	"github.com/greghaskins/csi-miami/internal/transport"
)

// StorageClass parameter names.
const (
	ParamKind   = "kind"
	ParamSubdir = "subdir"
	ParamMode   = "mode"
)

// Parameters the external-provisioner injects when --extra-create-metadata
// is on. ParseParameters reads them for token values. The PV name it also
// injects is deliberately not read: the volume's name comes from the volume
// ID alone.
const (
	paramPVCName      = "csi.storage.k8s.io/pvc/name"
	paramPVCNamespace = "csi.storage.k8s.io/pvc/namespace"

	// providerPrefix marks every parameter the provisioner injects; ones we
	// do not read are ignored rather than rejected.
	providerPrefix = "csi.storage.k8s.io/"
)

// Params are the StorageClass parameters of a volume, parsed and checked but
// with the subdir template not yet expanded.
type Params struct {
	Kind           Kind
	Transport      transport.Spec
	SubdirTemplate string // empty when the StorageClass sets none: no prefix
	Mode           string // canonical four-digit octal text; empty when the StorageClass sets none: DefaultMode
	Tokens         Tokens // from the provisioner's extra create metadata
}

// ParseParameters parses and validates StorageClass parameters. Parameter
// names are case-sensitive and an unknown one is InvalidArgument, so a typo
// cannot silently mean the default. Keys under csi.storage.k8s.io/ other
// than the two the provisioner uses for tokens are ignored. An empty or
// absent subdir means no prefix, and an empty or absent mode means
// DefaultMode; a mode that is set must satisfy ParseMode.
func ParseParameters(params map[string]string) (Params, error) {
	// Sorted, so that which unknown key is named does not depend on map
	// order.
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		switch k {
		case ParamKind, ParamSubdir, ParamMode,
			transport.ParamKind, transport.ParamServer, transport.ParamExport, transport.ParamPath:
		default:
			if strings.HasPrefix(k, providerPrefix) {
				continue
			}
			return Params{}, invalid("unknown parameter " + quote(k))
		}
	}

	if _, set := params[ParamKind]; !set {
		return Params{}, invalid("parameter " + ParamKind + " is required (directory)")
	}
	p := Params{
		Kind:           Kind(params[ParamKind]),
		SubdirTemplate: params[ParamSubdir],
		Tokens: Tokens{
			PVCNamespace: params[paramPVCNamespace],
			PVCName:      params[paramPVCName],
		},
	}
	if !p.Kind.Valid() {
		return Params{}, invalid("parameter " + ParamKind + " must be directory")
	}

	if raw := params[ParamMode]; raw != "" {
		_, canonical, err := ParseMode(raw)
		if err != nil {
			return Params{}, err
		}
		p.Mode = canonical
	}

	spec, err := transport.ParseSpec(params)
	if err != nil {
		return Params{}, err
	}
	p.Transport = spec
	return p, nil
}

// quote renders untrusted text (a key or a token name) for an error
// message: truncated, with anything that is not printable replaced.
func quote(s string) string {
	const limit = 64
	if len(s) > limit {
		s = s[:limit] + "..."
	}
	return "\"" + strings.Map(func(r rune) rune {
		if !unicode.IsPrint(r) || r == '"' || r == '\\' {
			return '?'
		}
		return r
	}, s) + "\""
}
