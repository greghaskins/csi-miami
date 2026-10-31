package volume

import (
	"sort"
	"strings"

	"github.com/greghaskins/csi-miami/internal/transport"
)

// StorageClass parameter names.
const (
	ParamKind   = "kind"
	ParamSubdir = "subdir"
)

// Parameters the external-provisioner injects when --extra-create-metadata
// is on. ParseParameters reads them for token values.
const (
	paramPVCName      = "csi.storage.k8s.io/pvc/name"
	paramPVCNamespace = "csi.storage.k8s.io/pvc/namespace"
	paramPVName       = "csi.storage.k8s.io/pv/name"

	// providerPrefix marks every parameter the provisioner injects; ones we
	// do not read are ignored rather than rejected.
	providerPrefix = "csi.storage.k8s.io/"
)

// Params are the StorageClass parameters of a volume, parsed and checked but
// with the subdir template not yet expanded.
type Params struct {
	Kind           Kind
	Transport      transport.Spec
	SubdirTemplate string // DefaultSubdir when the StorageClass sets none
	Tokens         Tokens // from the provisioner's extra create metadata
}

// ParseParameters parses and validates StorageClass parameters. Parameter
// names are case-sensitive and an unknown one is InvalidArgument, so a typo
// cannot silently mean the default. Keys under csi.storage.k8s.io/ other
// than the three the provisioner uses for tokens are ignored.
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
		case ParamKind, ParamSubdir,
			transport.ParamKind, transport.ParamServer, transport.ParamExport, transport.ParamPath:
		default:
			if strings.HasPrefix(k, providerPrefix) {
				continue
			}
			return Params{}, invalid("unknown parameter " + quote(k))
		}
	}

	p := Params{
		Kind:           Kind(params[ParamKind]),
		SubdirTemplate: DefaultSubdir,
		Tokens: Tokens{
			PVCNamespace: params[paramPVCNamespace],
			PVCName:      params[paramPVCName],
			PVName:       params[paramPVName],
		},
	}
	if _, set := params[ParamKind]; !set {
		return Params{}, invalid("parameter " + ParamKind + " is required (directory)")
	}
	if !p.Kind.Valid() {
		return Params{}, invalid("parameter " + ParamKind + " must be directory")
	}

	spec, err := transport.ParseSpec(params)
	if err != nil {
		return Params{}, err
	}
	p.Transport = spec

	if tmpl, set := params[ParamSubdir]; set {
		if tmpl == "" {
			return Params{}, invalid("parameter " + ParamSubdir + " must not be empty; omit it for the default " + DefaultSubdir)
		}
		p.SubdirTemplate = tmpl
	}
	return p, nil
}

// quote renders an untrusted key for an error message.
func quote(s string) string {
	const limit = 64
	if len(s) > limit {
		s = s[:limit] + "..."
	}
	return "\"" + strings.Map(func(r rune) rune {
		if r < ' ' || r == '"' || r == '\\' || r == 0x7f {
			return '?'
		}
		return r
	}, s) + "\""
}
