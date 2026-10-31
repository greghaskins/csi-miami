package volume

import (
	"sort"
	"strings"

	"github.com/greghaskins/csi-miami/internal/transport"
)

// Volume context keys. Every key the driver owns starts with contextPrefix;
// Kubernetes adds keys of its own, which are ignored.
const (
	contextPrefix = "miami/"

	keyKind            = contextPrefix + "kind"
	keyTransportKind   = contextPrefix + "transport.kind"
	keyTransportServer = contextPrefix + "transport.server"
	keyTransportExport = contextPrefix + "transport.export"
	keyTransportPath   = contextPrefix + "transport.path"
	keySubdir          = contextPrefix + "subdir"
)

// Context is the volume context set by CreateVolume, stored on the PV and
// sent on every node call.
type Context struct {
	Kind      Kind
	Transport transport.Spec
	Subdir    string // resolved, relative, cleaned
}

// Validate reports whether the Context is complete and well formed. The
// error carries the gRPC code InvalidArgument.
func (c Context) Validate() error {
	if !c.Kind.Valid() {
		return invalid("volume context " + keyKind + " must be directory")
	}
	if err := c.Transport.Validate(); err != nil {
		return err
	}
	cleaned, err := cleanSubdir(c.Subdir)
	if err != nil {
		return err
	}
	if cleaned != c.Subdir {
		return invalid("subdir must be a cleaned path")
	}
	return nil
}

// Encode returns the volume context map. Empty values are omitted.
func (c Context) Encode() map[string]string {
	m := make(map[string]string, 6)
	set := func(k, v string) {
		if v != "" {
			m[k] = v
		}
	}
	set(keyKind, string(c.Kind))
	set(keyTransportKind, c.Transport.Kind)
	set(keyTransportServer, c.Transport.Server)
	set(keyTransportExport, c.Transport.Export)
	set(keyTransportPath, c.Transport.Path)
	set(keySubdir, c.Subdir)
	return m
}

// DecodeContext reads a volume context. It returns InvalidArgument for a
// missing or malformed key and for any unknown key that starts with
// "miami/"; keys without that prefix are ignored.
func DecodeContext(m map[string]string) (Context, error) {
	var unknown []string
	for k := range m {
		if !strings.HasPrefix(k, contextPrefix) {
			continue
		}
		switch k {
		case keyKind, keyTransportKind, keyTransportServer, keyTransportExport, keyTransportPath, keySubdir:
		default:
			unknown = append(unknown, k)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return Context{}, invalid("unknown volume context key " + quote(unknown[0]))
	}

	for _, k := range []string{keyKind, keyTransportKind, keySubdir} {
		if m[k] == "" {
			return Context{}, invalid("volume context key " + k + " is required")
		}
	}

	c := Context{
		Kind: Kind(m[keyKind]),
		Transport: transport.Spec{
			Kind:   m[keyTransportKind],
			Server: m[keyTransportServer],
			Export: m[keyTransportExport],
			Path:   m[keyTransportPath],
		},
		Subdir: m[keySubdir],
	}
	if !c.Kind.Valid() {
		return Context{}, invalid("volume context key " + keyKind + " must be directory")
	}
	subdir, err := cleanSubdir(c.Subdir)
	if err != nil {
		return Context{}, err
	}
	c.Subdir = subdir
	if err := c.Validate(); err != nil {
		return Context{}, err
	}
	return c, nil
}

// NewContext parses StorageClass parameters, resolves the subdir template
// and validates the result, for CreateVolume.
func NewContext(params map[string]string) (Context, error) {
	p, err := ParseParameters(params)
	if err != nil {
		return Context{}, err
	}
	subdir, err := ResolveSubdir(p.SubdirTemplate, p.Tokens)
	if err != nil {
		return Context{}, err
	}
	c := Context{Kind: p.Kind, Transport: p.Transport, Subdir: subdir}
	if err := c.Validate(); err != nil {
		return Context{}, err
	}
	return c, nil
}
