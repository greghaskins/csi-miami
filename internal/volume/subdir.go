package volume

import (
	"path"
	"strings"
)

// DefaultSubdir is the subdir template used when a StorageClass sets none.
const DefaultSubdir = "${pv.name}"

// Tokens are the values a subdir template may use. They come from the
// provisioner's extra create metadata.
type Tokens struct {
	PVCNamespace string // ${pvc.namespace}
	PVCName      string // ${pvc.name}
	PVName       string // ${pv.name}
}

// lookup returns the value of a token and whether the name is a known one.
func (t Tokens) lookup(name string) (string, bool) {
	switch name {
	case "pvc.namespace":
		return t.PVCNamespace, true
	case "pvc.name":
		return t.PVCName, true
	case "pv.name":
		return t.PVName, true
	default:
		return "", false
	}
}

// ResolveSubdir expands the ${pvc.namespace}, ${pvc.name} and ${pv.name}
// tokens in template and returns the cleaned, relative path.
//
// Each token the template uses must have a value that is a single non-empty
// path element. The result must be relative, must not contain a ".." element
// or NUL, and must not clean to ".". Anything else, including a token that
// is unknown or left unresolved, is InvalidArgument.
func ResolveSubdir(template string, tokens Tokens) (string, error) {
	var b strings.Builder
	rest := template
	for {
		i := strings.Index(rest, "${")
		if i < 0 {
			b.WriteString(rest)
			break
		}
		b.WriteString(rest[:i])
		rest = rest[i+2:]
		end := strings.IndexByte(rest, '}')
		if end < 0 {
			return "", invalid("subdir has an unterminated ${ token")
		}
		name := rest[:end]
		rest = rest[end+1:]
		value, known := tokens.lookup(name)
		if !known {
			return "", invalid("subdir uses unknown token ${" + name + "}; known tokens are ${pvc.namespace}, ${pvc.name} and ${pv.name}")
		}
		if err := validateTokenValue(name, value); err != nil {
			return "", err
		}
		b.WriteString(value)
	}
	resolved := b.String()

	// A token value cannot smuggle a "${" in: it would be an unresolved
	// token in the result.
	if strings.Contains(resolved, "${") {
		return "", invalid("subdir has an unresolved token")
	}
	return cleanSubdir(resolved)
}

func validateTokenValue(name, value string) error {
	switch {
	case value == "":
		return invalid("subdir token ${" + name + "} is empty; the provisioner must run with --extra-create-metadata")
	case strings.Contains(value, "/"):
		return invalid("subdir token ${" + name + "} must be a single path element without '/'")
	case value == "." || value == "..":
		return invalid("subdir token ${" + name + "} must not be '.' or '..'")
	}
	return nil
}

// cleanSubdir checks the rules that hold for any subdir, whether it was just
// resolved or read back from a volume context, and returns the cleaned path.
func cleanSubdir(s string) (string, error) {
	if s == "" {
		return "", invalid("subdir must not be empty")
	}
	if strings.ContainsRune(s, 0) {
		return "", invalid("subdir must not contain NUL")
	}
	if path.IsAbs(s) {
		return "", invalid("subdir must be a relative path")
	}
	for _, elem := range strings.Split(s, "/") {
		if elem == ".." {
			return "", invalid("subdir must not contain '..'")
		}
	}
	cleaned := path.Clean(s)
	if cleaned == "." {
		return "", invalid("subdir must not resolve to the transport root")
	}
	return cleaned, nil
}
