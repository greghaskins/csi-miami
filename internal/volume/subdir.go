package volume

import (
	"path"
	"regexp"
	"strings"
)

const (
	// Limits of the Linux path interface.
	maxElementLen = 255
	maxPathLen    = 4096

	// maxSubdirLen leaves room for "/" and the longest volume name, so that
	// the subdir plus the name always fits in maxPathLen.
	maxSubdirLen = maxPathLen - 1 - maxNameLen
)

// pvNameShape is what the external-provisioner names a PV. No element of a
// subdir may look like one, so a claim named like another volume cannot
// place a directory inside that volume's.
var pvNameShape = regexp.MustCompile(`^pvc-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// Tokens are the values a subdir template may use. They come from the
// provisioner's extra create metadata. The volume's own name is not a token:
// it is always appended after the subdir, from the volume ID.
type Tokens struct {
	PVCNamespace string // ${pvc.namespace}
	PVCName      string // ${pvc.name}
}

// lookup returns the value of a token and whether the name is a known one.
func (t Tokens) lookup(name string) (string, bool) {
	switch name {
	case "pvc.namespace":
		return t.PVCNamespace, true
	case "pvc.name":
		return t.PVCName, true
	default:
		return "", false
	}
}

// ResolveSubdir expands the ${pvc.namespace} and ${pvc.name} tokens in
// template and returns the cleaned, relative prefix. The empty string means
// no prefix, and is the result for an empty template and for one that cleans
// to ".".
//
// Each token the template uses must have a value that is a single non-empty
// path element without control characters. The result must be relative and
// must not contain a ".." element, a control character, an unresolved
// "${", an element over 255 bytes or an element shaped like a PV name.
// Anything else, including an unknown token, is InvalidArgument.
func ResolveSubdir(template string, tokens Tokens) (string, error) {
	if template == "" {
		return "", nil
	}
	if hasControl(template) {
		return "", invalid("subdir must not contain control characters")
	}

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
			return "", invalid("subdir uses unknown token " + quote(name) + "; known tokens are ${pvc.namespace} and ${pvc.name}")
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
	if path.IsAbs(resolved) {
		return "", invalid("subdir must be a relative path")
	}
	// Cleaning would resolve a ".." away, so look for it first.
	for _, elem := range strings.Split(resolved, "/") {
		if elem == ".." {
			return "", invalid("subdir must not contain '..'")
		}
	}
	cleaned := path.Clean(resolved)
	if cleaned == "." {
		return "", nil
	}
	if err := validateSubdir(cleaned); err != nil {
		return "", err
	}
	return cleaned, nil
}

func validateTokenValue(name, value string) error {
	switch {
	case value == "":
		return invalid("subdir token ${" + name + "} is empty; the provisioner must run with --extra-create-metadata")
	case strings.Contains(value, "/"):
		return invalid("subdir token ${" + name + "} must be a single path element without '/'")
	case value == "." || value == "..":
		return invalid("subdir token ${" + name + "} must not be '.' or '..'")
	case hasControl(value):
		return invalid("subdir token ${" + name + "} must not contain control characters")
	case len(value) > maxElementLen:
		return invalid("subdir token ${" + name + "} must be at most 255 bytes")
	}
	return nil
}

// hasControl reports whether s has a byte below 0x20 or 0x7f.
func hasControl(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] == 0x7f {
			return true
		}
	}
	return false
}

// validateSubdir checks a non-empty subdir exactly: it must already be in
// the form ResolveSubdir returns. It is the one check shared by NewContext
// and DecodeContext, so the node is never more lenient than the controller.
func validateSubdir(s string) error {
	if s == "" {
		return invalid("subdir must not be empty")
	}
	if hasControl(s) {
		return invalid("subdir must not contain control characters")
	}
	if strings.Contains(s, "${") {
		return invalid("subdir has an unresolved token")
	}
	if path.IsAbs(s) {
		return invalid("subdir must be a relative path")
	}
	if len(s) > maxSubdirLen {
		return invalid("subdir must be at most 3977 bytes")
	}
	for _, elem := range strings.Split(s, "/") {
		switch {
		case elem == "..":
			return invalid("subdir must not contain '..'")
		case elem == "" || elem == ".":
			return invalid("subdir must be a cleaned path (no trailing, doubled or '.' elements)")
		case len(elem) > maxElementLen:
			return invalid("subdir must not have a path element longer than 255 bytes")
		case pvNameShape.MatchString(elem):
			return invalid("subdir must not have an element shaped like a PV name (pvc-<uuid>), so that no directory can nest inside another volume's")
		}
	}
	return nil
}
