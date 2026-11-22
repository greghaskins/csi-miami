package main

import (
	"fmt"
	"regexp"
)

// The pin file is deploy/kustomize/base/kustomization.yaml (the
// kustomization const).
// Its images entry for imageName carries the content tag and the digest:
//
//	images:
//	  - name: ghcr.io/greghaskins/csi-miami
//	    newTag: r-<64 hex>
//	    digest: sha256:<64 hex>
//
// kustomize renders that as imageName:r-<64 hex>@sha256:<64 hex>. The runtime
// pulls by the digest; the tag says which inputs the image was built from.
// The pin file is not an image input (hack/content-hash.sh), so pinning never
// changes the tag.
const imageName = "ghcr.io/greghaskins/csi-miami"

var (
	tagLine = regexp.MustCompile(`(?m)^(\s+newTag: )(\S*)$`)
)

// pin is what a pin file names: the content tag and the image digest.
type pin struct{ Tag, Digest string }

func (p pin) String() string { return imageName + ":" + p.Tag + "@" + p.Digest }

// readPin returns the pin in kustomization content. A file without a newTag
// line (from before every commit pinned) gives an empty Tag and no error;
// callers decide what that means. The digest line must be there.
func readPin(content []byte) (pin, error) {
	d, err := getDigest(content)
	if err != nil {
		return pin{}, err
	}
	m := tagLine.FindAllSubmatch(content, -1)
	switch len(m) {
	case 0:
		return pin{Digest: d}, nil
	case 1:
		return pin{Tag: string(m[0][2]), Digest: d}, nil
	default:
		return pin{}, fmt.Errorf("want at most one newTag line in %s, found %d", kustomization, len(m))
	}
}

// writePin sets the tag and digest in kustomization content. A missing
// newTag line is added just above the digest line, with its indentation.
func writePin(content []byte, p pin) ([]byte, error) {
	if !rTagRE.MatchString(p.Tag) {
		return nil, fmt.Errorf("tag %q is not r-<64 hex>", p.Tag)
	}
	out, err := setDigest(content, p.Digest)
	if err != nil {
		return nil, err
	}
	switch n := len(tagLine.FindAll(out, -1)); n {
	case 0:
		return digestLine.ReplaceAllFunc(out, func(line []byte) []byte {
			indent := digestLine.FindSubmatch(line)[1]
			indent = indent[:len(indent)-len("digest: ")]
			return append([]byte(string(indent)+"newTag: "+p.Tag+"\n"), line...)
		}), nil
	case 1:
		return tagLine.ReplaceAll(out, []byte("${1}"+p.Tag)), nil
	default:
		return nil, fmt.Errorf("want at most one newTag line in %s, found %d", kustomization, n)
	}
}
