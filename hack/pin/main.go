// Command pin pins the base's driver image by content tag and digest
// (deploy/kustomize/base/kustomization.yaml). See docs/releasing.md. The
// `pin` check itself runs in CI from greghaskins/release-checks.
//
//	go run ./hack/pin pin          pin the current branch
package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

const kustomization = "deploy/kustomize/base/kustomization.yaml"

var (
	digestRE   = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	digestLine = regexp.MustCompile(`(?m)^(\s+digest: )(sha256:[0-9a-f]{64})$`)
	rTagRE     = regexp.MustCompile(`^r-[0-9a-f]{64}$`)
	// pinnedChecksRE finds the release-checks commit ci.yml calls pin.yml at,
	// only on a whole `uses:` line, so a comment that mentions pin.yml cannot
	// be read as the pinned SHA.
	pinnedChecksRE = regexp.MustCompile(`(?m)^\s*uses:\s*greghaskins/release-checks/\.github/workflows/pin\.yml@([0-9a-f]{40})\s*(#.*)?$`)
)

type command func(args []string, stdout, stderr io.Writer) int

var commands = map[string]command{}

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 {
		if cmd, ok := commands[args[0]]; ok {
			return cmd(args[1:], stdout, stderr)
		}
	}
	names := make([]string, 0, len(commands))
	for n := range commands {
		names = append(names, n)
	}
	sort.Strings(names)
	_, _ = fmt.Fprintf(stderr, "usage: go run ./hack/pin <%s> [args]\n", strings.Join(names, "|"))
	return 2
}

// contentTag computes the content tag of rev with release-checks'
// content-tag.sh, fetched at the SHA .github/workflows/ci.yml pins pin.yml
// at: the same script and SHA the `pin` check runs, so the two cannot
// disagree. It reads .github/image.json from rev, never the work tree.
func contentTag(rev string) (string, error) {
	top, err := runner("git")("rev-parse", "--show-toplevel")
	if err != nil {
		return "", err
	}
	wf, err := os.ReadFile(filepath.Join(top, ciWorkflowFile)) //nolint:gosec // the repo's own workflow
	if err != nil {
		return "", err
	}
	m := pinnedChecksRE.FindSubmatch(wf)
	if m == nil {
		return "", failf("%s has no `uses: greghaskins/release-checks/.github/workflows/pin.yml@<40 hex>` line, so the pin script cannot tell which content-tag.sh to run", ciWorkflowFile)
	}
	sha := string(m[1])
	script, err := runner("gh")("api", "-H", "Accept: application/vnd.github.raw",
		"repos/greghaskins/release-checks/contents/.github/scripts/content-tag.sh?ref="+sha)
	if err != nil {
		return "", failf("fetching release-checks' content-tag.sh at %s failed: %v\ncheck `gh auth status` and the network, then run the pin script again", sha[:12], err)
	}
	f, err := os.CreateTemp("", "content-tag-*.sh")
	if err != nil {
		return "", err
	}
	defer func() { _ = os.Remove(f.Name()) }()
	if _, err := f.WriteString(script + "\n"); err != nil {
		return "", err
	}
	if err := f.Close(); err != nil {
		return "", err
	}
	t, err := runner("bash")(f.Name(), top, rev)
	if err != nil {
		// content-tag.sh names the broken rule: "content-tag: <rule>: <detail>".
		return "", failf("computing the content tag failed: %v\nfix .github/image.json or the file it names, commit, then run the pin script again", err)
	}
	if !rTagRE.MatchString(t) {
		return "", fmt.Errorf("content-tag.sh printed %q, want r-<64 hex>", t)
	}
	return t, nil
}

// ciWorkflowFile is where the pin script reads the release-checks SHA.
const ciWorkflowFile = ".github/workflows/ci.yml"

// setDigest rewrites the pinned digest in kustomization content.
func setDigest(content []byte, digest string) ([]byte, error) {
	if !digestRE.MatchString(digest) {
		return nil, fmt.Errorf("digest %q is not sha256:<64 hex>", digest)
	}
	if n := len(digestLine.FindAll(content, -1)); n != 1 {
		return nil, fmt.Errorf("want exactly one digest line in %s, found %d", kustomization, n)
	}
	return digestLine.ReplaceAll(content, []byte("${1}"+digest)), nil
}

func getDigest(content []byte) (string, error) {
	m := digestLine.FindAllSubmatch(content, -1)
	if len(m) != 1 {
		return "", fmt.Errorf("want exactly one digest line in %s, found %d", kustomization, len(m))
	}
	return string(m[0][2]), nil
}
