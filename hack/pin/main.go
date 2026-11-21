// Command pin keeps the base's driver image pinned by content tag and
// digest (deploy/kustomize/base/kustomization.yaml). See docs/releasing.md.
//
//	go run ./hack/pin pin          pin the current branch
//	go run ./hack/pin check BASE   the `pin` check CI runs
package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"
)

const kustomization = "deploy/kustomize/base/kustomization.yaml"

var (
	digestRE   = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	digestLine = regexp.MustCompile(`(?m)^(\s+digest: )(sha256:[0-9a-f]{64})$`)
	hashRE     = regexp.MustCompile(`^[0-9a-f]{16}$`)
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

// contentTag is the image tag of the checked-out tree: x- plus
// hack/content-hash.sh. That script hashes the working tree's index, so only
// HEAD of a clean checkout is supported.
func contentTag(rev string) (string, error) {
	if rev != "HEAD" {
		return "", fmt.Errorf("content tag of %s: only HEAD is supported", rev)
	}
	root, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return "", exitDetail(err)
	}
	cmd := exec.Command("hack/content-hash.sh")
	cmd.Dir = strings.TrimSpace(string(root))
	out, err := cmd.Output()
	if err != nil {
		return "", exitDetail(err)
	}
	h := strings.TrimSpace(string(out))
	if !hashRE.MatchString(h) {
		return "", fmt.Errorf("hack/content-hash.sh printed %q, want 16 hex", h)
	}
	return "x-" + h, nil
}

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
