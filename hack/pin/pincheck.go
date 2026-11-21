package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"sigs.k8s.io/kustomize/api/krusty"
	"sigs.k8s.io/kustomize/kyaml/filesys"
)

func init() { commands["check"] = pinCheck }

const pinFix = "run `go run ./hack/pin pin` on the branch"

// errNotBuilt is what a registry lookup returns when the tag does not exist.
var errNotBuilt = errors.New("not built")

// errAbsent is what show returns when the revision lacks the file.
var errAbsent = errors.New("absent")

// pinCheckEnv is everything the pin check reads, so tests can fake it.
type pinCheckEnv struct {
	// tag computes the content tag of the tree at rev.
	tag func(rev string) (string, error)
	// show reads path at rev. errAbsent means rev has no such file; any
	// other error means rev could not be read.
	show func(rev, path string) ([]byte, error)
	// registry returns the digest of imageName:tag, or errNotBuilt.
	registry func(tag string) (string, error)
	// render runs kustomize build on the pin file's directory.
	render func() ([]byte, error)
}

// ruleFailure is one broken rule: what is wrong and how to fix it.
type ruleFailure struct {
	Rule      int
	What, Fix string
}

func (f ruleFailure) String() string {
	return fmt.Sprintf("pin rule %d: %s\n  fix: %s", f.Rule, f.What, f.Fix)
}

// checkPin applies the four rules of the pin check (docs/releasing.md) to the
// tree at HEAD, comparing against the pin at base for rule 4. It returns the
// pin it checked and every rule that fails, in rule order.
func checkPin(env pinCheckEnv, base string) (pin, []ruleFailure) {
	var fails []ruleFailure
	fail := func(rule int, what, fix string) { fails = append(fails, ruleFailure{rule, what, fix}) }

	content, err := env.show("HEAD", kustomization)
	if err != nil {
		fail(1, fmt.Sprintf("cannot read %s: %v", kustomization, err), "restore the file")
		return pin{}, fails
	}
	p, err := readPin(content)
	if err != nil {
		fail(1, err.Error(), "keep one images entry for "+imageName+" with one newTag and one digest line")
		return pin{}, fails
	}

	// Rule 1: the pinned tag is this tree's content tag.
	want, err := env.tag("HEAD")
	switch {
	case err != nil:
		fail(1, "cannot compute the content tag: "+err.Error(), "fix the image inputs listed in hack/content-hash.sh")
	case p.Tag == "":
		fail(1, fmt.Sprintf("%s has no newTag for %s; this tree's content tag is %s", kustomization, imageName, want), pinFix)
	case p.Tag != want:
		fail(1, fmt.Sprintf("pinned tag %s is not this tree's content tag %s; the pin is stale", p.Tag, want), pinFix)
	}

	// Rule 2: the registry holds the content tag, under the pinned digest.
	if err == nil {
		got, rerr := env.registry(want)
		switch {
		case errors.Is(rerr, errNotBuilt):
			fail(2, fmt.Sprintf("image %s:%s not built", imageName, want),
				"wait for the image job on this PR to pass, then "+pinFix+". A fork PR that changes image inputs cannot be built here; the maintainer handles it")
		case rerr != nil:
			fail(2, fmt.Sprintf("cannot read %s:%s from the registry: %v", imageName, want, rerr), "re-run this job; if it keeps failing, check the registry")
		case got != p.Digest:
			fail(2, fmt.Sprintf("pinned digest %s is not the registry's digest %s for %s", p.Digest, got, want), pinFix)
		}
	}

	// Rule 3: the render references the image by the pinned digest.
	if out, rerr := env.render(); rerr != nil {
		fail(3, "kustomize build "+pinDir()+" failed: "+rerr.Error(), "fix the kustomization so it builds")
	} else if bad := unpinnedRefs(out, p.Digest); len(bad) > 0 {
		fail(3, fmt.Sprintf("the render references %s", strings.Join(bad, ", ")),
			"pin the image only through the images entry in "+kustomization+"; set no other image for "+imageName)
	}

	// Rule 4: a tag the base already pins keeps its digest. Only a base
	// without the pin file, or with no pin in it at all, skips it; an
	// unreadable base or a garbled pin fails, so the re-push guard never
	// turns itself off.
	bc, berr := env.show(base, kustomization)
	switch {
	case errors.Is(berr, errAbsent):
	case berr != nil:
		fail(4, fmt.Sprintf("cannot read the base pin (%s at %s): %v", kustomization, base, berr), "re-run this job; the base must be readable to rule out a re-pushed tag")
	case unpinned(bc):
		// The base predates the pin (the first pin lands here); nothing to
		// compare against.
	default:
		bp, perr := readPin(bc)
		switch {
		case perr != nil:
			fail(4, fmt.Sprintf("cannot read the base pin (%s at %s): %v", kustomization, base, perr), "escalate to the maintainer: the base's pin file is malformed")
		case bp.Tag != "" && bp.Tag == p.Tag && bp.Digest != p.Digest:
			fail(4, fmt.Sprintf("same tag %s now has digest %s; escalate to the maintainer; change a hashed input to get a new tag", p.Tag, p.Digest),
				"do not re-pin this tag. The base pinned "+bp.Digest+"; a changed digest may mean the tag was re-pushed")
		}
	}
	return p, fails
}

// imageRefRE matches a rendered container image of imageName.
var imageRefRE = regexp.MustCompile(`(?m)^\s*(?:-\s+)?image:\s*["']?(` + regexp.QuoteMeta(imageName) + `[^\s"']*)`)

// unpinnedRefs lists the rendered references to imageName that do not end in
// @digest. A render with no reference at all counts as one bad reference.
func unpinnedRefs(render []byte, digest string) []string {
	var bad []string
	ms := imageRefRE.FindAllSubmatch(render, -1)
	if len(ms) == 0 {
		return []string{"no " + imageName + " image at all"}
	}
	for _, m := range ms {
		if ref := string(m[1]); digest == "" || !strings.HasSuffix(ref, "@"+digest) {
			bad = append(bad, ref)
		}
	}
	return bad
}

func pinDir() string { return strings.TrimSuffix(kustomization, "/kustomization.yaml") }

// pinCheck is the CLI: go run ./hack/pin check BASE. CI runs it as the
// `pin` job, with BASE the first parent (HEAD^1): on a PR's merge ref that is
// the base branch, and on a push to main it is the previous main.
func pinCheck(args []string, stdout, stderr io.Writer) int {
	if len(args) != 1 {
		_, _ = fmt.Fprintln(stderr, "usage: go run ./hack/pin check BASE-REV")
		return 2
	}
	env := pinCheckEnv{
		tag:      contentTag,
		show:     gitShow,
		registry: registryDigest,
		render:   renderBase,
	}
	p, fails := checkPin(env, args[0])
	actions := os.Getenv("GITHUB_ACTIONS") == "true"
	for _, f := range fails {
		_, _ = fmt.Fprintln(stderr, f)
		if actions {
			// One annotation per rule, shown on the PR's checks page.
			_, _ = fmt.Fprintf(stderr, "::error title=pin rule %d::%s. Fix: %s\n", f.Rule, f.What, f.Fix)
		}
	}
	if len(fails) > 0 {
		_, _ = fmt.Fprintf(stderr, "pin: %d of 4 rules failed\n", len(fails))
		return 1
	}
	_, _ = fmt.Fprintf(stdout, "pin: ok, %s\n", p)
	return 0
}

// gitShow reads path at rev. A rev that does not resolve is an error; a
// resolved rev without the file is errAbsent.
func gitShow(rev, path string) ([]byte, error) {
	if err := exec.Command("git", "rev-parse", "--verify", "--quiet", rev+"^{commit}").Run(); err != nil { //nolint:gosec // rev is a git revision from the CLI
		return nil, fmt.Errorf("revision %s does not resolve", rev)
	}
	// ls-tree separates "no such file" (empty output) from a git error.
	ls, err := exec.Command("git", "ls-tree", "--name-only", rev, "--", path).Output() //nolint:gosec // as above
	if err != nil {
		return nil, exitDetail(err)
	}
	if strings.TrimSpace(string(ls)) == "" {
		return nil, errAbsent
	}
	out, err := exec.Command("git", "show", rev+":"+path).Output() //nolint:gosec // as above
	return out, exitDetail(err)
}

// registryDigest asks the registry for the index digest of imageName:tag.
// Only a "not found" answer is errNotBuilt; any other failure is an error, so
// a flaky registry never reads as a missing image.
func registryDigest(tag string) (string, error) {
	ref := imageName + ":" + tag
	cmd := exec.Command("docker", "buildx", "imagetools", "inspect", ref, "--format", "{{json .Manifest}}") //nolint:gosec // ref is our image name and a checked tag
	var errb strings.Builder
	cmd.Stderr = &errb
	out, err := cmd.Output()
	if err != nil {
		msg := strings.TrimSpace(errb.String())
		if regexp.MustCompile(`(?i)not found|MANIFEST_UNKNOWN`).MatchString(msg) {
			return "", errNotBuilt
		}
		if msg == "" {
			return "", err
		}
		return "", fmt.Errorf("%v: %s", err, msg)
	}
	var m struct{ Digest string }
	if err := json.Unmarshal(out, &m); err != nil || !digestRE.MatchString(m.Digest) {
		return "", fmt.Errorf("unexpected inspect output for %s: %q", ref, out)
	}
	return m.Digest, nil
}

// exitDetail adds a command's stderr to its error.
func exitDetail(err error) error {
	var ee *exec.ExitError
	if errors.As(err, &ee) && len(ee.Stderr) > 0 {
		return fmt.Errorf("%v: %s", err, strings.TrimSpace(string(ee.Stderr)))
	}
	return err
}

// renderBase builds the pin file's directory with the kustomize library the
// deploy tests use, so the check needs no kustomize or kubectl binary.
func renderBase() ([]byte, error) {
	root, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return nil, exitDetail(err)
	}
	m, err := krusty.MakeKustomizer(krusty.MakeDefaultOptions()).Run(filesys.MakeFsOnDisk(), filepath.Join(strings.TrimSpace(string(root)), pinDir()))
	if err != nil {
		return nil, err
	}
	return m.AsYaml()
}

// unpinned reports whether kustomization content carries no pin at all: no
// digest line and no newTag line.
func unpinned(content []byte) bool {
	return len(digestLine.FindAll(content, -1)) == 0 && len(tagLine.FindAll(content, -1)) == 0
}
