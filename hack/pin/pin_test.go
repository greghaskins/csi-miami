package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var (
	dA = "sha256:" + strings.Repeat("a", 64)
	dB = "sha256:" + strings.Repeat("b", 64)
)

func pinned(tag, digest string) string {
	s := "images:\n  - name: ghcr.io/greghaskins/csi-miami\n"
	if tag != "" {
		s += "    newTag: " + tag + "\n"
	}
	return s + "    digest: " + digest + "\n"
}

func TestReadWritePin(t *testing.T) {
	// A file from before every commit pinned: no newTag; writing adds it.
	old := pinned("", dA)
	p, err := readPin([]byte(old))
	if err != nil || p != (pin{Digest: dA}) {
		t.Fatalf("readPin(old) = %+v, %v", p, err)
	}
	out, err := writePin([]byte(old), pin{"r-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", dB})
	if err != nil {
		t.Fatal(err)
	}
	if want := pinned("r-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", dB); string(out) != want {
		t.Fatalf("writePin added\n%s\nwant\n%s", out, want)
	}
	// Rewriting an existing pin touches only the two values.
	out, err = writePin(out, pin{"r-abababababababababababababababababababababababababababababababab", dA})
	if err != nil || string(out) != pinned("r-abababababababababababababababababababababababababababababababab", dA) {
		t.Fatalf("writePin rewrite = %q, %v", out, err)
	}
	for _, bad := range []pin{{"0000", dA}, {"x-ABC", dA}, {"x-0123456789abcdef", dA}, {"r-0000000000000000000000000000000000000000000000000000000000000000", "sha256:1"}} {
		if _, err := writePin([]byte(old), bad); err == nil {
			t.Errorf("writePin(%+v): want error", bad)
		}
	}
	// main may still pin an old x- tag: reading it works, so the script can
	// replace it.
	if p, err := readPin([]byte(pinned("x-0123456789abcdef", dA))); err != nil || p.Tag != "x-0123456789abcdef" {
		t.Errorf("readPin of an x- pin = %+v, %v", p, err)
	}
	two := pinned("r-0000000000000000000000000000000000000000000000000000000000000000", dA) + "    newTag: r-1111111111111111111111111111111111111111111111111111111111111111\n"
	if _, err := readPin([]byte(two)); err == nil {
		t.Error("readPin with two newTag lines: want error")
	}
}

// pinRepo is a real git repo with an origin, for the pin script. Its content
// tag is faked: the test sets it.
type pinRepo struct {
	t          *testing.T
	dir, bare  string
	tag        string
	ghCalls    []string
	jobs       string // JSON for run view
	noRun      bool
	noDownload bool
	artifact   string // image-ref.json content
	out        strings.Builder
}

func (r *pinRepo) git(args ...string) string {
	r.t.Helper()
	out, err := runner("git")(append([]string{"-C", r.dir}, args...)...)
	if err != nil {
		r.t.Fatal(err)
	}
	return out
}

func newPinRepo(t *testing.T) *pinRepo {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	root := t.TempDir()
	r := &pinRepo{t: t, dir: filepath.Join(root, "work"), bare: filepath.Join(root, "origin.git"), tag: "r-1111111111111111111111111111111111111111111111111111111111111111",
		jobs: `{"status":"completed","conclusion":"failure","jobs":[{"name":"image / ref","status":"completed","conclusion":"success","url":"u"},{"name":"pin","status":"completed","conclusion":"failure"}]}`}
	r.artifact = `{"tag":"r-1111111111111111111111111111111111111111111111111111111111111111","digest":"` + dA + `"}`
	for _, args := range [][]string{{"init", "-q", "--bare", "-b", "main", r.bare}, {"clone", "-q", r.bare, r.dir}} {
		if out, err := exec.Command("git", args...).CombinedOutput(); err != nil { //nolint:gosec // test repo setup
			t.Fatalf("git %v: %v %s", args, err, out)
		}
	}
	r.git("config", "user.name", "t")
	r.git("config", "user.email", "t@example.com")
	r.git("config", "commit.gpgsign", "false")
	r.git("config", "core.hooksPath", "/dev/null")
	r.git("checkout", "-q", "-B", "main")
	r.write(kustomization, pinned("r-0000000000000000000000000000000000000000000000000000000000000000", dB))
	r.write("README", "a\n")
	r.git("add", "-A")
	r.git("commit", "-q", "-m", "init")
	r.git("push", "-q", "origin", "main")
	r.git("checkout", "-q", "-b", "feature")
	r.write("README", "b\n")
	r.git("commit", "-q", "-am", "change")
	r.git("push", "-q", "origin", "feature")
	return r
}

func (r *pinRepo) write(path, content string) {
	p := filepath.Join(r.dir, path)
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		r.t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		r.t.Fatal(err)
	}
}

func (r *pinRepo) script() pinScript {
	return pinScript{
		out: &r.out,
		git: func(args ...string) (string, error) { return runner("git")(append([]string{"-C", r.dir}, args...)...) },
		gh: func(args ...string) (string, error) {
			r.ghCalls = append(r.ghCalls, strings.Join(args[:2], " "))
			switch args[0] + " " + args[1] {
			case "run list":
				if r.noRun {
					return "[]", nil
				}
				return `[{"databaseId":7,"url":"https://example.com/run/7"}]`, nil
			case "run view":
				return r.jobs, nil
			case "run download":
				if r.noDownload {
					return "", errors.New("no artifact matches any of the names provided")
				}
				dir := args[len(args)-1]
				return "", os.WriteFile(filepath.Join(dir, pinJSON), []byte(r.artifact), 0o600) //nolint:gosec // the test's own temp dir
			}
			return "", fmt.Errorf("unexpected gh %v", args)
		},
		tag:     func(string) (string, error) { return r.tag, nil },
		appear:  2 * time.Millisecond,
		timeout: 3 * time.Millisecond,
		poll:    time.Millisecond,
		sleep:   func(time.Duration) {},
	}
}

func (r *pinRepo) remote(ref string) string {
	out, err := exec.Command("git", "--git-dir", r.bare, "rev-parse", ref).Output() //nolint:gosec // test repo
	if err != nil {
		r.t.Fatal(err)
	}
	return strings.TrimSpace(string(out))
}

func TestPinScriptPins(t *testing.T) {
	r := newPinRepo(t)
	// main moves on with its own pin, so the merge conflicts in the pin file.
	r.git("checkout", "-q", "main")
	r.write(kustomization, pinned("r-0101010101010101010101010101010101010101010101010101010101010101", dA))
	r.git("commit", "-q", "-am", "pin: r-0101010101010101010101010101010101010101010101010101010101010101")
	r.git("push", "-q", "origin", "main")
	r.git("checkout", "-q", "feature")
	r.write(kustomization, pinned("r-0202020202020202020202020202020202020202020202020202020202020202", dB))
	r.git("commit", "-q", "-am", "pin: r-0202020202020202020202020202020202020202020202020202020202020202")
	r.git("push", "-q", "origin", "feature")

	if err := r.script().run(); err != nil {
		t.Fatalf("%v\n%s", err, r.out.String())
	}
	if got := r.git("log", "-1", "--format=%s"); got != "pin: r-1111111111111111111111111111111111111111111111111111111111111111" {
		t.Errorf("last commit %q, want pin: r-1111111111111111111111111111111111111111111111111111111111111111", got)
	}
	if r.remote("feature") != r.git("rev-parse", "HEAD") {
		t.Error("pin commit not pushed")
	}
	if got, _ := os.ReadFile(filepath.Join(r.dir, kustomization)); string(got) != pinned("r-1111111111111111111111111111111111111111111111111111111111111111", dA) {
		t.Errorf("pin file:\n%s", got)
	}
	// Updated by merge: main is an ancestor, and nothing was rewritten.
	if _, err := runner("git")("-C", r.dir, "merge-base", "--is-ancestor", "origin/main", "HEAD"); err != nil {
		t.Error("origin/main not merged in")
	}
	if !strings.Contains(r.out.String(), "ask for review") && !strings.Contains(r.out.String(), "Ask for review") {
		t.Errorf("output does not say what to do next:\n%s", r.out.String())
	}
}

func TestPinScriptAlreadyPinned(t *testing.T) {
	r := newPinRepo(t)
	r.tag = "r-0000000000000000000000000000000000000000000000000000000000000000"
	before := r.remote("feature")
	if err := r.script().run(); err != nil {
		t.Fatal(err)
	}
	if r.remote("feature") != before || len(r.ghCalls) != 0 {
		t.Errorf("pushed or waited for nothing to pin; gh calls %v", r.ghCalls)
	}
}

func TestPinScriptFailsWithoutPushing(t *testing.T) {
	cases := map[string]func(*pinRepo){
		"image job failed": func(r *pinRepo) {
			r.jobs = `{"status":"completed","conclusion":"failure","jobs":[{"name":"image / ref","status":"completed","conclusion":"failure","url":"u"}]}`
		},
		"image job never finishes": func(r *pinRepo) {
			r.jobs = `{"status":"in_progress","jobs":[{"name":"image / ref","status":"in_progress"}]}`
		},
		"run cancelled before the image job": func(r *pinRepo) {
			r.jobs = `{"status":"completed","conclusion":"cancelled","jobs":[]}`
		},
		"no image run": func(r *pinRepo) { r.noRun = true },
		"run built another tag": func(r *pinRepo) {
			r.artifact = `{"tag":"r-9999999999999999999999999999999999999999999999999999999999999999","digest":"` + dA + `"}`
		},
		"artifact garbage": func(r *pinRepo) {
			r.artifact = `{"tag":"r-1111111111111111111111111111111111111111111111111111111111111111"}`
		},
	}
	for name, breakIt := range cases {
		t.Run(name, func(t *testing.T) {
			r := newPinRepo(t)
			breakIt(r)
			before := r.remote("feature")
			if err := r.script().run(); err == nil {
				t.Fatal("want an error")
			}
			if r.remote("feature") != before {
				t.Error("pushed on failure")
			}
			if got := r.git("log", "-1", "--format=%s"); strings.HasPrefix(got, "pin:") {
				t.Errorf("left a pin commit: %q", got)
			}
		})
	}
}

func TestPinScriptRefusesConflictsAndMain(t *testing.T) {
	r := newPinRepo(t)
	r.git("checkout", "-q", "main")
	r.write("README", "main\n")
	r.git("commit", "-q", "-am", "main change")
	r.git("push", "-q", "origin", "main")
	r.git("checkout", "-q", "feature")
	before := r.remote("feature")
	err := r.script().run()
	if err == nil || !strings.Contains(err.Error(), "README") {
		t.Fatalf("want a conflict naming README, got %v", err)
	}
	if r.remote("feature") != before {
		t.Error("pushed on conflict")
	}
	if st := r.git("status", "--porcelain"); st != "" {
		t.Errorf("merge not aborted: %q", st)
	}

	r.git("checkout", "-q", "main")
	if err := r.script().run(); err == nil || !strings.Contains(err.Error(), "on main") {
		t.Errorf("on main: got %v", err)
	}
}

// The pinned SHA comes only from a real `uses:` line, not a comment that
// mentions pin.yml.
func TestPinnedChecksRE(t *testing.T) {
	sha := strings.Repeat("a", 40)
	other := strings.Repeat("b", 40)
	wf := "# was greghaskins/release-checks/.github/workflows/pin.yml@" + other + "\n" +
		"jobs:\n  pin:\n    uses: greghaskins/release-checks/.github/workflows/pin.yml@" + sha + " # v1\n"
	m := pinnedChecksRE.FindSubmatch([]byte(wf))
	if m == nil || string(m[1]) != sha {
		t.Fatalf("got %q, want %s", m, sha)
	}
	if pinnedChecksRE.Match([]byte("# greghaskins/release-checks/.github/workflows/pin.yml@" + sha + "\n")) {
		t.Error("a comment matched")
	}
}
