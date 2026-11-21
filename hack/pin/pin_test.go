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
	out, err := writePin([]byte(old), pin{"x-0123456789abcdef", dB})
	if err != nil {
		t.Fatal(err)
	}
	if want := pinned("x-0123456789abcdef", dB); string(out) != want {
		t.Fatalf("writePin added\n%s\nwant\n%s", out, want)
	}
	// Rewriting an existing pin touches only the two values.
	out, err = writePin(out, pin{"x-0000", dA})
	if err != nil || string(out) != pinned("x-0000", dA) {
		t.Fatalf("writePin rewrite = %q, %v", out, err)
	}
	for _, bad := range []pin{{"0000", dA}, {"x-ABC", dA}, {"x-00", "sha256:1"}} {
		if _, err := writePin([]byte(old), bad); err == nil {
			t.Errorf("writePin(%+v): want error", bad)
		}
	}
	two := pinned("x-00", dA) + "    newTag: x-11\n"
	if _, err := readPin([]byte(two)); err == nil {
		t.Error("readPin with two newTag lines: want error")
	}
}

func TestRealPinFileRenders(t *testing.T) {
	b, err := os.ReadFile("../../" + kustomization)
	if err != nil {
		t.Fatal(err)
	}
	p, err := readPin(b)
	if err != nil {
		t.Fatal(err)
	}
	if !tagRE.MatchString(p.Tag) {
		t.Errorf("the pin file has no content tag: %+v", p)
	}
	out, err := renderBase()
	if err != nil {
		t.Fatal(err)
	}
	if bad := unpinnedRefs(out, p.Digest); len(bad) > 0 {
		t.Errorf("render references %v, want %s", bad, p)
	}
}

// fakeEnv is a tree whose content tag is x-11 and whose registry holds x-11
// under dA, pinned correctly; each test breaks one thing.
type fakeEnv struct {
	head, base string // pin file content at HEAD and at base ("" = absent)
	tag        string
	registry   map[string]string
	regErr     error
	baseErr    error
	render     string
}

func goodEnv() *fakeEnv {
	return &fakeEnv{
		head:     pinned("x-11", dA),
		base:     pinned("x-00", dB),
		tag:      "x-11",
		registry: map[string]string{"x-11": dA},
		render:   "spec:\n  containers:\n    - image: ghcr.io/greghaskins/csi-miami:x-11@" + dA + "\n      name: miami\n",
	}
}

func (f *fakeEnv) env() pinCheckEnv {
	return pinCheckEnv{
		tag: func(string) (string, error) { return f.tag, nil },
		show: func(rev, _ string) ([]byte, error) {
			if f.baseErr != nil && rev == "BASE" {
				return nil, f.baseErr
			}
			c := map[string]string{"HEAD": f.head, "BASE": f.base}[rev]
			if c == "" {
				return nil, errAbsent
			}
			return []byte(c), nil
		},
		registry: func(tag string) (string, error) {
			if f.regErr != nil {
				return "", f.regErr
			}
			if d, ok := f.registry[tag]; ok {
				return d, nil
			}
			return "", errNotBuilt
		},
		render: func() ([]byte, error) { return []byte(f.render), nil },
	}
}

func TestCheckPin(t *testing.T) {
	cases := []struct {
		name    string
		breakIt func(*fakeEnv)
		rules   []int
		text    string // must appear in the failure output
	}{
		{"all rules hold", func(*fakeEnv) {}, nil, ""},
		{"docs-only PR: base pins the same tag and digest", func(f *fakeEnv) { f.base = f.head }, nil, ""},
		{"base from before pinning has no tag", func(f *fakeEnv) { f.base = pinned("", dB) }, nil, ""},
		{"rule 1: no tag pinned", func(f *fakeEnv) {
			f.head = pinned("", dA)
			f.render = strings.Replace(f.render, ":x-11", "", 1)
		}, []int{1}, "has no newTag"},
		{"rule 1: stale tag", func(f *fakeEnv) { f.tag = "x-22"; f.registry["x-22"] = dA }, []int{1}, "pinned tag x-11 is not this tree's content tag x-22"},
		{"rule 2: not built (image job failed, or a fork PR)", func(f *fakeEnv) { delete(f.registry, "x-11") }, []int{2}, "not built"},
		{"rule 2: registry error is not 'not built'", func(f *fakeEnv) { f.regErr = errors.New("timeout") }, []int{2}, "cannot read"},
		{"rule 2: wrong digest", func(f *fakeEnv) { f.registry["x-11"] = dB }, []int{2}, "is not the registry's digest"},
		{"rule 3: render without digest", func(f *fakeEnv) {
			f.render = "    - image: ghcr.io/greghaskins/csi-miami:x-11\n"
		}, []int{3}, "the render references ghcr.io/greghaskins/csi-miami:x-11"},
		{"rule 3: no image rendered", func(f *fakeEnv) { f.render = "kind: Namespace\n" }, []int{3}, "no ghcr.io/greghaskins/csi-miami image"},
		{"base without a pin file skips rule 4", func(f *fakeEnv) { f.base = "" }, nil, ""},
		{"rule 4: unreadable base does not skip the guard", func(f *fakeEnv) { f.baseErr = errors.New("bad revision") }, []int{4}, "cannot read the base pin"},
		{"base from before the pin skips rule 4", func(f *fakeEnv) { f.base = "resources:\n  - daemonset-node.yaml\n" }, nil, ""},
		{"rule 4: malformed base pin", func(f *fakeEnv) { f.base = pinned("x-11", dA) + "    digest: " + dB + "\n" }, []int{4}, "cannot read the base pin"},
		{"rule 4: same tag, new digest", func(f *fakeEnv) { f.base = pinned("x-11", dB) }, []int{4},
			"same tag x-11 now has digest " + dA + "; escalate to the maintainer; change a hashed input to get a new tag"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := goodEnv()
			c.breakIt(f)
			_, fails := checkPin(f.env(), "BASE")
			var got []int
			var text strings.Builder
			for _, fl := range fails {
				got = append(got, fl.Rule)
				text.WriteString(fl.String() + "\n")
				if fl.Fix == "" {
					t.Errorf("rule %d has no fix", fl.Rule)
				}
			}
			if fmt.Sprint(got) != fmt.Sprint(c.rules) {
				t.Fatalf("failed rules %v, want %v:\n%s", got, c.rules, text.String())
			}
			if !strings.Contains(text.String(), c.text) {
				t.Errorf("output lacks %q:\n%s", c.text, text.String())
			}
			if len(got) > 0 && !strings.Contains(text.String(), fmt.Sprintf("pin rule %d: ", got[0])) {
				t.Errorf("output does not name the rule:\n%s", text.String())
			}
		})
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
	r := &pinRepo{t: t, dir: filepath.Join(root, "work"), bare: filepath.Join(root, "origin.git"), tag: "x-11",
		jobs: `{"status":"completed","conclusion":"failure","jobs":[{"name":"manifest","status":"completed","conclusion":"success","url":"u"},{"name":"pin","status":"completed","conclusion":"failure"}]}`}
	r.artifact = `{"tag":"x-11","digest":"` + dA + `"}`
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
	r.write(kustomization, pinned("x-00", dB))
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
				return "", os.WriteFile(filepath.Join(dir, pinJSON), []byte(r.artifact), 0o600)
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
	r.write(kustomization, pinned("x-01", dA))
	r.git("commit", "-q", "-am", "pin: x-01")
	r.git("push", "-q", "origin", "main")
	r.git("checkout", "-q", "feature")
	r.write(kustomization, pinned("x-02", dB))
	r.git("commit", "-q", "-am", "pin: x-02")
	r.git("push", "-q", "origin", "feature")

	if err := r.script().run(); err != nil {
		t.Fatalf("%v\n%s", err, r.out.String())
	}
	if got := r.git("log", "-1", "--format=%s"); got != "pin: x-11" {
		t.Errorf("last commit %q, want pin: x-11", got)
	}
	if r.remote("feature") != r.git("rev-parse", "HEAD") {
		t.Error("pin commit not pushed")
	}
	if got, _ := os.ReadFile(filepath.Join(r.dir, kustomization)); string(got) != pinned("x-11", dA) {
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
	r.tag = "x-00"
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
			r.jobs = `{"status":"completed","conclusion":"failure","jobs":[{"name":"manifest","status":"completed","conclusion":"failure","url":"u"}]}`
		},
		"image job never finishes": func(r *pinRepo) {
			r.jobs = `{"status":"in_progress","jobs":[{"name":"manifest","status":"in_progress"}]}`
		},
		"run cancelled before the image job": func(r *pinRepo) {
			r.jobs = `{"status":"completed","conclusion":"cancelled","jobs":[]}`
		},
		"no image run":          func(r *pinRepo) { r.noRun = true },
		"run built another tag": func(r *pinRepo) { r.artifact = `{"tag":"x-99","digest":"` + dA + `"}` },
		"artifact garbage":      func(r *pinRepo) { r.artifact = `{"tag":"x-11"}` },
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

func TestGitShowAbsentVersusUnreadable(t *testing.T) {
	r := newPinRepo(t)
	t.Chdir(r.dir)
	if _, err := gitShow("HEAD", kustomization); err != nil {
		t.Errorf("present file: %v", err)
	}
	if _, err := gitShow("HEAD", "no/such/file"); !errors.Is(err, errAbsent) {
		t.Errorf("absent file: got %v, want errAbsent", err)
	}
	if _, err := gitShow("no-such-rev", kustomization); err == nil || errors.Is(err, errAbsent) {
		t.Errorf("unresolvable rev: got %v, want a non-absent error", err)
	}
}
