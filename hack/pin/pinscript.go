package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

func init() { commands["pin"] = pinCmd }

const (
	imageWorkflow = "ci.yml"
	imageJob      = "manifest"
	pinArtifact   = "image-ref"
	pinJSON       = "image-ref.json"
)

// pinScript pins the current branch: it merges origin/main in, waits for the
// image job of the branch head, and commits and pushes the tag and digest
// that job published. See docs/releasing.md.
type pinScript struct {
	out io.Writer
	// git and gh run in the working tree and return stdout.
	git func(args ...string) (string, error)
	gh  func(args ...string) (string, error)
	tag func(rev string) (string, error)
	// appear bounds the wait for the image run to show up; timeout bounds the
	// whole wait for its image job (the job's own timeout is 60 min).
	appear, timeout, poll time.Duration
	sleep                 func(time.Duration)
}

type pinError struct{ msg string }

func (e pinError) Error() string { return e.msg }

func failf(format string, a ...any) error { return pinError{fmt.Sprintf(format, a...)} }

func (s pinScript) step(n int, format string, a ...any) {
	_, _ = fmt.Fprintf(s.out, "pin %d/4: %s\n", n, fmt.Sprintf(format, a...))
}

func (s pinScript) run() error {
	// 1. Update the branch from main, by merge (never rebase: stacked PRs
	// keep their bases, and nothing is force-pushed).
	if st, err := s.git("status", "--porcelain", "--untracked-files=no"); err != nil {
		return err
	} else if st != "" {
		return failf("the working tree has uncommitted changes; commit or stash them first")
	}
	branch, err := s.git("symbolic-ref", "--short", "-q", "HEAD")
	if err != nil || branch == "" {
		return failf("not on a branch; check out the PR branch first")
	}
	if branch == "main" {
		return failf("on main; the pin script runs on a PR branch")
	}
	s.step(1, "updating %s from origin/main", branch)
	if _, err := s.git("fetch", "--quiet", "origin"); err != nil {
		return err
	}
	if _, err := s.git("merge-base", "--is-ancestor", "origin/main", "HEAD"); err == nil {
		s.step(1, "already up to date with origin/main")
	} else if err := s.mergeMain(); err != nil {
		return err
	}

	// 2. The content tag of the updated tree.
	tag, err := s.tag("HEAD")
	if err != nil {
		return err
	}
	content, err := s.git("show", "HEAD:"+kustomization)
	if err != nil {
		return err
	}
	cur, err := readPin([]byte(content + "\n"))
	if err != nil {
		return err
	}
	s.step(2, "content tag %s (pinned: %s)", tag, orNone(cur.Tag))

	head, err := s.git("rev-parse", "HEAD")
	if err != nil {
		return err
	}
	remote, _ := s.git("rev-parse", "--verify", "-q", "origin/"+branch)
	if cur.Tag == tag {
		// The tag's digest is already pinned; the `pin` check confirms it
		// against the registry. Only an update from main is left to push.
		if remote != head {
			if err := s.push(branch); err != nil {
				return err
			}
			s.step(4, "already pinned to %s; pushed the update from main", tag)
		} else {
			s.step(4, "already pinned to %s; nothing to push", tag)
		}
		return nil
	}

	// 3. The image job of this exact commit builds or finds the tag. CI only
	// runs on pushed commits, so an unpushed head (local commits or the merge
	// from main) is pushed first; the pin commit itself comes only after the
	// job passed.
	if remote != head {
		s.step(3, "pushing %s so CI builds %s", short(head), tag)
		if err := s.push(branch); err != nil {
			return err
		}
	}
	runID, url, err := s.waitImageJob(head)
	if err != nil {
		return err
	}

	// 4. Write the tag and digest the job published, commit, push.
	got, err := s.download(runID, url)
	if err != nil {
		return err
	}
	if got.Tag != tag {
		return failf("the image run %s published %s, but this tree's tag is %s; origin/main may have moved, so run the pin script again", url, got.Tag, tag)
	}
	next, err := writePin([]byte(content+"\n"), got)
	if err != nil {
		return err
	}
	root, err := s.git("rev-parse", "--show-toplevel")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(root, kustomization), next, 0o644); err != nil { //nolint:gosec // a tracked file, mode as committed
		return err
	}
	if _, err := s.git("commit", "--quiet", "-m", "pin: "+tag, "--", kustomization); err != nil {
		_, _ = s.git("checkout", "--", kustomization)
		return err
	}
	if err := s.push(branch); err != nil {
		// Drop the local pin commit, so a rerun waits and pins again rather
		// than finding the tag "already pinned" and pushing unchecked.
		if _, rerr := s.git("reset", "--quiet", "--keep", "HEAD^"); rerr != nil {
			return failf("pushing the pin commit failed: %v\ndropping the local pin commit failed too: %v\nrun `git reset --keep HEAD^`, fetch, then run the pin script again", err, rerr)
		}
		return failf("pushing the pin commit failed: %v\nthe local pin commit was dropped; fetch, then run the pin script again", err)
	}
	sha, _ := s.git("rev-parse", "HEAD")
	s.step(4, "pinned %s; pushed %s. Ask for review once `pin` is green", got, short(sha))
	return nil
}

// mergeMain merges origin/main into the branch. A conflict only in the pin
// file takes main's pin (this run re-pins next); any other conflict aborts.
func (s pinScript) mergeMain() error {
	if _, err := s.git("merge", "--no-edit", "--quiet", "origin/main"); err == nil {
		s.step(1, "merged origin/main")
		return nil
	}
	files, _ := s.git("diff", "--name-only", "--diff-filter=U")
	if files != kustomization {
		_, _ = s.git("merge", "--abort")
		if files == "" {
			return failf("merging origin/main failed; run `git merge origin/main` to see why")
		}
		return failf("merging origin/main conflicts in:\n%s\nresolve it by hand (`git merge origin/main`), then run the pin script again", files)
	}
	if _, err := s.git("checkout", "--theirs", "--", kustomization); err != nil {
		_, _ = s.git("merge", "--abort")
		return err
	}
	if _, err := s.git("add", "--", kustomization); err != nil {
		_, _ = s.git("merge", "--abort")
		return err
	}
	if _, err := s.git("commit", "--no-edit", "--quiet"); err != nil {
		_, _ = s.git("merge", "--abort")
		return err
	}
	s.step(1, "merged origin/main; its pin replaced this branch's, to be re-pinned")
	return nil
}

func (s pinScript) push(branch string) error {
	_, err := s.git("push", "--quiet", "origin", "HEAD:refs/heads/"+branch)
	return err
}

type ghRun struct {
	DatabaseID int64  `json:"databaseId"`
	URL        string `json:"url"`
}

type ghJob struct {
	Name, Status, Conclusion, URL string
}

// waitImageJob waits for the image job of commit sha to finish and returns
// its run. It fails when no run appears, the job fails, or time runs out.
func (s pinScript) waitImageJob(sha string) (int64, string, error) {
	var run ghRun
	for waited := time.Duration(0); ; waited += s.poll {
		out, err := s.ghRetry("run", "list", "--workflow", imageWorkflow, "--commit", sha, "--limit", "1", "--json", "databaseId,url")
		if err != nil {
			return 0, "", failf("listing %s runs for %s failed: %v\ncheck `gh auth status` and the Actions tab, then run the pin script again", imageWorkflow, short(sha), err)
		}
		var runs []ghRun
		if err := json.Unmarshal([]byte(out), &runs); err != nil {
			return 0, "", failf("gh run list gave unreadable output (%v); run the pin script again", err)
		}
		if len(runs) > 0 {
			run = runs[0]
			break
		}
		if waited >= s.appear {
			return 0, "", failf("no %s run for %s after %s; is a PR open for this branch? Check the Actions tab", imageWorkflow, short(sha), s.appear)
		}
		s.sleep(s.poll)
	}
	s.step(3, "waiting for the image job (up to %s): %s", s.timeout, run.URL)
	for waited := time.Duration(0); ; waited += s.poll {
		// gh run view reports the run's latest attempt, so a re-run counts.
		out, err := s.ghRetry("run", "view", fmt.Sprint(run.DatabaseID), "--json", "status,conclusion,jobs")
		if err != nil {
			return 0, "", failf("reading the image run failed: %v\n%s\nrun the pin script again once the run is readable", err, run.URL)
		}
		var v struct {
			Status, Conclusion string
			Jobs               []ghJob
		}
		if err := json.Unmarshal([]byte(out), &v); err != nil {
			return 0, "", failf("gh run view gave unreadable output (%v): %s\nrun the pin script again", err, run.URL)
		}
		for _, j := range v.Jobs {
			if j.Name != imageJob || j.Status != "completed" {
				continue
			}
			if j.Conclusion != "success" {
				return 0, "", failf("the image job ended %q: %s\nopen the run, fix the build or use \"Re-run failed jobs\", then run the pin script again", j.Conclusion, run.URL)
			}
			s.step(3, "image job passed")
			return run.DatabaseID, run.URL, nil
		}
		if v.Status == "completed" {
			return 0, "", failf("the image run ended %q before its image job finished: %s\nre-run it, then run the pin script again", v.Conclusion, run.URL)
		}
		if waited >= s.timeout {
			return 0, "", failf("the image job did not finish within %s: %s\nrun the pin script again once it has", s.timeout, run.URL)
		}
		s.sleep(s.poll)
	}
}

// download reads the tag and digest the image job uploaded. Every failure
// names the run, so the reader can open it.
func (s pinScript) download(runID int64, url string) (pin, error) {
	bad := func(format string, a ...any) error {
		return failf("%s\n%s\nre-run the image job if the artifact is missing, then run the pin script again", fmt.Sprintf(format, a...), url)
	}
	dir, err := os.MkdirTemp("", "image-ref-")
	if err != nil {
		return pin{}, err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	if _, err := s.ghRetry("run", "download", fmt.Sprint(runID), "--name", pinArtifact, "--dir", dir); err != nil {
		return pin{}, bad("downloading the %s artifact failed: %v", pinArtifact, err)
	}
	b, err := os.ReadFile(filepath.Join(dir, pinJSON)) //nolint:gosec // our own temp dir
	if err != nil {
		return pin{}, bad("the %s artifact has no %s", pinArtifact, pinJSON)
	}
	var p pin
	if err := json.Unmarshal(b, &p); err != nil || !tagRE.MatchString(p.Tag) || !digestRE.MatchString(p.Digest) {
		return pin{}, bad("%s holds %q, not a tag and a digest", pinJSON, b)
	}
	return p, nil
}

// ghRetry runs gh, retrying twice on failure: a wait of up to 75 minutes
// should not die on one transient API error.
func (s pinScript) ghRetry(args ...string) (string, error) {
	var out string
	var err error
	for i := 0; i < 3; i++ {
		if i > 0 {
			s.sleep(s.poll)
		}
		if out, err = s.gh(args...); err == nil {
			return out, nil
		}
	}
	return "", err
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}

func short(sha string) string {
	if len(sha) > 12 {
		return sha[:12]
	}
	return sha
}

// runner runs name in the working tree and returns its trimmed stdout. Its
// error includes stderr.
func runner(name string) func(args ...string) (string, error) {
	return func(args ...string) (string, error) {
		cmd := exec.Command(name, args...) //nolint:gosec // name is git or gh; args are built here
		var stdout, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		if err := cmd.Run(); err != nil {
			return "", fmt.Errorf("%s %s: %v: %s", name, strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
		}
		return strings.TrimSpace(stdout.String()), nil
	}
}

func pinCmd(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("pin", flag.ContinueOnError)
	fs.SetOutput(stderr)
	// The manifest job waits on both builds (20 min each, in parallel) and
	// takes up to 10 min itself; an hour leaves room for queueing.
	timeout := fs.Duration("timeout", time.Hour, "how long to wait for the manifest job")
	fs.Usage = func() {
		_, _ = fmt.Fprintln(stderr, "usage: go run ./hack/pin pin [--timeout 1h]")
		_, _ = fmt.Fprintln(stderr, "Merges origin/main in, waits for this commit's manifest job, then commits and pushes `pin: <tag>`.")
	}
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 {
		if err == nil {
			fs.Usage()
		}
		return 2
	}
	s := pinScript{
		out:     stdout,
		git:     runner("git"),
		gh:      runner("gh"),
		tag:     contentTag,
		appear:  5 * time.Minute,
		timeout: *timeout,
		poll:    15 * time.Second,
		sleep:   time.Sleep,
	}
	if err := s.run(); err != nil {
		var pe pinError
		if errors.As(err, &pe) {
			_, _ = fmt.Fprintf(stderr, "pin: %s\n", pe.msg)
		} else {
			_, _ = fmt.Fprintf(stderr, "pin: %v\n", err)
		}
		_, _ = fmt.Fprintln(stderr, "pin: no pin commit pushed")
		return 1
	}
	return 0
}
