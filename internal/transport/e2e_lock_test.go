//go:build e2e

package transport_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/transport"
)

// The miami lock helper on the real nfs3 transport. Two nodes are two
// processes, each in a mount namespace of its own with its own mount of the
// export: they share the server and nothing else, not even the client's page
// and attribute caches. hack/e2e.sh builds the helper and names it in
// MIAMI_E2E_MIAMI.
//
// The test binary starts itself as the nodes (TestE2ELockChild is the entry
// point; it does nothing unless the parent set the environment below).
const (
	miamiEnv    = "MIAMI_E2E_MIAMI"
	roleEnv     = "MIAMI_E2E_LOCK_ROLE" // "contend" or "hold"
	subdirEnv   = "MIAMI_E2E_LOCK_SUBDIR"
	holderEnv   = "MIAMI_E2E_LOCK_HOLDER"
	sectionsEnv = "MIAMI_E2E_LOCK_SECTIONS"
	lockNameEnv = "MIAMI_E2E_LOCK_NAME"
)

// staleAfter is the smallest period the helper accepts. The stale test waits
// for the server's clock to pass it; there is no shorter test-only value
// because the floor is part of what is tested.
const staleAfter = 60 * time.Second

// sectionScript is the critical section the contenders run under the lock,
// as `sh -c script sh <dir> <holder>`. It makes overlap observable without
// trusting the lock: `inside` is made with mkdir, which is atomic on the
// server, and a holder that finds it already there records it and fails. The
// counter is read, written back one higher after a pause that makes a
// lost update likely if two holders overlap, and the holder is recorded
// under the new count. Exit 3 is an overlap and 4 any other failure, so that
// 1 stays what the helper uses for "held by another".
const sectionScript = `d="$1"; holder="$2"
if ! mkdir "$d/inside" 2>/dev/null; then
  echo "$holder" > "$d/overlap-$holder-$$"
  exit 3
fi
( set -e
  n=$(cat "$d/counter")
  sleep 0.05
  n=$((n + 1))
  echo "$n" > "$d/counter"
  echo "$holder" > "$d/section-$n"
)
rc=$?
rmdir "$d/inside" || exit 4
[ "$rc" -eq 0 ] || exit 4
`

func TestE2ENFS3Lock(t *testing.T) {
	server := os.Getenv(serverEnv)
	if server == "" {
		t.Fatalf("%s is not set: run this through hack/e2e.sh", serverEnv)
	}
	miami := os.Getenv(miamiEnv)
	if miami == "" {
		t.Fatalf("%s is not set: run this through hack/e2e.sh", miamiEnv)
	}
	export := os.Getenv(exportEnv)
	if export == "" {
		export = "/export"
	}
	base, m := kernelEnv(t)

	spec := transport.Spec{Kind: transport.KindNFS3, Server: server, Export: export}
	root := filepath.Join(base, "parent")
	mkdir(t, root)
	tr, err := transport.New(spec, root, m, transport.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Mount(context.Background()); err != nil {
		t.Fatalf("mounting the export: %v", err)
	}
	const subdir = "lock-e2e"
	work := filepath.Join(tr.Dir(), subdir)
	mkdir(t, work)
	t.Cleanup(func() { _ = os.RemoveAll(work) })

	t.Run("contention", func(t *testing.T) { testContention(t, miami, work) })
	t.Run("stale", func(t *testing.T) { testStale(t, miami, work) })
}

// Two nodes take the lock in turn, each ten times, to increment one counter.
func testContention(t *testing.T, miami, work string) {
	const perNode = 10
	dir := filepath.Join(work, "contention")
	mkdir(t, dir)
	if err := os.WriteFile(filepath.Join(dir, "counter"), []byte("0\n"), 0o644); err != nil { //nolint:gosec // shared with the other node's user
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	type result struct {
		holder string
		out    bytes.Buffer
		err    error
	}
	holders := []string{"node-a", "node-b"}
	results := make([]*result, len(holders))
	cmds := make([]*exec.Cmd, len(holders))
	for i, h := range holders {
		r := &result{holder: h}
		results[i] = r
		cmds[i] = childCommand(ctx, miami, "contend", "lock-e2e/contention", h)
		cmds[i].Env = append(cmds[i].Env, sectionsEnv+"="+strconv.Itoa(perNode), lockNameEnv+"=counter")
		cmds[i].Stdout, cmds[i].Stderr = &r.out, &r.out
	}
	for i, c := range cmds {
		if err := c.Start(); err != nil {
			t.Fatalf("starting node %s: %v", holders[i], err)
		}
	}
	for i, c := range cmds {
		results[i].err = c.Wait()
	}
	for _, r := range results {
		t.Logf("%s:\n%s", r.holder, r.out.String())
		if r.err != nil {
			t.Errorf("node %s: %v", r.holder, r.err)
		}
	}

	// The parent reads the files through its own mount, which has seen none of
	// the writes.
	got, err := os.ReadFile(filepath.Join(dir, "counter"))
	if err != nil {
		t.Fatal(err)
	}
	count, _ := strconv.Atoi(strings.TrimSpace(string(got)))
	t.Logf("final count %d of %d expected (%d sections on each of %d nodes)", count, perNode*len(holders), perNode, len(holders))
	if count != perNode*len(holders) {
		t.Errorf("final count = %d, want %d: increments were lost, so two holders overlapped", count, perNode*len(holders))
	}

	owners := map[string]int{}
	var order []string
	for n := 1; n <= perNode*len(holders); n++ {
		b, err := os.ReadFile(filepath.Join(dir, fmt.Sprintf("section-%d", n)))
		if err != nil {
			t.Errorf("section %d has no record: %v", n, err)
			continue
		}
		h := strings.TrimSpace(string(b))
		owners[h]++
		order = append(order, strings.TrimPrefix(h, "node-"))
	}
	for _, h := range holders {
		if owners[h] != perNode {
			t.Errorf("%s ran %d sections, want %d", h, owners[h], perNode)
		}
	}
	t.Logf("holder of each section in order: %s", strings.Join(order, ""))

	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), "overlap-") || e.Name() == "inside" || strings.HasPrefix(e.Name(), ".") || strings.HasSuffix(e.Name(), ".lock") {
			t.Errorf("%s was left in the directory: an overlap, or debris of the lock", e.Name())
		}
	}
}

// A holder is killed without releasing. Another node acquires only once the
// server's clock says the lock's stamp is more than the stale period old.
func testStale(t *testing.T, miami, work string) {
	dir := filepath.Join(work, "stale")
	mkdir(t, dir)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	// The first holder runs `miami lock run` around a long sleep, renewing
	// every 10s, in its own process group so that one kill ends all of it.
	var holderOut bytes.Buffer
	hold := childCommand(ctx, miami, "hold", "lock-e2e/stale", "dead-node")
	hold.Env = append(hold.Env, lockNameEnv+"=db")
	hold.Stdout, hold.Stderr = &holderOut, &holderOut
	hold.SysProcAttr.Setpgid = true
	if err := hold.Start(); err != nil {
		t.Fatal(err)
	}
	killed := false
	t.Cleanup(func() {
		if !killed {
			_ = syscall.Kill(-hold.Process.Pid, syscall.SIGKILL)
			_ = hold.Wait()
		}
	})

	holderFile := filepath.Join(dir, "db.lock", "holder")
	since := filepath.Join(dir, "db.lock", "since")
	waitFor(t, "the first holder to take the lock", 60*time.Second, func() (string, bool) {
		b, err := os.ReadFile(holderFile)
		return fmt.Sprintf("holder %q, %v; helper output: %s", b, err, holderOut.String()), err == nil && string(b) == "dead-node"
	})
	first := mustModTime(t, since)
	// The holder renews: the server stamps a newer since while it lives.
	var renewed time.Time
	waitFor(t, "the holder to renew the lock (a newer since on the server)", 90*time.Second, func() (string, bool) {
		mt, err := freshModTime(since)
		renewed = mt
		return fmt.Sprintf("since %s (first %s), %v", mt.Format(time.StampMilli), first.Format(time.StampMilli), err), err == nil && mt.After(first)
	})
	t.Logf("the holder renewed: since moved from %s to %s on the server", first.Format(time.StampMilli), renewed.Format(time.StampMilli))

	// Kill it without a release: the whole group, so nothing renews.
	if err := syscall.Kill(-hold.Process.Pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	killed = true
	_ = hold.Wait()
	lastStamp := mustModTime(t, since)
	t.Logf("holder killed; the last stamp on the server is %s", lastStamp.Format(time.StampMilli))

	// Poll one acquisition at a time. Before the server's clock passes the
	// stamp by the stale period every attempt must be refused; the attempt
	// that succeeds is the break. The server's clock is read around each
	// attempt (a file this node creates and stats), so that "refused too
	// early" and "accepted too early" are judged on it and not on a sleep.
	deadline := time.Now().Add(staleAfter + 3*time.Minute)
	attempts := 0
	for {
		if time.Now().After(deadline) {
			t.Fatalf("the lock was still not broken %s after the first attempt (%d attempts); server clock minus stamp: %s", staleAfter+3*time.Minute, attempts, serverTime(t, dir).Sub(lastStamp))
		}
		before := serverTime(t, dir)
		code, out := runMiami(t, ctx, miami, "lock", "acquire", "--dir", dir, "--name", "db", "--holder", "breaker",
			"--stale", staleAfter.String(), "--timeout", "20s")
		after := serverTime(t, dir)
		attempts++
		switch code {
		case 1:
			// Refused: the helper's probe was between before and after, and
			// it did not see the stamp older than the period, so before
			// cannot be more than the period past the stamp.
			if before.Sub(lastStamp) > staleAfter {
				t.Fatalf("attempt %d was refused although the server's clock was already %s past the stamp (the period is %s): %s", attempts, before.Sub(lastStamp), staleAfter, out)
			}
			time.Sleep(3 * time.Second)
			continue
		case 0:
			age := after.Sub(lastStamp)
			t.Logf("stale lock broken on attempt %d: the server's clock was between %s and %s past the dead holder's last stamp (stale period %s)", attempts, before.Sub(lastStamp).Round(time.Millisecond), age.Round(time.Millisecond), staleAfter)
			if age <= staleAfter {
				t.Errorf("the lock was broken when the server's clock was only %s past the stamp, within the period %s", age, staleAfter)
			}
		default:
			t.Fatalf("attempt %d: miami lock acquire exited %d: %s", attempts, code, out)
		}
		break
	}

	if b, err := os.ReadFile(holderFile); err != nil || string(b) != "breaker" {
		t.Errorf("holder after the break = %q, %v; want breaker", b, err)
	}
	if code, out := runMiami(t, ctx, miami, "lock", "release", "--dir", dir, "--name", "db", "--holder", "breaker", "--timeout", "20s"); code != 0 {
		t.Errorf("release by the breaker exited %d: %s", code, out)
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var left []string
	for _, e := range ents {
		left = append(left, e.Name())
	}
	sort.Strings(left)
	if len(left) != 0 {
		t.Errorf("the directory holds %v after the break and the release, want nothing (no broken lock, probe or temporary file)", left)
	}
}

// TestE2ELockChild is the node of the tests above: it mounts the export in a
// mount namespace of its own and runs the helper. It does nothing unless the
// parent started it with the role in the environment.
func TestE2ELockChild(t *testing.T) {
	role := os.Getenv(roleEnv)
	if role == "" {
		return
	}
	server, export, miami := os.Getenv(serverEnv), os.Getenv(exportEnv), os.Getenv(miamiEnv)
	holder, name := os.Getenv(holderEnv), os.Getenv(lockNameEnv)

	// Nothing mounted here may reach the parent's namespace.
	if err := unix.Mount("", "/", "", unix.MS_REC|unix.MS_PRIVATE, ""); err != nil {
		t.Fatalf("making the mount namespace private: %v", err)
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	spec := transport.Spec{Kind: transport.KindNFS3, Server: server, Export: export}
	tr, err := transport.New(spec, root, mount.New(10*time.Second), transport.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Mount(context.Background()); err != nil {
		t.Fatalf("mounting the export: %v", err)
	}
	// Unmount before the temporary directory is removed, which would otherwise
	// walk into the export and delete what is on it. (Cleanups run last in,
	// first out.)
	t.Cleanup(func() { _ = unix.Unmount(tr.Dir(), unix.MNT_DETACH) })
	dir := filepath.Join(tr.Dir(), os.Getenv(subdirEnv))

	switch role {
	case "hold":
		// Becomes the helper, so that killing the process group ends the node.
		args := []string{miami, "lock", "run", "--dir", dir, "--name", name, "--holder", holder,
			"--stale", staleAfter.String(), "--renew", "10s", "--timeout", "20s", "--", "sleep", "3600"}
		t.Fatalf("exec: %v", syscall.Exec(miami, args, os.Environ()))
	case "contend":
		n, _ := strconv.Atoi(os.Getenv(sectionsEnv))
		contended := 0
		for done := 0; done < n; {
			cmd := exec.Command(miami, "lock", "run", "--dir", dir, "--name", name, "--holder", holder,
				"--stale", staleAfter.String(), "--renew", "10s", "--timeout", "20s", "--",
				"sh", "-c", sectionScript, "sh", dir, holder)
			out, err := cmd.CombinedOutput()
			var ee *exec.ExitError
			switch {
			case err == nil:
				done++
			case errors.As(err, &ee) && ee.ExitCode() == 1:
				contended++
				time.Sleep(20 * time.Millisecond)
			default:
				t.Fatalf("section %d: %v: %s", done+1, err, out)
			}
		}
		t.Logf("%s ran %d sections, refused %d times while the other node held the lock", holder, n, contended)
	default:
		t.Fatalf("unknown role %q", role)
	}
}

// childCommand is the test binary run as a node: in a new mount namespace.
func childCommand(ctx context.Context, miami, role, subdir, holder string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestE2ELockChild$", "-test.v", "-test.count=1")
	cmd.Env = append(os.Environ(), roleEnv+"="+role, subdirEnv+"="+subdir, holderEnv+"="+holder, miamiEnv+"="+miami)
	cmd.SysProcAttr = &syscall.SysProcAttr{Cloneflags: syscall.CLONE_NEWNS}
	return cmd
}

// runMiami runs the helper in this process's namespace and returns its exit
// code and combined output.
func runMiami(t *testing.T, ctx context.Context, miami string, args ...string) (int, string) {
	t.Helper()
	out, err := exec.CommandContext(ctx, miami, args...).CombinedOutput()
	var ee *exec.ExitError
	switch {
	case err == nil:
		return 0, string(out)
	case errors.As(err, &ee):
		return ee.ExitCode(), string(out)
	}
	t.Fatalf("running %s: %v", args[1], err)
	return -1, ""
}

// freshModTime is the modification time the server gives path, read from an
// open file: opening revalidates the attributes, where stat may answer from
// the 30-second cache.
func freshModTime(path string) (time.Time, error) {
	f, err := os.Open(path)
	if err != nil {
		return time.Time{}, err
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		return time.Time{}, err
	}
	return st.ModTime(), nil
}

func mustModTime(t *testing.T, path string) time.Time {
	t.Helper()
	mt, err := freshModTime(path)
	if err != nil {
		t.Fatalf("reading the stamp: %v", err)
	}
	return mt
}

// serverTime is the server's clock now: the modification time of a file just
// created in dir.
func serverTime(t *testing.T, dir string) time.Time {
	t.Helper()
	p := filepath.Join(dir, fmt.Sprintf(".clock-%d-%d", os.Getpid(), time.Now().UnixNano()))
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatalf("reading the server's clock: %v", err)
	}
	st, err := f.Stat()
	_ = f.Close()
	_ = os.Remove(p)
	if err != nil {
		t.Fatalf("reading the server's clock: %v", err)
	}
	return st.ModTime()
}

// waitFor polls check every 500ms until it holds, and fails the test, naming
// what it waited for and what it last saw, when the deadline passes.
func waitFor(t *testing.T, what string, within time.Duration, check func() (seen string, ok bool)) {
	t.Helper()
	end := time.Now().Add(within)
	var seen string
	for {
		var ok bool
		if seen, ok = check(); ok {
			return
		}
		if time.Now().After(end) {
			t.Fatalf("timed out after %s waiting for %s; last saw: %s", within, what, seen)
		}
		time.Sleep(500 * time.Millisecond)
	}
}
