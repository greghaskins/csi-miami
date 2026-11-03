package tool_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/tool"
	"github.com/greghaskins/csi-miami/internal/tool/fake"
)

func TestRealRunnerPassesExtraFilesAsFd3(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("cat"); err != nil {
		t.Skip("cat not found")
	}
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	if _, err := w.WriteString("secret-key"); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()

	stdout, stderr, code, err := tool.Real().Run(context.Background(),
		[]string{"cat", "/dev/fd/3"}, []*os.File{r})
	if err != nil || code != 0 {
		t.Fatalf("Run: code %d err %v stderr %q", code, err, stderr)
	}
	if string(stdout) != "secret-key" {
		t.Fatalf("stdout = %q, want the pipe's bytes", stdout)
	}
}

func TestRealRunnerReportsExitCodeAndStderr(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh not found")
	}
	stdout, stderr, code, err := tool.Real().Run(context.Background(),
		[]string{"sh", "-c", "echo out; echo err >&2; exit 3"}, nil)
	if err != nil {
		t.Fatalf("a non-zero exit is a code, not an error: %v", err)
	}
	if code != 3 || string(stdout) != "out\n" || string(stderr) != "err\n" {
		t.Fatalf("got code %d stdout %q stderr %q", code, stdout, stderr)
	}
}

func TestRealRunnerErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		argv []string
	}{
		{"empty argv", nil},
		{"missing binary", []string{"/nonexistent/miami-no-such-tool"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, _, code, err := tool.Real().Run(context.Background(), tt.argv, nil)
			if err == nil || code != -1 {
				t.Fatalf("got code %d err %v, want -1 and an error", code, err)
			}
		})
	}
}

func TestRealRunnerDeadlineKillsProcessGroup(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh not found")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	// The grandchild holds stdout open; only a group kill lets Run return.
	_, _, _, err := tool.Real().Run(ctx, []string{"sh", "-c", "sleep 30 & sleep 30"}, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("Run took %v, the group was not killed", d)
	}
}

func TestFakeRunnerRecordsArgv(t *testing.T) {
	t.Parallel()
	f := &fake.Runner{Replies: []fake.Reply{
		{Stdout: "ok", Code: 0},
		{Stderr: "bad key", Code: 2},
	}}
	_, w, _ := os.Pipe()
	defer func() { _ = w.Close() }()

	out, _, code, err := f.Run(context.Background(), []string{"cryptsetup", "open"}, []*os.File{w})
	if err != nil || code != 0 || string(out) != "ok" {
		t.Fatalf("first: %q %d %v", out, code, err)
	}
	_, errb, code, err := f.Run(context.Background(), []string{"e2fsck", "-p", "/dev/x"}, nil)
	if err != nil || code != 2 || string(errb) != "bad key" {
		t.Fatalf("second: %q %d %v", errb, code, err)
	}
	// Past the script: success with no output.
	if _, _, code, err = f.Run(context.Background(), []string{"x"}, nil); err != nil || code != 0 {
		t.Fatalf("third: %d %v", code, err)
	}

	calls := f.Calls()
	if len(calls) != 3 {
		t.Fatalf("recorded %d calls, want 3", len(calls))
	}
	if got := strings.Join(calls[0].Argv, " "); got != "cryptsetup open" || calls[0].Extra != 1 {
		t.Fatalf("call 0 = %q extra %d", got, calls[0].Extra)
	}
	if got := strings.Join(calls[1].Argv, " "); got != "e2fsck -p /dev/x" || calls[1].Extra != 0 {
		t.Fatalf("call 1 = %q extra %d", got, calls[1].Extra)
	}
}

func TestFakeRunnerRecordsExtraFileContents(t *testing.T) {
	t.Parallel()
	pipe := func(s string) *os.File {
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		_, _ = w.WriteString(s)
		_ = w.Close()
		t.Cleanup(func() { _ = r.Close() })
		return r
	}
	f3, f4 := pipe("old-key"), pipe("new-key")
	f := &fake.Runner{}
	if _, _, _, err := f.Run(context.Background(), []string{"cryptsetup", "luksChangeKey"}, []*os.File{f3, f4}); err != nil {
		t.Fatal(err)
	}
	c := f.Calls()[0]
	if len(c.Files) != 2 || c.Files[0] != f3 || c.Files[1] != f4 {
		t.Fatalf("Files = %v, want the two files passed", c.Files)
	}
	if string(c.Data[0]) != "old-key" || string(c.Data[1]) != "new-key" {
		t.Fatalf("Data = %q, want fd 3 old-key and fd 4 new-key", c.Data)
	}
}
