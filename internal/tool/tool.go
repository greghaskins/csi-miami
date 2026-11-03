// Package tool is the one place the driver starts an external program.
//
// The driver execs exactly two tools, cryptsetup and the e2fsprogs binaries;
// everything else is syscalls, netlink or D-Bus. Callers hold a Runner, so a
// test substitutes tool/fake and never needs the real binaries.
package tool

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// Runner runs one program to completion.
//
// argv[0] is the program, looked up on PATH when it has no slash. Each file in
// extra becomes a descriptor in the child, starting at 3 (extra[0] is fd 3),
// which is how a key reaches cryptsetup without touching a command line or a
// file. The returned code is the program's exit status. A program that exits
// non-zero is reported through code with a nil error; err is for failing to
// start it, being killed by a signal or the context ending, and code is -1
// then. The caller owns the files in extra and closes them.
type Runner interface {
	Run(ctx context.Context, argv []string, extra []*os.File) (stdout, stderr []byte, code int, err error)
}

// Result is a classified outcome for a caller that keeps only the exit code
// and the program's complaint.
type Result struct {
	Code   int
	Stderr string
}

// Real returns the Runner that starts processes on the host.
func Real() Runner { return host{} }

type host struct{}

func (host) Run(ctx context.Context, argv []string, extra []*os.File) ([]byte, []byte, int, error) {
	if len(argv) == 0 {
		return nil, nil, -1, errors.New("tool: empty argv")
	}
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...) //nolint:gosec // the driver's own fixed tool names and device paths
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.ExtraFiles = extra
	// Own process group, so a deadline takes the program's children with it.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = 5 * time.Second
	cmd.Cancel = func() error {
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}

	err := cmd.Run()
	var ee *exec.ExitError
	switch {
	case err == nil:
		return stdout.Bytes(), stderr.Bytes(), 0, nil
	case ctx.Err() != nil:
		return stdout.Bytes(), stderr.Bytes(), -1, fmt.Errorf("tool: %s: %w", argv[0], ctx.Err())
	case errors.As(err, &ee) && ee.ExitCode() >= 0:
		return stdout.Bytes(), stderr.Bytes(), ee.ExitCode(), nil
	default:
		return stdout.Bytes(), stderr.Bytes(), -1, fmt.Errorf("tool: %s: %w", argv[0], err)
	}
}
