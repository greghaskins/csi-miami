// Package tool is the one place the driver starts an external program.
//
// The driver execs exactly the programs of Programs (cryptsetup, the
// e2fsprogs binaries and blkid); everything else is syscalls, netlink or
// D-Bus. Callers hold a Runner, so a test substitutes tool/fake and never needs
// the real binaries.
package tool

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// Programs are the external programs the driver starts, by name. The image
// carries each of them (hack/tools.txt is the list the Dockerfile and the smoke
// test read), and TestProgramsAreTheImagesTools keeps the three in agreement.
var Programs = []string{"cryptsetup", "mkfs.ext4", "e2fsck", "resize2fs", "blkid"}

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

// Device is what a read-only look at a block device finds.
type Device struct {
	// Blank is true when the first sector block (4 KiB) reads as all zeros.
	Blank bool
}

// Prober is a Runner that can also look at a block device itself, without a
// program: whether it exists, is a block device and can be read. A tool's
// exit status cannot tell a device that is blank from one that is not there
// or cannot be read (blkid exits 2 for both, and cryptsetup isLuks exits 1 for
// a header it could not read), so a caller that is about to format on "nothing
// found" first establishes that the device is there with ProbeDevice.
type Prober interface {
	ProbeDevice(dev string) (Device, error)
}

// ProbeDevice looks at dev through r. A Runner that cannot is an error, never
// a blank device.
func ProbeDevice(r Runner, dev string) (Device, error) {
	p, ok := r.(Prober)
	if !ok {
		return Device{}, fmt.Errorf("tool: this runner cannot look at %s", dev)
	}
	return p.ProbeDevice(dev)
}

// probeSize is how much of the start of the device is read: the sector block
// where a boot sector, a LUKS header and the first ext4 structures start.
const probeSize = 4096

// ProbeDevice stats dev, requires a block device, opens it read-only and reads
// its first 4 KiB. A missing device node, anything that is not a block device
// and a device that cannot be opened or read is an error.
func (host) ProbeDevice(dev string) (Device, error) {
	fi, err := os.Stat(dev)
	if err != nil {
		return Device{}, fmt.Errorf("tool: device %s: %w", dev, err)
	}
	if fi.Mode()&os.ModeDevice == 0 || fi.Mode()&os.ModeCharDevice != 0 {
		return Device{}, fmt.Errorf("tool: %s is not a block device (mode %s)", dev, fi.Mode())
	}
	f, err := os.Open(dev) //nolint:gosec // the driver's own device path, opened read-only
	if err != nil {
		return Device{}, fmt.Errorf("tool: device %s: %w", dev, err)
	}
	defer func() { _ = f.Close() }()
	buf := make([]byte, probeSize)
	if _, err := io.ReadFull(f, buf); err != nil {
		return Device{}, fmt.Errorf("tool: reading the start of %s: %w", dev, err)
	}
	for _, b := range buf {
		if b != 0 {
			return Device{}, nil
		}
	}
	return Device{Blank: true}, nil
}
