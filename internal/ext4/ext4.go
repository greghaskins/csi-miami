// Package ext4 drives the e2fsprogs tools and the freeze ioctls for a
// volume's filesystem.
package ext4

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/greghaskins/csi-miami/internal/tool"
)

// MountOptions are the options every ext4 stage mount uses.
const MountOptions = "discard,errors=remount-ro"

// ErrFsck is returned when e2fsck leaves errors uncorrected or fails.
var ErrFsck = errors.New("ext4: filesystem check failed")

// Mkfs creates an ext4 filesystem on dev, zeroing the inode tables and journal
// up front so first use does no deferred background writes.
func Mkfs(ctx context.Context, r tool.Runner, dev string) error {
	return run(ctx, r, "mkfs", []string{"mkfs.ext4", "-q", "-F", "-E", "lazy_itable_init=0,lazy_journal_init=0", dev})
}

// HasFilesystem reports whether dev carries a file system signature, by
// blkid's low-level probe (exit 2 is "nothing found"). A blank device is the
// one case where Mkfs is right; a device with any signature is Fsck's.
//
// blkid exits 2 for a device that is not there or cannot be read as it does
// for a blank one, so the device is looked at first: it must exist, be a block
// device and read, or the answer is an error. "Nothing found" counts as no
// file system only if the start of the device also reads as zeros; a device
// with data and no signature is an error, left for a person. Any other failure
// is an error too, so a probe that cannot run does not lead to a mkfs over
// data.
func HasFilesystem(ctx context.Context, r tool.Runner, dev string) (bool, error) {
	d, err := tool.ProbeDevice(r, dev)
	if err != nil {
		return false, fmt.Errorf("ext4: %w", err)
	}
	stdout, stderr, code, err := r.Run(ctx, []string{"blkid", "-p", "-o", "value", "-s", "TYPE", dev}, nil)
	if err != nil {
		return false, fmt.Errorf("ext4: blkid: %w", err)
	}
	switch {
	case code == 0 && strings.TrimSpace(string(stdout)) != "":
		return true, nil
	case code == 2 || code == 0:
		if !d.Blank {
			return false, fmt.Errorf("ext4: %s holds data but no file system signature; not treating it as blank", dev)
		}
		return false, nil
	}
	return false, fmt.Errorf("ext4: blkid: exit %d: %s", code, strings.TrimSpace(string(stderr)))
}

// Fsck runs e2fsck -p. Exit 0 (clean), 1 (corrected) and 2 (corrected, reboot
// advised, which a mapping does not need) are success; 4 and above are ErrFsck.
func Fsck(ctx context.Context, r tool.Runner, dev string) error {
	_, stderr, code, err := r.Run(ctx, []string{"e2fsck", "-p", dev}, nil)
	if err != nil {
		return fmt.Errorf("ext4: e2fsck: %w", err)
	}
	// The exit code is a bit set; 3 is 1 and 2 together.
	if code >= 0 && code <= 3 {
		return nil
	}
	return fmt.Errorf("%w: exit %d: %s", ErrFsck, code, strings.TrimSpace(string(stderr)))
}

// Resize grows the filesystem on dev to the device's size. It works on a
// mounted filesystem.
func Resize(ctx context.Context, r tool.Runner, dev string) error {
	return run(ctx, r, "resize2fs", []string{"resize2fs", dev})
}

func run(ctx context.Context, r tool.Runner, op string, argv []string) error {
	_, stderr, code, err := r.Run(ctx, argv, nil)
	if err != nil {
		return fmt.Errorf("ext4: %s: %w", op, err)
	}
	if code != 0 {
		return fmt.Errorf("ext4: %s: exit %d: %s", op, code, strings.TrimSpace(string(stderr)))
	}
	return nil
}
