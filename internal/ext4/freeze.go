package ext4

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// The freeze ioctls, from <linux/fs.h>: _IOWR('X', 119, int) and 120. The
// unix package does not export them.
const (
	fifreeze = 0xC0045877
	fithaw   = 0xC0045878
)

// Freeze flushes the filesystem and blocks writers until Thaw. fd is any
// descriptor on the mounted filesystem, such as its root directory.
func Freeze(fd int) error {
	if err := unix.IoctlSetInt(fd, fifreeze, 0); err != nil {
		return fmt.Errorf("ext4: FIFREEZE: %w", err)
	}
	return nil
}

// Thaw releases a Freeze.
func Thaw(fd int) error {
	if err := unix.IoctlSetInt(fd, fithaw, 0); err != nil {
		return fmt.Errorf("ext4: FITHAW: %w", err)
	}
	return nil
}
