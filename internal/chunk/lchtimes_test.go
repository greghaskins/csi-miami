package chunk

import (
	"time"

	"golang.org/x/sys/unix"
)

// unixLchtimes sets the times of a symlink itself.
func unixLchtimes(path string, t time.Time) error {
	ts := []unix.Timespec{unix.NsecToTimespec(t.UnixNano()), unix.NsecToTimespec(t.UnixNano())}
	return unix.UtimesNanoAt(unix.AT_FDCWD, path, ts, unix.AT_SYMLINK_NOFOLLOW)
}
