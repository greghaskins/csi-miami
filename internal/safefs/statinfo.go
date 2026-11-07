package safefs

import (
	"io/fs"
	"time"

	"golang.org/x/sys/unix"
)

// statInfo is fs.FileInfo over a fstatat result.
type statInfo struct {
	name string
	st   unix.Stat_t
}

func (s statInfo) Name() string       { return s.name }
func (s statInfo) Size() int64        { return s.st.Size }
func (s statInfo) ModTime() time.Time { return time.Unix(int64(s.st.Mtim.Sec), int64(s.st.Mtim.Nsec)) } //nolint:unconvert // Sec is int32 on some platforms
func (s statInfo) IsDir() bool        { return s.Mode().IsDir() }
func (s statInfo) Sys() any           { return &s.st }

func (s statInfo) Mode() fs.FileMode {
	m := fs.FileMode(s.st.Mode & 0o777)
	switch s.st.Mode & unix.S_IFMT {
	case unix.S_IFDIR:
		m |= fs.ModeDir
	case unix.S_IFLNK:
		m |= fs.ModeSymlink
	case unix.S_IFREG:
	default:
		m |= fs.ModeIrregular
	}
	return m
}
