package cachecap

import (
	"context"
	"fmt"
	"sync"

	"golang.org/x/sys/unix"
)

// FSStat reports the size and the space available to unprivileged writers, in
// bytes, of the filesystem holding dir.
type FSStat func(dir string) (size, free int64, err error)

// RealFSStat is the FSStat that asks the kernel.
func RealFSStat(dir string) (size, free int64, err error) {
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return 0, 0, fmt.Errorf("statfs %s: %w", dir, err)
	}
	// A filesystem larger than 8 EiB does not exist, so the conversions hold.
	bs := int64(st.Bsize)                                    //nolint:gosec // block size is small and positive
	return int64(st.Blocks) * bs, int64(st.Bavail) * bs, nil //nolint:gosec // see above
}

// Source answers what the node's budget is now. The stage check and the
// advertisement Runner share one, so they never disagree.
type Source struct {
	cfg      Config
	cacheDir string
	stat     FSStat

	mu         sync.Mutex
	size, free int64
}

// NewSource returns a Source for the filesystem holding cacheDir.
func NewSource(c Config, cacheDir string, stat FSStat) *Source {
	return &Source{cfg: c, cacheDir: cacheDir, stat: stat}
}

// Current statfs-es the cache directory and returns the budget; enabled is
// false when accounting is off. A budget of zero with enabled true is a real
// answer.
func (s *Source) Current(ctx context.Context) (budget int64, enabled bool, err error) {
	if !s.cfg.Accounting {
		return 0, false, nil
	}
	if err := ctx.Err(); err != nil {
		return 0, true, err
	}
	size, free, err := s.stat(s.cacheDir)
	if err != nil {
		return 0, true, err
	}
	s.mu.Lock()
	s.size, s.free = size, free
	s.mu.Unlock()
	return Budget(size, s.cfg), true, nil
}

// Stats returns the filesystem figures of the latest successful Current, or
// (0, 0) before any.
func (s *Source) Stats() (fsSize, fsFree int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size, s.free
}
