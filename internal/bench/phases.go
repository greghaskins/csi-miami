// Package bench measures the metadata and sequential throughput of a volume's
// device stack, to decide gate G1: the nbd stack (blockd, LUKS2, ext4) must be
// within 1.5x of the same stack over a loop device on every phase.
//
// The phases are the ones of the design's section 3.2 that need no external
// tool. Each runs in a fresh directory on the stack's mount, the whole list
// runs once per round, and the report keeps every round's seconds; the gate
// reads the median of rounds 2 to N (decision 60).
package bench

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
)

// WorkPrefix starts the name of every directory the bench creates in --dir and
// --store. A directory holding nothing else is "empty or ours"; the bench never
// removes anything it did not create itself.
const WorkPrefix = "miami-bench-"

// Scale is how much each phase does. Full is the spec's.
type Scale struct {
	SmallFiles    int // files of 1 KiB, no fsync
	TreeFiles     int // files of 1 to 8 KiB spread over TreeDirs directories
	TreeDirs      int
	FsyncFiles    int   // files of 4 KiB, each created and fsynced
	InPlaceWrites int   // 4 KiB overwrites of an existing file, each fsynced
	SeqBytes      int64 // one sequential file, written, fsynced, read back
}

// FullScale is the phase sizes of the design's section 3.2.
func FullScale() Scale {
	return Scale{SmallFiles: 2000, TreeFiles: 5000, TreeDirs: 50, FsyncFiles: 1000, InPlaceWrites: 1000, SeqBytes: 512 << 20}
}

// SmallScale is a quick run for CI, which logs the table and enforces nothing.
func SmallScale() Scale {
	return Scale{SmallFiles: 400, TreeFiles: 1000, TreeDirs: 20, FsyncFiles: 200, InPlaceWrites: 200, SeqBytes: 64 << 20}
}

// env is what a phase needs besides the directory: the scale and a hook that
// empties the page cache (nil when the bench is not root).
type env struct {
	dir   string
	scale Scale
	drop  func() error
}

// phase is one timed step. setup, if any, runs first and is not timed.
type phase struct {
	name  string
	setup func(e *env) error
	run   func(ctx context.Context, e *env) error
}

// phaseList is the ordered phases. They share a round directory: the stat
// sweep walks what the first two wrote, and the last removes everything.
func phaseList(s Scale) []phase {
	return []phase{
		{name: fmt.Sprintf("create %d x 1 KiB files", s.SmallFiles), run: createSmall},
		{name: fmt.Sprintf("write %d-file tree", s.TreeFiles), run: writeTree},
		{name: "stat sweep", run: statSweep},
		{name: fmt.Sprintf("%d x create + fsync", s.FsyncFiles), run: createFsync},
		{name: fmt.Sprintf("%d x 4 KiB overwrite + fsync", s.InPlaceWrites), setup: prepareInPlace, run: overwriteFsync},
		{name: fmt.Sprintf("sequential write %d MiB", s.SeqBytes>>20), run: seqWrite},
		{name: fmt.Sprintf("sequential read %d MiB", s.SeqBytes>>20), setup: dropCaches, run: seqRead},
		{name: "rm -rf", run: removeAll},
	}
}

func createSmall(ctx context.Context, e *env) error {
	d := filepath.Join(e.dir, "small")
	if err := os.Mkdir(d, 0o750); err != nil {
		return err
	}
	buf := fill(1024, 1)
	for i := range e.scale.SmallFiles {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(d, fmt.Sprintf("f%05d", i)), buf, 0o644); err != nil { //nolint:gosec // bench file
			return err
		}
	}
	return nil
}

func writeTree(ctx context.Context, e *env) error {
	root := filepath.Join(e.dir, "tree")
	dirs := max(e.scale.TreeDirs, 1)
	for i := range dirs {
		if err := os.MkdirAll(filepath.Join(root, fmt.Sprintf("d%03d", i)), 0o750); err != nil {
			return err
		}
	}
	for i := range e.scale.TreeFiles {
		if err := ctx.Err(); err != nil {
			return err
		}
		name := filepath.Join(root, fmt.Sprintf("d%03d", i%dirs), fmt.Sprintf("f%05d", i))
		if err := os.WriteFile(name, fill((1+i%8)*1024, byte(i)), 0o644); err != nil { //nolint:gosec // bench file
			return err
		}
	}
	return nil
}

func statSweep(ctx context.Context, e *env) error {
	n := 0
	err := filepath.WalkDir(e.dir, func(path string, _ os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := os.Lstat(path); err != nil {
			return err
		}
		n++
		return nil
	})
	if err == nil && n == 0 {
		err = errors.New("the stat sweep found nothing")
	}
	return err
}

func createFsync(ctx context.Context, e *env) error {
	d := filepath.Join(e.dir, "fsync")
	if err := os.Mkdir(d, 0o750); err != nil {
		return err
	}
	buf := fill(4096, 2)
	for i := range e.scale.FsyncFiles {
		if err := ctx.Err(); err != nil {
			return err
		}
		f, err := os.Create(filepath.Join(d, fmt.Sprintf("f%05d", i))) //nolint:gosec // bench file
		if err != nil {
			return err
		}
		_, werr := f.Write(buf)
		if werr == nil {
			werr = f.Sync()
		}
		if cerr := f.Close(); werr == nil {
			werr = cerr
		}
		if werr != nil {
			return werr
		}
	}
	return nil
}

// overwriteFsync overwrites 4 KiB blocks of a file that already exists and has
// reached the disk; the setup is not timed (see runRound).
func overwriteFsync(ctx context.Context, e *env) error {
	f, err := os.OpenFile(filepath.Join(e.dir, "inplace"), os.O_WRONLY, 0) //nolint:gosec // bench file
	if err != nil {
		return err
	}
	buf := fill(4096, 3)
	for i := range e.scale.InPlaceWrites {
		if err := ctx.Err(); err != nil {
			_ = f.Close()
			return err
		}
		if _, err := f.WriteAt(buf, int64(i)*4096); err != nil {
			_ = f.Close()
			return err
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return err
		}
	}
	return f.Close()
}

func prepareInPlace(e *env) error {
	f, err := os.Create(filepath.Join(e.dir, "inplace")) //nolint:gosec // bench file
	if err != nil {
		return err
	}
	// Allocate real blocks: a sparse file would make the overwrites allocating
	// writes, which is a different phase.
	buf := fill(1<<20, 5)
	total := int64(e.scale.InPlaceWrites) * 4096
	for off := int64(0); off < total; off += int64(len(buf)) {
		n := min(int64(len(buf)), total-off)
		if _, err := f.WriteAt(buf[:n], off); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// dropCaches empties the page cache so the read comes from the device. The
// sync first makes the drop complete; without root there is no hook and the
// read may be served from memory (the report says so).
func dropCaches(e *env) error {
	if e.drop == nil {
		return nil
	}
	unix.Sync()
	if err := e.drop(); err != nil {
		return fmt.Errorf("dropping caches: %w", err)
	}
	return nil
}

func seqWrite(ctx context.Context, e *env) error {
	f, err := os.Create(filepath.Join(e.dir, "seq")) //nolint:gosec // bench file
	if err != nil {
		return err
	}
	buf := fill(1<<20, 6)
	for done := int64(0); done < e.scale.SeqBytes; {
		if err := ctx.Err(); err != nil {
			_ = f.Close()
			return err
		}
		n := min(int64(len(buf)), e.scale.SeqBytes-done)
		if _, err := f.Write(buf[:n]); err != nil {
			_ = f.Close()
			return err
		}
		done += n
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func seqRead(ctx context.Context, e *env) error {
	f, err := os.Open(filepath.Join(e.dir, "seq")) //nolint:gosec // bench file
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	buf := make([]byte, 1<<20)
	var got int64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := f.Read(buf)
		got += int64(n)
		if err != nil {
			if got != e.scale.SeqBytes {
				return fmt.Errorf("read %d bytes of %d", got, e.scale.SeqBytes)
			}
			return nil
		}
	}
}

func removeAll(ctx context.Context, e *env) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return os.RemoveAll(e.dir)
}

// fill returns n bytes that no filesystem or cipher can treat as zeros.
func fill(n int, seed byte) []byte {
	b := make([]byte, n)
	x := uint32(seed)*2654435761 + 1
	for i := range b {
		x = x*1664525 + 1013904223
		b[i] = byte(x >> 24)
	}
	return b
}

// runRound runs every phase once in a fresh directory under work and returns
// the names and seconds of each. Between phases the file system is synced,
// outside the timer, so one phase's dirty pages are not the next phase's
// writeback; the phases carry no sync of their own except where they say fsync.
func runRound(ctx context.Context, work string, round int, s Scale, drop func() error) ([]string, []float64, error) {
	e := &env{dir: filepath.Join(work, fmt.Sprintf("round%d", round)), scale: s, drop: drop}
	if err := os.Mkdir(e.dir, 0o750); err != nil {
		return nil, nil, err
	}
	phases := phaseList(s)
	names := make([]string, len(phases))
	secs := make([]float64, len(phases))
	for i, p := range phases {
		names[i] = p.name
		if p.setup != nil {
			if err := p.setup(e); err != nil {
				return nil, nil, fmt.Errorf("%s setup: %w", p.name, err)
			}
		}
		unix.Sync()
		start := time.Now()
		if err := p.run(ctx, e); err != nil {
			return nil, nil, fmt.Errorf("%s: %w", p.name, err)
		}
		secs[i] = time.Since(start).Seconds()
	}
	return names, secs, nil
}
