package bench

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/greghaskins/csi-miami/internal/chunk"
	"github.com/greghaskins/csi-miami/internal/crypt"
)

// Stacks the bench can build.
const (
	StackNBD  = "nbd"  // chunk store, blockd, LUKS2, ext4: the production stack
	StackLoop = "loop" // file, loop device, LUKS2, ext4: the baseline
	StackNone = "none" // the directory as it is
)

// Options are the bench's inputs.
type Options struct {
	Stack  string
	Dir    string // local directory: the loop image, the nbd cache, or the tree for "none"
	Store  string // the chunk store's parent directory (nbd only)
	Size   int64  // device size in bytes (nbd and loop)
	Rounds int
	Scale  Scale
	// The parameters a sweep varies. Each zero value is the driver's default,
	// which is what the bench ran before they existed.
	//
	// ChunkSize is the nbd stack's chunk size (chunk.DefaultChunkSize of Size
	// when zero). Cipher is the LUKS2 cipher of the nbd and loop stacks
	// (crypt.DefaultCipher when empty). CacheBound is the nbd stack's cache
	// bound (the rounded size when zero: the whole volume fits). With a
	// GenerationInterval the nbd stack publishes a generation (freeze,
	// boundary, thaw, publish) every interval while the rounds run, so the
	// phase times include them; zero runs none during the rounds.
	ChunkSize          int64
	Cipher             string
	CacheBound         int64
	GenerationInterval time.Duration
	// Exe is the miami binary the nbd stack starts as `blockd`; the running
	// executable when empty.
	Exe string
	// Log receives progress lines; nil discards them.
	Log io.Writer
}

// cipher is the LUKS2 cipher the stacks use.
func (o Options) cipher() string {
	if o.Cipher == "" {
		return crypt.DefaultCipher
	}
	return o.Cipher
}

// nbdGeometry is the nbd stack's chunk size, device size (whole chunks) and
// cache bound.
func (o Options) nbdGeometry() (chunkSize, size, bound int64) {
	chunkSize = o.ChunkSize
	if chunkSize == 0 {
		chunkSize = chunk.DefaultChunkSize(o.Size)
	}
	size = chunk.RoundUp(o.Size, chunkSize)
	bound = o.CacheBound
	if bound == 0 {
		bound = size
	}
	return chunkSize, size, bound
}

func (o Options) logf(format string, args ...any) {
	if o.Log != nil {
		_, _ = fmt.Fprintf(o.Log, "bench: "+format+"\n", args...)
	}
}

// CheckDir accepts a directory that does not exist, is empty, or holds only
// entries this bench made (names starting with WorkPrefix). Anything else is
// somebody's data and is refused, naming the first such entry.
func CheckDir(path string) error {
	fi, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return fmt.Errorf("bench: %s is not a directory", path)
	}
	ents, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	for _, e := range ents {
		if !strings.HasPrefix(e.Name(), WorkPrefix) {
			return fmt.Errorf("bench: refusing to use %s: it holds %q, which this bench did not create; give it an empty directory", path, e.Name())
		}
	}
	return nil
}

// Run builds the stack, runs the phases for the configured rounds and tears the
// stack down again on every path, including a cancelled ctx. The report is
// returned even when the teardown failed, with the teardown error.
func Run(ctx context.Context, o Options) (rep Report, runErr error) {
	switch o.Stack {
	case StackNBD, StackLoop, StackNone:
	default:
		return Report{}, fmt.Errorf("bench: unknown stack %q (nbd, loop or none)", o.Stack)
	}
	if o.Dir == "" {
		return Report{}, errors.New("bench: a directory is required")
	}
	if o.Rounds <= 0 {
		o.Rounds = 2
	}
	if o.Stack != StackNone && os.Geteuid() != 0 {
		return Report{}, fmt.Errorf("bench: the %s stack mounts file systems and must run as root", o.Stack)
	}
	if o.Stack != StackNone && o.Size <= 0 {
		return Report{}, errors.New("bench: a positive size is required")
	}
	if o.Stack == StackNBD && o.Store == "" {
		return Report{}, errors.New("bench: the nbd stack needs a store directory")
	}
	if err := CheckDir(o.Dir); err != nil {
		return Report{}, err
	}
	if o.Stack == StackNBD {
		if err := CheckDir(o.Store); err != nil {
			return Report{}, err
		}
	}
	if err := os.MkdirAll(o.Dir, 0o755); err != nil { //nolint:gosec // the operator's bench directory
		return Report{}, err
	}
	work, err := os.MkdirTemp(o.Dir, WorkPrefix)
	if err != nil {
		return Report{}, err
	}

	rep = Report{Stack: o.Stack, SizeBytes: o.Size, Rounds: o.Rounds}
	st, err := newStack(ctx, o, work)
	if err != nil {
		return rep, err
	}
	defer func() {
		if cerr := st.close(); cerr != nil {
			runErr = errors.Join(runErr, fmt.Errorf("teardown: %w", cerr))
		}
	}()

	drop, note := dropHook()
	rep.Notes = append(rep.Notes, note)
	if o.Stack != StackNone {
		rep.Notes = append(rep.Notes, "encrypted with LUKS2 "+o.cipher()+" on both stacks")
		rep.Params.Cipher = o.cipher()
	}
	if o.Stack == StackNBD {
		c, _, bound := o.nbdGeometry()
		rep.Params.ChunkBytes, rep.Params.CacheBoundBytes = c, bound
		rep.Params.GenerationIntervalSeconds = o.GenerationInterval.Seconds()
	}

	p, canPublish := st.(publisher)
	stopGens := func() error { return nil }
	if canPublish && o.GenerationInterval > 0 {
		stopGens = startGenerations(ctx, p, o, &rep)
	}
	for round := 1; round <= o.Rounds; round++ {
		o.logf("round %d of %d", round, o.Rounds)
		names, secs, rerr := runRound(ctx, st.mountpoint(), round, o.Scale, drop)
		if rerr != nil {
			return rep, errors.Join(rerr, stopGens())
		}
		if round == 1 {
			for _, n := range names {
				rep.Phases = append(rep.Phases, PhaseResult{Name: n})
			}
		}
		for i, s := range secs {
			rep.Phases[i].Seconds = append(rep.Phases[i].Seconds, s)
		}
	}
	if err := stopGens(); err != nil {
		return rep, err
	}
	if canPublish {
		o.logf("publishing a generation")
		secs, perr := p.publish(ctx)
		if perr != nil {
			return rep, fmt.Errorf("publish: %w", perr)
		}
		rep.PublishSeconds = secs
	}
	return rep, nil
}

// startGenerations publishes a generation every o.GenerationInterval until the
// returned stop is called, recording each one's seconds in rep. stop waits for
// a publish in flight and returns the first error, after which no more were
// tried.
func startGenerations(ctx context.Context, p publisher, o Options, rep *Report) (stop func() error) {
	gctx, cancel := context.WithCancel(ctx)
	var (
		mu    sync.Mutex
		secs  []float64
		first error
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(o.GenerationInterval)
		defer t.Stop()
		for {
			select {
			case <-gctx.Done():
				return
			case <-t.C:
			}
			s, err := p.publish(gctx)
			mu.Lock()
			if err != nil {
				if gctx.Err() == nil {
					first = fmt.Errorf("generation during the rounds: %w", err)
				}
				mu.Unlock()
				return
			}
			secs = append(secs, s)
			mu.Unlock()
			o.logf("generation published in %.2f s", s)
		}
	}()
	return func() error {
		cancel()
		<-done
		mu.Lock()
		defer mu.Unlock()
		rep.GenerationSeconds = secs
		return first
	}
}

// dropHook returns the function that empties the page cache and the note that
// says whether it will. Without root, or where /proc/sys is read-only, it is
// nil.
func dropHook() (func() error, string) {
	if os.Geteuid() != 0 {
		return nil, "not root: the page cache is not dropped before the sequential read, which may be served from memory"
	}
	drop := func() error { return os.WriteFile("/proc/sys/vm/drop_caches", []byte("3"), 0o200) }
	if err := drop(); err != nil {
		return nil, fmt.Sprintf("the page cache cannot be dropped (%v): the sequential read may be served from memory", err)
	}
	return drop, "page cache dropped before the sequential read"
}
