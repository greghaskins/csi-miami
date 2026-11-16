package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/greghaskins/csi-miami/internal/bench"
	"github.com/greghaskins/csi-miami/internal/chunk"
	"github.com/greghaskins/csi-miami/internal/crypt"
)

const (
	exitBenchFail  = 1 // --compare: some phase is over the limit
	exitBenchError = 3 // --compare: the reports could not be read or compared
)

// runBench is `miami bench`: it builds a device stack, times the metadata and
// sequential phases of the design's section 3.2 on it, and prints a table or
// JSON. With --compare it instead reads an nbd report and a loop report and
// prints the per-phase ratio and gate G1's verdict.
//
// Exit codes: 0 the run finished (or the comparison passed), 1 the run failed
// or, with --compare, the comparison failed the gate (exitBenchFail), 2 a usage
// error, 3 a comparison that could not be made (exitBenchError).
func runBench(args []string, stdout, stderr io.Writer) int {
	f, err := parseBenchFlags(args)
	if errors.Is(err, flag.ErrHelp) {
		_, _ = io.WriteString(stdout, f.usage)
		return 0
	}
	if err != nil {
		if f.usage != "" {
			_, _ = io.WriteString(stderr, f.usage)
		} else {
			_, _ = fmt.Fprintf(stderr, "miami bench: %v\n", err)
		}
		return 2
	}
	if f.compare {
		return benchCompare(f.files[0], f.files[1], f.asJSON, stdout, stderr)
	}
	asJSON, outFile := f.asJSON, f.outFile
	if outFile != "" {
		if _, err := os.Lstat(outFile); err == nil {
			_, _ = fmt.Fprintf(stderr, "miami bench: refusing to overwrite %s\n", outFile)
			return 1
		}
	}
	exe, err := os.Executable()
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "miami bench: %v\n", err)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	f.opts.Exe, f.opts.Log = exe, stderr
	rep, err := bench.Run(ctx, f.opts)
	if err != nil {
		if ctx.Err() != nil {
			_, _ = fmt.Fprintln(stderr, "miami bench: interrupted; the stack was torn down")
		}
		_, _ = fmt.Fprintf(stderr, "miami bench: %v\n", err)
		return 1
	}
	if outFile != "" {
		if err := writeReportFile(outFile, rep); err != nil {
			_, _ = fmt.Fprintf(stderr, "miami bench: %v\n", err)
			return 1
		}
	}
	if asJSON {
		if err := rep.WriteJSON(stdout); err != nil {
			_, _ = fmt.Fprintf(stderr, "miami bench: %v\n", err)
			return 1
		}
		return 0
	}
	rep.WriteTable(stdout)
	return 0
}

// benchFlags is a parsed `miami bench` command line.
type benchFlags struct {
	opts            bench.Options
	compare, asJSON bool
	outFile         string
	files           []string // --compare's two reports
	// usage is the flag package's message for a parse error or -h; empty for
	// an error found after parsing.
	usage string
}

// The ciphers --cipher takes: the ones a StorageClass may name.
var benchCiphers = []string{crypt.DefaultCipher, "aes-xts-plain64"}

// parseBenchFlags parses and checks a `miami bench` command line. Every error
// is a usage error (exit 2).
func parseBenchFlags(args []string) (benchFlags, error) {
	var (
		f                                                     benchFlags
		out                                                   bytes.Buffer
		stack, dir, store, size, scale, chunkSize, cacheBound string
		rounds                                                int
	)
	fs := flag.NewFlagSet("miami bench", flag.ContinueOnError)
	fs.SetOutput(&out)
	fs.StringVar(&stack, "stack", "", "nbd (chunk store, blockd, LUKS2, ext4), loop (file, loop device, LUKS2, ext4) or none (the directory as is)")
	fs.StringVar(&dir, "dir", "", "an empty local directory: the loop image, the nbd cache, or the tree for none")
	fs.StringVar(&store, "store", "", "an empty directory for the chunk store (nbd only), on the transport under test")
	fs.StringVar(&size, "size", "8Gi", "device size (nbd and loop)")
	fs.IntVar(&rounds, "rounds", 2, "rounds; round 1 is warm-up, --compare uses the median of the rest (use 5 for the gate)")
	fs.StringVar(&scale, "scale", "full", "full (the design's phase sizes) or small (a quick run for CI)")
	fs.StringVar(&f.outFile, "out", "", "also write the JSON report to this file")
	fs.BoolVar(&f.asJSON, "json", false, "print the report as JSON")
	fs.BoolVar(&f.compare, "compare", false, "compare two JSON reports given as arguments (one nbd, one loop) against the 1.5x limit of gate G1; exit 1 on FAIL")
	fs.StringVar(&chunkSize, "chunk-size", "", "chunk size (nbd only), a power of two from 1Mi to 256Mi (default: the driver's for --size)")
	fs.StringVar(&f.opts.Cipher, "cipher", "", "LUKS2 cipher (nbd and loop): "+strings.Join(benchCiphers, " or ")+" (default "+crypt.DefaultCipher+")")
	fs.StringVar(&cacheBound, "cache-bound", "", "cache byte bound (nbd only), at least the chunk size (default: the whole device)")
	fs.DurationVar(&f.opts.GenerationInterval, "generation-interval", 0, "publish a generation this often during the rounds (nbd only), at least 1s (default: none during the rounds)")

	if err := fs.Parse(args); err != nil {
		f.usage = out.String()
		return f, err
	}
	if f.compare {
		if fs.NArg() != 2 {
			return f, errors.New("--compare takes two report files")
		}
		f.files = fs.Args()
		return f, nil
	}
	if fs.NArg() > 0 {
		return f, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	if stack == "" || dir == "" {
		return f, errors.New("--stack and --dir are required")
	}
	if stack == bench.StackNBD && store == "" {
		return f, errors.New("--stack nbd needs --store")
	}
	n, err := bench.ParseSize(size)
	if err != nil {
		return f, err
	}
	switch scale {
	case "full":
		f.opts.Scale = bench.FullScale()
	case "small":
		f.opts.Scale = bench.SmallScale()
	default:
		return f, fmt.Errorf("unknown --scale %q (full or small)", scale)
	}
	if rounds < 1 {
		return f, errors.New("--rounds must be at least 1")
	}
	f.opts.Stack, f.opts.Dir, f.opts.Store, f.opts.Size, f.opts.Rounds = stack, dir, store, n, rounds

	nbdOnly := func(name string) error {
		return fmt.Errorf("--%s applies to --stack nbd only", name)
	}
	if chunkSize != "" {
		if stack != bench.StackNBD {
			return f, nbdOnly("chunk-size")
		}
		if f.opts.ChunkSize, err = bench.ParseSize(chunkSize); err != nil {
			return f, fmt.Errorf("--chunk-size: %w", err)
		}
		if err := chunk.CheckChunkSize(n, f.opts.ChunkSize); err != nil {
			return f, fmt.Errorf("--chunk-size: %w", err)
		}
	}
	if cacheBound != "" {
		if stack != bench.StackNBD {
			return f, nbdOnly("cache-bound")
		}
		if f.opts.CacheBound, err = bench.ParseSize(cacheBound); err != nil {
			return f, fmt.Errorf("--cache-bound: %w", err)
		}
		c := f.opts.ChunkSize
		if c == 0 {
			c = chunk.DefaultChunkSize(n)
		}
		if f.opts.CacheBound < c {
			return f, fmt.Errorf("--cache-bound %d is below the chunk size %d", f.opts.CacheBound, c)
		}
	}
	if f.opts.Cipher != "" {
		if stack == bench.StackNone {
			return f, errors.New("--cipher applies to --stack nbd and loop only")
		}
		if !slices.Contains(benchCiphers, f.opts.Cipher) {
			return f, fmt.Errorf("unknown --cipher %q (%s)", f.opts.Cipher, strings.Join(benchCiphers, " or "))
		}
	}
	if f.opts.GenerationInterval != 0 {
		if stack != bench.StackNBD {
			return f, nbdOnly("generation-interval")
		}
		if f.opts.GenerationInterval < time.Second {
			return f, fmt.Errorf("--generation-interval %s is below 1s", f.opts.GenerationInterval)
		}
	}
	return f, nil
}

func benchCompare(a, b string, asJSON bool, stdout, stderr io.Writer) int {
	nbd, loop, err := bench.ReadPair(a, b)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "miami bench: %v\n", err)
		return exitBenchError
	}
	c, err := bench.Compare(nbd, loop)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "miami bench: %v\n", err)
		return exitBenchError
	}
	if asJSON {
		if err := bench.WriteJSON(stdout, c); err != nil {
			_, _ = fmt.Fprintf(stderr, "miami bench: %v\n", err)
			return exitBenchError
		}
	} else {
		c.WriteTable(stdout)
	}
	if !c.Pass {
		return exitBenchFail
	}
	return 0
}

func writeReportFile(path string, rep bench.Report) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644) //nolint:gosec // operator-supplied output path; O_EXCL refuses an existing file
	if err != nil {
		return err
	}
	if err := rep.WriteJSON(f); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
