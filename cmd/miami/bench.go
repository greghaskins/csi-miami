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
	"syscall"

	"github.com/greghaskins/csi-miami/internal/bench"
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
	var out bytes.Buffer
	var (
		stack, dir, store, size, scale, outFile string
		rounds                                  int
		asJSON, compare                         bool
	)
	fs := flag.NewFlagSet("miami bench", flag.ContinueOnError)
	fs.SetOutput(&out)
	fs.StringVar(&stack, "stack", "", "nbd (chunk store, blockd, LUKS2, ext4), loop (file, loop device, LUKS2, ext4) or none (the directory as is)")
	fs.StringVar(&dir, "dir", "", "an empty local directory: the loop image, the nbd cache, or the tree for none")
	fs.StringVar(&store, "store", "", "an empty directory for the chunk store (nbd only), on the transport under test")
	fs.StringVar(&size, "size", "8Gi", "device size (nbd and loop)")
	fs.IntVar(&rounds, "rounds", 2, "rounds; the last one is the one compared")
	fs.StringVar(&scale, "scale", "full", "full (the design's phase sizes) or small (a quick run for CI)")
	fs.StringVar(&outFile, "out", "", "also write the JSON report to this file")
	fs.BoolVar(&asJSON, "json", false, "print the report as JSON")
	fs.BoolVar(&compare, "compare", false, "compare two JSON reports given as arguments (one nbd, one loop) against the 1.5x limit of gate G1; exit 1 on FAIL")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			_, _ = stdout.Write(out.Bytes())
			return 0
		}
		_, _ = stderr.Write(out.Bytes())
		return 2
	}
	if compare {
		if fs.NArg() != 2 {
			_, _ = fmt.Fprintln(stderr, "miami bench: --compare takes two report files")
			return 2
		}
		return benchCompare(fs.Arg(0), fs.Arg(1), asJSON, stdout, stderr)
	}
	if fs.NArg() > 0 {
		_, _ = fmt.Fprintf(stderr, "miami bench: unexpected argument %q\n", fs.Arg(0))
		return 2
	}
	if stack == "" || dir == "" {
		_, _ = fmt.Fprintln(stderr, "miami bench: --stack and --dir are required")
		return 2
	}
	if stack == bench.StackNBD && store == "" {
		_, _ = fmt.Fprintln(stderr, "miami bench: --stack nbd needs --store")
		return 2
	}
	n, err := bench.ParseSize(size)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "miami bench: %v\n", err)
		return 2
	}
	var sc bench.Scale
	switch scale {
	case "full":
		sc = bench.FullScale()
	case "small":
		sc = bench.SmallScale()
	default:
		_, _ = fmt.Fprintf(stderr, "miami bench: unknown --scale %q (full or small)\n", scale)
		return 2
	}
	if rounds < 1 {
		_, _ = fmt.Fprintln(stderr, "miami bench: --rounds must be at least 1")
		return 2
	}
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
	rep, err := bench.Run(ctx, bench.Options{
		Stack: stack, Dir: dir, Store: store, Size: n, Rounds: rounds, Scale: sc, Exe: exe, Log: stderr,
	})
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
