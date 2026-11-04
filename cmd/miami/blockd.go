package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os/signal"
	"syscall"
	"time"

	"github.com/greghaskins/csi-miami/internal/blockd"
)

// blockdFlags are the flags of `miami blockd`.
type blockdFlags struct {
	volume     string
	store      string
	cacheDir   string
	control    string
	size       int64
	chunkSize  int64
	cacheBound int64
	epoch      uint64
	node       string
	readOnly   bool
	timeout    time.Duration
	deadConn   time.Duration
}

// runBlockd parses the flags of `miami blockd` and serves one volume as an nbd
// device until a Stop RPC or SIGTERM/SIGINT, then stops gracefully. Help goes
// to stdout and exits 0; a flag error goes to stderr and exits 2; a failure to
// serve exits 1.
func runBlockd(args []string, stdout, stderr io.Writer) int {
	var out bytes.Buffer
	var f blockdFlags
	fs := flag.NewFlagSet("miami blockd", flag.ContinueOnError)
	fs.SetOutput(&out)
	fs.StringVar(&f.volume, "volume", "", "the volume name; also the nbd backend identifier (required)")
	fs.StringVar(&f.store, "store", "", "the volume's directory on the transport (required)")
	fs.StringVar(&f.cacheDir, "cache-dir", "", "the node-local cache directory (required)")
	fs.StringVar(&f.control, "control", "", "the control socket to listen on (required)")
	fs.Int64Var(&f.size, "size", 0, "device size in bytes, a whole number of chunks (required)")
	fs.Int64Var(&f.chunkSize, "chunk-size", 0, "chunk size in bytes, a power of two (required)")
	fs.Int64Var(&f.cacheBound, "cache-bound", 0, "cache byte bound (required)")
	fs.Uint64Var(&f.epoch, "epoch", 0, "the holder epoch to publish under (required)")
	fs.StringVar(&f.node, "node", "", "this node's name (required)")
	fs.BoolVar(&f.readOnly, "read-only", false, "serve the device read-only")
	fs.DurationVar(&f.timeout, "timeout", blockd.DefaultTimeout, "nbd request timeout")
	fs.DurationVar(&f.deadConn, "dead-conn-timeout", blockd.DefaultDeadConnTimeout, "nbd dead-connection timeout")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			_, _ = stdout.Write(out.Bytes())
			return 0
		}
		_, _ = stderr.Write(out.Bytes())
		return 2
	}
	if fs.NArg() > 0 {
		_, _ = fmt.Fprintf(stderr, "miami blockd: unexpected argument %q\n", fs.Arg(0))
		return 2
	}
	for _, p := range []struct {
		msg string
		bad bool
	}{
		{"-volume is required", f.volume == ""},
		{"-store is required", f.store == ""},
		{"-cache-dir is required", f.cacheDir == ""},
		{"-control is required", f.control == ""},
		{"-node is required", f.node == ""},
		{"-size must be positive", f.size <= 0},
		{"-chunk-size must be positive", f.chunkSize <= 0},
		{"-cache-bound must be positive", f.cacheBound <= 0},
		{"-epoch must be positive", f.epoch == 0},
	} {
		if p.bad {
			_, _ = fmt.Fprintf(stderr, "miami blockd: %s\n", p.msg)
			return 2
		}
	}

	cfg := blockd.Config{
		Volume:          f.volume,
		StoreDir:        f.store,
		CacheDir:        f.cacheDir,
		ControlSocket:   f.control,
		Size:            f.size,
		Chunk:           f.chunkSize,
		CacheBound:      f.cacheBound,
		Epoch:           f.epoch,
		Node:            f.node,
		ReadOnly:        f.readOnly,
		Timeout:         f.timeout,
		DeadConnTimeout: f.deadConn,
		Holder:          blockd.NewFileChecker(f.store, f.volume, f.node, f.epoch),
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	log := slog.New(slog.NewTextHandler(stderr, nil))
	if err := blockd.Run(ctx, cfg, log); err != nil {
		_, _ = fmt.Fprintf(stderr, "miami blockd: %v\n", err)
		return 1
	}
	return 0
}
