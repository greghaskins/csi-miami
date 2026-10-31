// Command miami is the csi-miami driver: one binary with a subcommand per
// role. In this scaffold each role only reports its name and the build
// version.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

const usage = `usage: miami <command>

commands:
  node        run the node plugin
  controller  run the controller plugin
  lock        run the lock helper
  version     print the build version
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run dispatches args to a subcommand and returns the process exit code.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		_, _ = fmt.Fprint(stderr, usage)
		return 2
	}

	switch args[0] {
	case "-h", "-help", "--help", "help":
		_, _ = fmt.Fprint(stdout, usage)
		return 0
	case "version":
		return runCommand("version", args[1:], stderr, func() {
			_, _ = fmt.Fprintln(stdout, version)
		})
	case "node", "controller", "lock":
		name := args[0]
		return runCommand(name, args[1:], stderr, func() {
			_, _ = fmt.Fprintf(stdout, "miami %s %s\n", name, version)
		})
	default:
		_, _ = fmt.Fprintf(stderr, "miami: unknown command %q\n\n%s", args[0], usage)
		return 2
	}
}

// runCommand parses the flags of one subcommand and, if they are valid, runs
// action.
func runCommand(name string, args []string, stderr io.Writer, action func()) int {
	fs := flag.NewFlagSet("miami "+name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() > 0 {
		_, _ = fmt.Fprintf(stderr, "miami %s: unexpected argument %q\n", name, fs.Arg(0))
		return 2
	}
	action()
	return 0
}
