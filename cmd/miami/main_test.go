package main

import (
	"bytes"
	"testing"
)

func TestRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		args       []string
		wantCode   int
		wantStdout string
		wantStderr string
	}{
		{name: "version", args: []string{"version"}, wantStdout: version + "\n"},
		{name: "node", args: []string{"node"}, wantStdout: "miami node " + version + "\n"},
		{name: "lock", args: []string{"lock"}, wantStdout: "miami lock " + version + "\n"},

		{name: "no command prints usage", args: nil, wantCode: 2, wantStderr: usage},
		{name: "-h", args: []string{"-h"}, wantStdout: usage},
		{name: "-help", args: []string{"-help"}, wantStdout: usage},
		{name: "--help", args: []string{"--help"}, wantStdout: usage},
		{name: "help", args: []string{"help"}, wantStdout: usage},

		{name: "node -h", args: []string{"node", "-h"}, wantStdout: "Usage of miami node:\n"},
		{name: "lock --help", args: []string{"lock", "--help"}, wantStdout: "Usage of miami lock:\n"},
		{name: "version -h", args: []string{"version", "-h"}, wantStdout: "Usage of miami version:\n"},

		{
			name:       "unknown command",
			args:       []string{"bogus"},
			wantCode:   2,
			wantStderr: "miami: unknown command \"bogus\"\n\n" + usage,
		},
		{
			name:       "extra argument",
			args:       []string{"node", "x"},
			wantCode:   2,
			wantStderr: "miami node: unexpected argument \"x\"\n",
		},
		{
			name:       "unknown flag",
			args:       []string{"node", "-nope"},
			wantCode:   2,
			wantStderr: "flag provided but not defined: -nope\nUsage of miami node:\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var stdout, stderr bytes.Buffer
			code := run(tt.args, &stdout, &stderr)

			if code != tt.wantCode {
				t.Errorf("exit code = %d, want %d", code, tt.wantCode)
			}
			if got := stdout.String(); got != tt.wantStdout {
				t.Errorf("stdout = %q, want %q", got, tt.wantStdout)
			}
			if got := stderr.String(); got != tt.wantStderr {
				t.Errorf("stderr = %q, want %q", got, tt.wantStderr)
			}
		})
	}
}
