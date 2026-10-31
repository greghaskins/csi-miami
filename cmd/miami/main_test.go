package main

import (
	"bytes"
	"strings"
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
		{name: "controller", args: []string{"controller"}, wantStdout: "miami controller " + version + "\n"},
		{name: "lock", args: []string{"lock"}, wantStdout: "miami lock " + version + "\n"},
		{name: "no command prints usage", args: nil, wantCode: 2, wantStderr: "usage: miami"},
		{name: "-h prints usage", args: []string{"-h"}, wantStdout: "usage: miami"},
		{name: "unknown command", args: []string{"bogus"}, wantCode: 2, wantStderr: `unknown command "bogus"`},
		{name: "extra argument", args: []string{"node", "x"}, wantCode: 2, wantStderr: `unexpected argument "x"`},
		{name: "unknown flag", args: []string{"node", "-nope"}, wantCode: 2, wantStderr: "flag provided but not defined"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var stdout, stderr bytes.Buffer
			code := run(tt.args, &stdout, &stderr)

			if code != tt.wantCode {
				t.Errorf("exit code = %d, want %d", code, tt.wantCode)
			}
			if !strings.Contains(stdout.String(), tt.wantStdout) {
				t.Errorf("stdout = %q, want it to contain %q", stdout.String(), tt.wantStdout)
			}
			if !strings.Contains(stderr.String(), tt.wantStderr) {
				t.Errorf("stderr = %q, want it to contain %q", stderr.String(), tt.wantStderr)
			}
		})
	}
}

func TestDefaultVersion(t *testing.T) {
	t.Parallel()

	if version == "" {
		t.Fatal("version must never be empty")
	}
}
