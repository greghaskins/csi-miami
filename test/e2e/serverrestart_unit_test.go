//go:build e2e

package e2e

import (
	"strings"
	"testing"
)

// These tests need no cluster: they check the reading of the holder pod's log
// and of the file it wrote, which the server restart test relies on.

func TestParseHeld(t *testing.T) {
	t.Parallel()
	log := strings.Join([]string{
		"holder: dd fsync available",
		"held write 1 ok",
		"held write 2 ok",
		"held write 3 failed: dd: fsync failed for 'standard output': Stale file handle 0+1 records in",
		"held write 4 failed:",
		"noise held write 9 ok",
		"held write x ok",
		"held write 5 ok",
	}, "\n")
	got := parseHeld(log)
	want := []heldWrite{
		{1, true, ""}, {2, true, ""},
		{3, false, "dd: fsync failed for 'standard output': Stale file handle 0+1 records in"},
		{4, false, ""}, {5, true, ""},
	}
	if len(got) != len(want) {
		t.Fatalf("parseHeld = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("attempt %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestSummarizeHeld(t *testing.T) {
	t.Parallel()
	writes := []heldWrite{
		{1, true, ""}, {2, true, ""}, {3, false, "e3"}, {4, false, "e4"}, {5, true, ""}, {6, false, "e6"},
	}
	s := summarizeHeld(writes, 3)
	if s.OKBefore != 2 || s.FailedBefore != 1 || s.OKAfter != 1 || s.FailedAfter != 2 {
		t.Errorf("summary = %+v", s)
	}
	if s.FirstFailure == nil || s.FirstFailure.N != 3 || s.LastFailure == nil || s.LastFailure.N != 6 {
		t.Errorf("first %v, last %v", s.FirstFailure, s.LastFailure)
	}
	if none := summarizeHeld(writes[:2], 3); none.FirstFailure != nil || none.LastFailure != nil || none.OKBefore != 2 {
		t.Errorf("summary of two good writes = %+v", none)
	}
}

func TestCheckHeld(t *testing.T) {
	t.Parallel()
	ok := func(ns ...int) []heldWrite {
		var out []heldWrite
		for _, n := range ns {
			out = append(out, heldWrite{N: n, OK: true})
		}
		return out
	}
	tests := []struct {
		name    string
		writes  []heldWrite
		file    string
		wantErr string
	}{
		{"every write is there", ok(1, 2, 3), "1\n2\n3\n", ""},
		{"a failed write may be there", append(ok(1), heldWrite{N: 2, Err: "x"}, heldWrite{N: 3, OK: true}), "1\n2\n3\n", ""},
		{"a failed write may be missing", append(ok(1), heldWrite{N: 2, Err: "x"}, heldWrite{N: 3, OK: true}), "1\n3\n", ""},
		{"an empty file and no writes", nil, "", ""},
		{"a write told ok is missing", ok(1, 2, 3), "1\n3\n", "2"},
		{"several are missing", ok(1, 2, 3), "1\n", "2, 3"},
		{"a number twice", ok(1, 2), "1\n2\n2\n", "repeated or reordered"},
		{"out of order", ok(1, 2), "2\n1\n", "repeated or reordered"},
		{"not a number", ok(1), "1\nx\n", "not a number"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := checkHeld(tt.writes, tt.file)
			switch {
			case tt.wantErr == "" && err != nil:
				t.Errorf("checkHeld = %v", err)
			case tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)):
				t.Errorf("checkHeld = %v, want an error containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestDescribeHeldFailure(t *testing.T) {
	t.Parallel()
	if got := describeHeldFailure(nil); got != "none" {
		t.Errorf("no failure = %q", got)
	}
	if got := describeHeldFailure(&heldWrite{N: 7, Err: "Stale file handle"}); !strings.Contains(got, "7") || !strings.Contains(got, "Stale file handle") {
		t.Errorf("failure = %q", got)
	}
}

func TestCheckHeldRecovered(t *testing.T) {
	t.Parallel()
	ok := func(n int) heldWrite { return heldWrite{N: n, OK: true} }
	fail := func(n int, why string) heldWrite { return heldWrite{N: n, Err: why} }
	tests := []struct {
		name    string
		writes  []heldWrite
		wantErr string
	}{
		{"all ok", []heldWrite{ok(1), ok(2), ok(3)}, ""},
		{"a timeout during the outage", []heldWrite{ok(1), fail(2, "dd: standard output: Connection timed out"), ok(3), ok(4)}, ""},
		{"the attempt in flight when the plugin said so may fail", []heldWrite{ok(1), ok(2), fail(3, "Connection timed out"), ok(4)}, ""},
		{"a failure after that is a descriptor that did not recover", []heldWrite{ok(1), ok(2), ok(3), fail(4, "Connection timed out"), ok(5)}, "did not recover"},
		{"a stale handle", []heldWrite{ok(1), fail(2, "dd: fsync failed for 'standard output': Stale file handle"), ok(3)}, "stale file handle"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := checkHeldRecovered(tt.writes, 2)
			switch {
			case tt.wantErr == "" && err != nil:
				t.Errorf("checkHeldRecovered = %v", err)
			case tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)):
				t.Errorf("checkHeldRecovered = %v, want an error containing %q", err, tt.wantErr)
			}
		})
	}
}
