package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func tiny() Scale {
	return Scale{SmallFiles: 20, TreeFiles: 50, TreeDirs: 5, FsyncFiles: 5, InPlaceWrites: 5, SeqBytes: 2 << 20}
}

func TestPhasesRunOnTmpDir(t *testing.T) {
	dir := t.TempDir()
	rep, err := Run(context.Background(), Options{Dir: dir, Scale: tiny(), Rounds: 2, Stack: "none"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"create 20 x 1 KiB files", "write 50-file tree", "stat sweep", "5 x create + fsync",
		"5 x 4 KiB overwrite + fsync", "sequential write 2 MiB", "sequential read 2 MiB", "rm -rf",
	}
	if len(rep.Phases) != len(want) {
		t.Fatalf("got %d phases, want %d", len(rep.Phases), len(want))
	}
	for i, p := range rep.Phases {
		if p.Name != want[i] {
			t.Errorf("phase %d is %q, want %q", i, p.Name, want[i])
		}
		if len(p.Seconds) != 2 {
			t.Errorf("%s: %d rounds, want 2", p.Name, len(p.Seconds))
		}
		for _, s := range p.Seconds {
			if s <= 0 {
				t.Errorf("%s: non-positive time %v", p.Name, s)
			}
		}
	}
	// The round directories are gone: only the bench's own directory is left
	// and it is empty or removed.
	ents, _ := os.ReadDir(dir)
	for _, e := range ents {
		if !strings.HasPrefix(e.Name(), WorkPrefix) {
			t.Errorf("stray entry %s", e.Name())
		}
	}
}

func TestPhasesStopOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Run(ctx, Options{Dir: t.TempDir(), Scale: tiny(), Rounds: 1, Stack: "none"}); err == nil {
		t.Fatal("a cancelled context ran the phases")
	}
}

func TestRefusesNonEmptyDir(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "precious"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Run(context.Background(), Options{Dir: dir, Scale: tiny(), Rounds: 1, Stack: "none"})
	if err == nil || !strings.Contains(err.Error(), "precious") {
		t.Fatalf("err = %v, want a refusal naming the file", err)
	}
	if _, serr := os.Stat(filepath.Join(dir, "precious")); serr != nil {
		t.Fatal("the user's file was touched")
	}
}

func TestCheckDirAcceptsEmptyMissingAndOurs(t *testing.T) {
	base := t.TempDir()
	if err := CheckDir(filepath.Join(base, "missing")); err != nil {
		t.Errorf("missing: %v", err)
	}
	if err := CheckDir(base); err != nil {
		t.Errorf("empty: %v", err)
	}
	if err := os.Mkdir(filepath.Join(base, WorkPrefix+"abc"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := CheckDir(base); err != nil {
		t.Errorf("ours: %v", err)
	}
	f := filepath.Join(base, "file")
	if err := os.WriteFile(f, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := CheckDir(f); err == nil {
		t.Error("a regular file was accepted as a directory")
	}
}

func TestParseSize(t *testing.T) {
	for in, want := range map[string]int64{"8Gi": 8 << 30, "512Mi": 512 << 20, "1Ti": 1 << 40, "4096": 4096, "2Ki": 2048} {
		got, err := ParseSize(in)
		if err != nil || got != want {
			t.Errorf("ParseSize(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "Gi", "-1Gi", "8GB", "0"} {
		if _, err := ParseSize(in); err == nil {
			t.Errorf("ParseSize(%q) accepted", in)
		}
	}
}

func sample(stack string, secs ...float64) Report {
	r := Report{Stack: stack, Rounds: 2, SizeBytes: 8 << 30}
	for i, s := range secs {
		r.Phases = append(r.Phases, PhaseResult{Name: string(rune('a' + i)), Seconds: []float64{s * 2, s}})
	}
	return r
}

func TestTableFormat(t *testing.T) {
	r := sample("loop", 1.5, 0.25)
	r.Notes = []string{"page cache dropped"}
	var b bytes.Buffer
	r.WriteTable(&b)
	want := `stack: loop   size: 8 GiB   rounds: 2
note: page cache dropped

phase  round 1  round 2
a        3.000    1.500
b        0.500    0.250
`
	if b.String() != want {
		t.Errorf("table:\n%s\nwant:\n%s", b.String(), want)
	}
}

func TestCompareRatios(t *testing.T) {
	nbd := sample("nbd", 3.0, 1.0, 0.001)
	loop := sample("loop", 2.0, 1.0, 0.002)
	c, err := Compare(nbd, loop)
	if err != nil {
		t.Fatal(err)
	}
	if got := c.Rows[0].Ratio; got != 1.5 || !c.Rows[0].Pass {
		t.Errorf("exactly 1.5x: ratio %v pass %v, want pass", got, c.Rows[0].Pass)
	}
	// Both under the floor: 0.001 and 0.002 both count as the floor, ratio 1.
	if got := c.Rows[2].Ratio; got != 1 {
		t.Errorf("floored ratio = %v, want 1", got)
	}
	if !c.Pass {
		t.Error("G1 failed on all-passing rows")
	}

	nbd.Phases[1].Seconds[1] = 1.51
	c, _ = Compare(nbd, loop)
	if c.Pass || c.Rows[1].Pass {
		t.Error("1.51x passed")
	}
	var b bytes.Buffer
	c.WriteTable(&b)
	if !strings.Contains(b.String(), "FAIL") || !strings.Contains(b.String(), "G1: FAIL") {
		t.Errorf("table lacks FAIL:\n%s", b.String())
	}
}

func TestCompareRejectsMismatch(t *testing.T) {
	if _, err := Compare(sample("nbd", 1), sample("loop", 1, 2)); err == nil {
		t.Error("different phase sets compared")
	}
	if _, err := Compare(sample("loop", 1), sample("loop", 1)); err == nil {
		t.Error("two loop reports compared")
	}
}

func TestReadReportsOrdersByStack(t *testing.T) {
	dir := t.TempDir()
	var paths []string
	for _, st := range []string{"loop", "nbd"} {
		p := filepath.Join(dir, st+".json")
		b, _ := json.Marshal(sample(st, 1))
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, p)
	}
	nbd, loop, err := ReadPair(paths[0], paths[1])
	if err != nil || nbd.Stack != "nbd" || loop.Stack != "loop" {
		t.Fatalf("ReadPair = %v %v %v", nbd.Stack, loop.Stack, err)
	}
}
