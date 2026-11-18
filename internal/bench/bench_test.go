package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
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

func TestMedian(t *testing.T) {
	for _, c := range []struct {
		in   []float64
		want float64
	}{
		{[]float64{3}, 3},
		{[]float64{5, 1, 3}, 3},
		{[]float64{4, 1, 3, 2}, 2.5},
	} {
		if got := median(c.in); got != c.want {
			t.Errorf("median(%v) = %v, want %v", c.in, got, c.want)
		}
	}
}

func rounds(stack string, secs ...float64) Report {
	return Report{Stack: stack, Rounds: len(secs), SizeBytes: 8 << 30,
		Phases: []PhaseResult{{Name: "a", Seconds: secs}}}
}

func TestCompareUsesMedianOfLaterRounds(t *testing.T) {
	// Round 1 is warm-up and ignored; rounds 2..5 give 1, 2, 3, 9 for nbd
	// (median 2.5) and 1 for loop.
	c, err := Compare(rounds("nbd", 100, 9, 1, 3, 2), rounds("loop", 100, 1, 1, 1, 1))
	if err != nil {
		t.Fatal(err)
	}
	r := c.Rows[0]
	if r.NBD != 2.5 || r.NBDMin != 1 || r.NBDMax != 9 || r.Loop != 1 || r.LoopMin != 1 || r.LoopMax != 1 {
		t.Errorf("row = %+v", r)
	}
	if r.Ratio != 2.5 || r.Pass || c.Pass {
		t.Errorf("ratio %v pass %v, want 2.5 fail", r.Ratio, r.Pass)
	}
	var b bytes.Buffer
	c.WriteTable(&b)
	for _, want := range []string{"median of rounds 2-5", "2.500", "1.000", "9.000", "FAIL"} {
		if !strings.Contains(b.String(), want) {
			t.Errorf("table lacks %q:\n%s", want, b.String())
		}
	}
}

func TestCompareFloorAppliesToMedians(t *testing.T) {
	c, err := Compare(rounds("nbd", 1, 0.001, 0.04, 0.03), rounds("loop", 1, 0.002, 0.001, 0.001))
	if err != nil {
		t.Fatal(err)
	}
	if c.Rows[0].Ratio != 1 {
		t.Errorf("ratio = %v, want 1 (both medians under the floor)", c.Rows[0].Ratio)
	}
}

func TestCompareRefusesRoundMismatch(t *testing.T) {
	if _, err := Compare(rounds("nbd", 1, 1, 1), rounds("loop", 1, 1)); err == nil || !strings.Contains(err.Error(), "rounds") {
		t.Errorf("different round counts: err = %v", err)
	}
	if _, err := Compare(rounds("nbd", 1), rounds("loop", 1)); err == nil || !strings.Contains(err.Error(), "at least 2") {
		t.Errorf("one round: err = %v", err)
	}
}

func TestCompareRefusesParameterMismatch(t *testing.T) {
	nbd, loop := sample("nbd", 1), sample("loop", 1)
	nbd.Params.Cipher, loop.Params.Cipher = "xchacha12,aes-adiantum-plain64", "aes-xts-plain64"
	if _, err := Compare(nbd, loop); err == nil {
		t.Error("reports encrypted with different ciphers compared")
	}
	loop.Params.Cipher = nbd.Params.Cipher
	if _, err := Compare(nbd, loop); err != nil {
		t.Errorf("same cipher: %v", err)
	}
}

// fakePublisher counts publishes and fails from the failAt-th on (0: never).
type fakePublisher struct {
	mu     sync.Mutex
	n      int
	failAt int
}

func (f *fakePublisher) publish(context.Context) (float64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n++
	if f.failAt > 0 && f.n >= f.failAt {
		return 0, errors.New("store away")
	}
	return 0.25, nil
}

func TestGenerationsRunEveryIntervalUntilStopped(t *testing.T) {
	p := &fakePublisher{}
	var rep Report
	stop := startGenerations(context.Background(), p, Options{GenerationInterval: 10 * time.Millisecond}, &rep)
	time.Sleep(100 * time.Millisecond)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	n := p.n
	p.mu.Unlock()
	if n < 3 || len(rep.GenerationSeconds) != n {
		t.Fatalf("%d publishes, %d recorded; want at least 3, all recorded", n, len(rep.GenerationSeconds))
	}
	time.Sleep(30 * time.Millisecond)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.n != n {
		t.Errorf("published %d more after stop", p.n-n)
	}
}

func TestGenerationsStopAtTheFirstError(t *testing.T) {
	p := &fakePublisher{failAt: 2}
	var rep Report
	stop := startGenerations(context.Background(), p, Options{GenerationInterval: 5 * time.Millisecond}, &rep)
	time.Sleep(60 * time.Millisecond)
	err := stop()
	if err == nil || !strings.Contains(err.Error(), "store away") {
		t.Fatalf("stop = %v, want the publish error", err)
	}
	if p.n != 2 || len(rep.GenerationSeconds) != 1 {
		t.Errorf("%d publishes, %d recorded; want 2 and 1", p.n, len(rep.GenerationSeconds))
	}
}

// A report with parameters shows its sweep point under the header line; one
// without (before the flags, or the none stack) prints as before.
func TestTableShowsTheParameters(t *testing.T) {
	r := sample("nbd", 1)
	r.Params = Params{ChunkBytes: 16 << 20, Cipher: "aes-xts-plain64", CacheBoundBytes: 2 << 30, GenerationIntervalSeconds: 90}
	var b bytes.Buffer
	r.WriteTable(&b)
	want := "chunk: 16 MiB   cipher: aes-xts-plain64   cache bound: 2 GiB   generations: 90s\n"
	if !strings.Contains(b.String(), want) {
		t.Errorf("table lacks %q:\n%s", want, b.String())
	}
	r.Params = Params{Cipher: "xchacha12,aes-adiantum-plain64"} // a loop report
	b.Reset()
	r.WriteTable(&b)
	if !strings.Contains(b.String(), "chunk: -   cipher: xchacha12,aes-adiantum-plain64   cache bound: -   generations: none during the rounds\n") {
		t.Errorf("loop table:\n%s", b.String())
	}
}
