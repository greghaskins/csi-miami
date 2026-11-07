package bench

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// Gate G1 numbers: every phase of the nbd stack within Threshold times the
// loop stack's. A phase that takes less than FloorSeconds on both counts as
// FloorSeconds, so a stat sweep of a few tens of milliseconds cannot fail the
// gate on timer noise.
const (
	Threshold    = 1.5
	FloorSeconds = 0.05
)

// PhaseResult is one phase's seconds, one entry per round.
type PhaseResult struct {
	Name    string    `json:"name"`
	Seconds []float64 `json:"seconds"`
}

// Report is one stack's run.
type Report struct {
	Stack     string        `json:"stack"`
	SizeBytes int64         `json:"size_bytes"`
	Rounds    int           `json:"rounds"`
	Notes     []string      `json:"notes,omitempty"`
	Phases    []PhaseResult `json:"phases"`
	// PublishSeconds is how long the nbd stack took to publish a generation
	// after the rounds (freeze, boundary, thaw, publish). It is reported, not
	// gated; zero for the other stacks.
	PublishSeconds float64 `json:"publish_seconds,omitempty"`
}

// WriteJSON writes r as indented JSON.
func (r Report) WriteJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

// WriteTable writes the human table: a header, the notes, then one row per
// phase with a column per round.
func (r Report) WriteTable(w io.Writer) {
	_, _ = fmt.Fprintf(w, "stack: %s   size: %s   rounds: %d\n", r.Stack, humanSize(r.SizeBytes), r.Rounds)
	for _, n := range r.Notes {
		_, _ = fmt.Fprintf(w, "note: %s\n", n)
	}
	_, _ = fmt.Fprintln(w)
	width := len("phase")
	for _, p := range r.Phases {
		width = max(width, len(p.Name))
	}
	_, _ = fmt.Fprintf(w, "%-*s", width, "phase")
	for i := 1; i <= r.Rounds; i++ {
		_, _ = fmt.Fprintf(w, "  %7s", "round "+strconv.Itoa(i))
	}
	_, _ = fmt.Fprintln(w)
	for _, p := range r.Phases {
		_, _ = fmt.Fprintf(w, "%-*s", width, p.Name)
		for _, s := range p.Seconds {
			_, _ = fmt.Fprintf(w, "  %7.3f", s)
		}
		_, _ = fmt.Fprintln(w)
	}
	if r.PublishSeconds > 0 {
		_, _ = fmt.Fprintf(w, "\npublish one generation: %.3f s (not gated)\n", r.PublishSeconds)
	}
}

func humanSize(n int64) string {
	for _, u := range []struct {
		n    int64
		name string
	}{{1 << 40, "TiB"}, {1 << 30, "GiB"}, {1 << 20, "MiB"}, {1 << 10, "KiB"}} {
		if n >= u.n && n%u.n == 0 {
			return fmt.Sprintf("%d %s", n/u.n, u.name)
		}
	}
	return fmt.Sprintf("%d B", n)
}

// ParseSize reads a positive byte count with an optional binary suffix:
// 4096, 512Mi, 8Gi.
func ParseSize(s string) (int64, error) {
	mult := int64(1)
	num := s
	for suffix, m := range map[string]int64{"Ki": 1 << 10, "Mi": 1 << 20, "Gi": 1 << 30, "Ti": 1 << 40} {
		if strings.HasSuffix(s, suffix) {
			mult, num = m, strings.TrimSuffix(s, suffix)
			break
		}
	}
	n, err := strconv.ParseInt(num, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("bench: %q is not a positive size (examples: 4096, 512Mi, 8Gi)", s)
	}
	return n * mult, nil
}

// Row is one phase of a comparison, from the last round of each report.
type Row struct {
	Phase string  `json:"phase"`
	NBD   float64 `json:"nbd_seconds"`
	Loop  float64 `json:"loop_seconds"`
	Ratio float64 `json:"ratio"`
	Pass  bool    `json:"pass"`
}

// Comparison is gate G1 over two reports.
type Comparison struct {
	Rows []Row `json:"rows"`
	Pass bool  `json:"pass"`
}

// Compare divides the nbd report's last-round seconds by the loop report's,
// phase by phase.
func Compare(nbd, loop Report) (Comparison, error) {
	if nbd.Stack != "nbd" || loop.Stack != "loop" {
		return Comparison{}, fmt.Errorf("bench: comparing needs an nbd and a loop report, got %q and %q", nbd.Stack, loop.Stack)
	}
	if len(nbd.Phases) != len(loop.Phases) {
		return Comparison{}, fmt.Errorf("bench: the reports have %d and %d phases; they were run at different scales", len(nbd.Phases), len(loop.Phases))
	}
	c := Comparison{Pass: true}
	for i, n := range nbd.Phases {
		l := loop.Phases[i]
		if n.Name != l.Name {
			return Comparison{}, fmt.Errorf("bench: phase %d is %q in one report and %q in the other; they were run at different scales", i+1, n.Name, l.Name)
		}
		if len(n.Seconds) == 0 || len(l.Seconds) == 0 {
			return Comparison{}, fmt.Errorf("bench: phase %q has no timings", n.Name)
		}
		nv := max(n.Seconds[len(n.Seconds)-1], FloorSeconds)
		lv := max(l.Seconds[len(l.Seconds)-1], FloorSeconds)
		row := Row{Phase: n.Name, NBD: n.Seconds[len(n.Seconds)-1], Loop: l.Seconds[len(l.Seconds)-1], Ratio: nv / lv}
		row.Pass = row.Ratio <= Threshold
		c.Pass = c.Pass && row.Pass
		c.Rows = append(c.Rows, row)
	}
	return c, nil
}

// WriteJSON writes the comparison as indented JSON.
func WriteJSON(w io.Writer, c Comparison) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(c)
}

// WriteTable writes the per-phase ratios and the verdict.
func (c Comparison) WriteTable(w io.Writer) {
	width := len("phase")
	for _, r := range c.Rows {
		width = max(width, len(r.Phase))
	}
	_, _ = fmt.Fprintf(w, "round-2 seconds, nbd against loop (times under %.2f s count as %.2f s)\n\n", FloorSeconds, FloorSeconds)
	_, _ = fmt.Fprintf(w, "%-*s  %8s  %8s  %6s  %s\n", width, "phase", "nbd", "loop", "ratio", "limit 1.5x")
	for _, r := range c.Rows {
		v := "PASS"
		if !r.Pass {
			v = "FAIL"
		}
		_, _ = fmt.Fprintf(w, "%-*s  %8.3f  %8.3f  %6.2f  %s\n", width, r.Phase, r.NBD, r.Loop, r.Ratio, v)
	}
	v := "PASS"
	if !c.Pass {
		v = "FAIL"
	}
	_, _ = fmt.Fprintf(w, "\nG1: %s\n", v)
}

// ReadReport reads a report written with WriteJSON.
func ReadReport(path string) (Report, error) {
	b, err := os.ReadFile(path) //nolint:gosec // operator-supplied report
	if err != nil {
		return Report{}, err
	}
	var r Report
	if err := json.Unmarshal(b, &r); err != nil {
		return Report{}, fmt.Errorf("bench: %s: %w", path, err)
	}
	return r, nil
}

// ReadPair reads two reports in either order and returns the nbd one first.
func ReadPair(a, b string) (nbd, loop Report, err error) {
	ra, err := ReadReport(a)
	if err != nil {
		return nbd, loop, err
	}
	rb, err := ReadReport(b)
	if err != nil {
		return nbd, loop, err
	}
	if ra.Stack == "loop" {
		ra, rb = rb, ra
	}
	return ra, rb, nil
}
