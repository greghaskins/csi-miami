//go:build e2e

package e2e

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// metricSample is one line of the Prometheus text format.
type metricSample struct {
	Name   string
	Labels map[string]string
	Value  float64
}

// metricSet is a parsed /metrics body: the families that announced themselves
// with a # TYPE line, and every sample.
type metricSet struct {
	Families map[string]bool
	Samples  []metricSample
}

var labelPair = regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"`)

// parseMetrics reads the text exposition format. It understands what the
// driver serves (no timestamps, no exemplars); a line it cannot read is
// ignored, and the assertions then fail for the missing sample.
func parseMetrics(body string) metricSet {
	ms := metricSet{Families: map[string]bool{}}
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case line == "":
		case strings.HasPrefix(line, "# TYPE "):
			if f := strings.Fields(line); len(f) >= 3 {
				ms.Families[f[2]] = true
			}
		case strings.HasPrefix(line, "#"):
		default:
			name, rest := line, ""
			labels := map[string]string{}
			if i := strings.IndexByte(line, '{'); i >= 0 {
				j := strings.LastIndexByte(line, '}')
				if j < i {
					continue
				}
				name, rest = line[:i], strings.TrimSpace(line[j+1:])
				for _, m := range labelPair.FindAllStringSubmatch(line[i+1:j], -1) {
					labels[m[1]] = m[2]
				}
			} else if i := strings.IndexByte(line, ' '); i >= 0 {
				name, rest = line[:i], strings.TrimSpace(line[i+1:])
			}
			v, err := strconv.ParseFloat(strings.Fields(rest + " ")[0], 64)
			if err != nil {
				continue
			}
			ms.Samples = append(ms.Samples, metricSample{Name: name, Labels: labels, Value: v})
		}
	}
	return ms
}

// value returns the value of the sample with exactly these labels.
func (ms metricSet) value(name string, labels map[string]string) (float64, bool) {
	for _, s := range ms.Samples {
		if s.Name != name || len(s.Labels) != len(labels) {
			continue
		}
		match := true
		for k, v := range labels {
			if s.Labels[k] != v {
				match = false
			}
		}
		if match {
			return s.Value, true
		}
	}
	return 0, false
}

// requests returns miami_csi_requests_total for a method and code.
func (ms metricSet) requests(name, method, code string) float64 {
	v, _ := ms.value(name, map[string]string{"method": method, "code": code})
	return v
}

// missingFamilies returns the wanted families the set does not have.
func (ms metricSet) missingFamilies(want []string) []string {
	var out []string
	for _, n := range want {
		if !ms.Families[n] {
			out = append(out, n)
		}
	}
	return out
}

func (ms metricSet) String() string {
	return fmt.Sprintf("%d families, %d samples", len(ms.Families), len(ms.Samples))
}
