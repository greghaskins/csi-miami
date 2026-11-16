package main

import (
	"bytes"
	"strings"
	"testing"
)

// -keep below 1 is a usage error, checked before anything is opened.
func TestBlockdRefusesKeepBelowOne(t *testing.T) {
	base := []string{"-volume", "pvc-1", "-mount", "/m", "-store", "/m/s", "-cache-dir", "/c", "-control", "/c/s",
		"-size", "8388608", "-chunk-size", "4194304", "-cache-bound", "8388608", "-epoch", "1", "-node", "n"}
	for _, keep := range []string{"0", "-1"} {
		var o, e bytes.Buffer
		if code := runBlockd(append(append([]string{}, base...), "-keep", keep), &o, &e); code != 2 || !strings.Contains(e.String(), "-keep must be at least 1") {
			t.Errorf("-keep %s: exit %d, stderr %q; want 2 and the reason", keep, code, e.String())
		}
	}
}
