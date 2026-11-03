package holder

import (
	"strings"
	"testing"
	"time"
)

func TestJudgementTable(t *testing.T) {
	t.Parallel()
	const T = 5 * time.Minute
	present := func(age time.Duration) Info { return Info{Present: true, Node: "kind-worker", Epoch: 1, Age: age} }
	tests := []struct {
		name     string
		info     Info
		node     NodeState
		observed time.Duration
		want     bool
		reason   string
	}{
		{"out of service wins over a fresh holder", present(time.Second), NodeState{OutOfService: true, Ready: true, InUse: true}, 0, true, "out-of-service"},
		{"ready, not in use, stale 30s", present(StaleAfter), NodeState{Ready: true}, 0, true, "Ready"},
		{"ready, not in use, just under 30s", present(StaleAfter - time.Second), NodeState{Ready: true}, 0, false, "under"},
		{"ready, in use, very old", present(time.Hour), NodeState{Ready: true, InUse: true}, time.Hour, false, "in use"},
		{"not ready, older than T, renewals unchanged for T", present(T), NodeState{}, T, true, "not Ready"},
		{"not ready, older than T, renewals changed recently", present(T + time.Minute), NodeState{}, time.Minute, false, "changed"},
		{"not ready, younger than T, renewals unchanged", present(T - time.Second), NodeState{}, time.Hour, false, "server's clock"},
		{"not ready, 30s stale is not enough", present(time.Minute), NodeState{}, time.Hour, false, "under"},
		{"no holder", Info{}, NodeState{OutOfService: true}, 0, false, "no holder"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := Judge(tc.info, tc.node, T, Observation{RenewalsUnchangedFor: tc.observed})
			if got.Takeover != tc.want {
				t.Errorf("Takeover = %v (%s), want %v", got.Takeover, got.Reason, tc.want)
			}
			if !strings.Contains(got.Reason, tc.reason) {
				t.Errorf("Reason = %q, want it to mention %q", got.Reason, tc.reason)
			}
		})
	}
}
