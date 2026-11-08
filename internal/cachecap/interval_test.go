package cachecap_test

import (
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/reconcile"
)

// The runner's default period is the reconciler's. This is an external test
// package so that the import cannot become a cycle.
func TestDefaultIntervalMatchesReconciler(t *testing.T) {
	t.Parallel()
	if reconcile.DefaultInterval != 30*time.Second {
		t.Errorf("reconcile.DefaultInterval = %v; update cachecap's defaultInterval to match", reconcile.DefaultInterval)
	}
}
