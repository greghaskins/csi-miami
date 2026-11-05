package kube

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/reconcile"
)

func node(name string, taints []any, ready string) map[string]any {
	spec := map[string]any{}
	if taints != nil {
		spec["taints"] = taints
	}
	return map[string]any{
		"apiVersion": "v1", "kind": "Node",
		"metadata": map[string]any{"name": name},
		"spec":     spec,
		"status": map[string]any{"conditions": []any{
			map[string]any{"type": "MemoryPressure", "status": "False"},
			map[string]any{"type": "Ready", "status": ready},
		}},
	}
}

func TestNodeStatus(t *testing.T) {
	t.Parallel()
	oos := func(effect string) []any {
		return []any{
			map[string]any{"key": "other", "effect": "NoSchedule"},
			map[string]any{"key": OutOfServiceTaint, "value": "nodeshutdown", "effect": effect},
		}
	}
	cfg := apiServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/nodes/plain":
			writeJSON(w, 200, node("plain", nil, "True"))
		case "/api/v1/nodes/notready":
			writeJSON(w, 200, node("notready", []any{map[string]any{"key": "node.kubernetes.io/unreachable", "effect": "NoExecute"}}, "Unknown"))
		case "/api/v1/nodes/oos-noexecute":
			writeJSON(w, 200, node("oos-noexecute", oos("NoExecute"), "False"))
		case "/api/v1/nodes/oos-noschedule":
			writeJSON(w, 200, node("oos-noschedule", oos("NoSchedule"), "True"))
		case "/api/v1/nodes/gone":
			writeJSON(w, 404, map[string]any{
				"kind": "Status", "apiVersion": "v1", "status": "Failure", "code": 404, "reason": "NotFound",
				"details": map[string]any{"name": "gone", "kind": "nodes"},
			})
		case "/api/v1/nodes/denied":
			writeJSON(w, 403, map[string]any{"kind": "Status", "apiVersion": "v1", "status": "Failure", "code": 403, "reason": "Forbidden"})
		case "/api/v1/nodes/badtaints":
			n := node("badtaints", nil, "True")
			n["spec"] = map[string]any{"taints": "oops"}
			writeJSON(w, 200, n)
		default:
			http.NotFound(w, r)
		}
	})
	c, err := NewClient(cfg, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, tc := range []struct {
		name         string
		ready, oos   bool
		wantErr      bool
		wantNotFound bool
	}{
		{name: "plain", ready: true},
		{name: "notready"},
		{name: "oos-noexecute", oos: true},
		{name: "oos-noschedule", ready: true, oos: true},
		{name: "gone", wantErr: true, wantNotFound: true},
		{name: "denied", wantErr: true},
		{name: "badtaints", wantErr: true},
	} {
		got, err := c.NodeStatus(ctx, tc.name)
		if (err != nil) != tc.wantErr {
			t.Errorf("%s: err = %v", tc.name, err)
			continue
		}
		if errors.Is(err, ErrNodeNotFound) != tc.wantNotFound {
			t.Errorf("%s: ErrNodeNotFound = %v", tc.name, errors.Is(err, ErrNodeNotFound))
		}
		if err == nil && (got.Ready != tc.ready || got.OutOfService != tc.oos) {
			t.Errorf("%s: %+v, want ready %t out-of-service %t", tc.name, got, tc.ready, tc.oos)
		}
	}
	if _, err := c.NodeStatus(ctx, ""); err == nil {
		t.Error("an empty node name was accepted")
	}
}

var _ reconcile.Cluster = (*Client)(nil)

// VolumeExists backs the volume item's proof of deletion: false only on an
// explicit NotFound. A PV of another driver, with another handle, is a PV
// under that name: present.
func TestPVExistsIsFalseOnlyOnNotFound(t *testing.T) {
	t.Parallel()
	pv := func(name, driver, handle string) map[string]any {
		return map[string]any{
			"apiVersion": "v1", "kind": "PersistentVolume",
			"metadata": map[string]any{"name": name},
			"spec":     map[string]any{"csi": map[string]any{"driver": driver, "volumeHandle": handle}},
		}
	}
	cfg := apiServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/persistentvolumes/ours":
			writeJSON(w, 200, pv("ours", "d.example.com", "volume:ours"))
		case "/api/v1/persistentvolumes/foreign":
			writeJSON(w, 200, pv("foreign", "other.example.com", "something-else"))
		case "/api/v1/persistentvolumes/gone":
			writeJSON(w, 404, map[string]any{
				"kind": "Status", "apiVersion": "v1", "status": "Failure", "code": 404, "reason": "NotFound",
				"details": map[string]any{"name": "gone", "kind": "persistentvolumes"},
			})
		case "/api/v1/persistentvolumes/odd404":
			http.NotFound(w, r) // not the API server's own answer
		default:
			writeJSON(w, 503, map[string]any{"kind": "Status", "apiVersion": "v1", "status": "Failure", "code": 503})
		}
	})
	c, err := NewClient(cfg, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		exists  bool
		wantErr bool
	}{
		{"ours", true, false}, {"foreign", true, false}, {"gone", false, false},
		{"odd404", false, true}, {"broken", false, true},
	} {
		got, err := c.PVExists(context.Background(), tc.name)
		if got != tc.exists || (err != nil) != tc.wantErr {
			t.Errorf("%s: %t, %v; want %t, error %t", tc.name, got, err, tc.exists, tc.wantErr)
		}
	}
}
