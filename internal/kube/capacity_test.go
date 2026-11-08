package kube

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/rest"

	"github.com/greghaskins/csi-miami/internal/cachecap"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// capacityCall is the one request a handler saw.
type capacityCall struct {
	method, path, contentType, body string
}

func recordingServer(t *testing.T, code int, reply any) (*Client, func() capacityCall) {
	t.Helper()
	var mu sync.Mutex
	var got capacityCall
	cfg := apiServer(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		got = capacityCall{r.Method, r.URL.Path, r.Header.Get("Content-Type"), string(b)}
		mu.Unlock()
		writeJSON(w, code, reply)
	})
	c, err := NewClient(cfg, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return c, func() capacityCall { mu.Lock(); defer mu.Unlock(); return got }
}

func TestSetCacheCapacityPatch(t *testing.T) {
	t.Parallel()
	c, call := recordingServer(t, 200, node("node-a", nil, "True"))
	if err := c.SetCacheCapacity(context.Background(), "node-a", 95<<30); err != nil {
		t.Fatal(err)
	}
	got := call()
	want := `[{"op":"add","path":"/status/capacity/miami.csi.greghaskins.com~1cache","value":"95Gi"}]`
	if got.method != http.MethodPatch || got.path != "/api/v1/nodes/node-a/status" {
		t.Errorf("request = %s %s, want PATCH on nodes/node-a/status", got.method, got.path)
	}
	if got.contentType != "application/json-patch+json" {
		t.Errorf("content type = %q", got.contentType)
	}
	if got.body != want {
		t.Errorf("body = %s, want %s", got.body, want)
	}
}

func TestSetCacheCapacityQuantities(t *testing.T) {
	t.Parallel()
	for bytes, want := range map[int64]string{0: `"0"`, 1 << 20: `"1Mi"`, 30 << 30: `"30Gi"`, 30<<30 + 1<<20: `"30721Mi"`} {
		c, call := recordingServer(t, 200, node("n", nil, "True"))
		if err := c.SetCacheCapacity(context.Background(), "n", bytes); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(call().body, `"value":`+want) {
			t.Errorf("%d bytes: body %s, want value %s", bytes, call().body, want)
		}
	}
}

func TestRemoveCacheCapacityPatch(t *testing.T) {
	t.Parallel()
	c, call := recordingServer(t, 200, node("node-a", nil, "True"))
	if err := c.RemoveCacheCapacity(context.Background(), "node-a"); err != nil {
		t.Fatal(err)
	}
	got := call()
	want := `[{"op":"remove","path":"/status/capacity/miami.csi.greghaskins.com~1cache"}]`
	if got.method != http.MethodPatch || got.path != "/api/v1/nodes/node-a/status" || got.body != want {
		t.Errorf("request = %s %s %s", got.method, got.path, got.body)
	}
}

func TestCacheCapacityPatchErrorsCarryNoURL(t *testing.T) {
	t.Parallel()
	c, _ := recordingServer(t, 403, map[string]any{
		"kind": "Status", "apiVersion": "v1", "status": "Failure", "code": 403, "reason": "Forbidden",
	})
	for name, err := range map[string]error{
		"set":    c.SetCacheCapacity(context.Background(), "node-a", 1<<20),
		"remove": c.RemoveCacheCapacity(context.Background(), "node-a"),
	} {
		if err == nil {
			t.Errorf("%s: no error", name)
			continue
		}
		if strings.Contains(err.Error(), "http") || strings.Contains(err.Error(), "127.0.0.1") {
			t.Errorf("%s: error leaks the address: %v", name, err)
		}
		if !apierrors.IsForbidden(err) {
			t.Errorf("%s: the denial is not visible to errors.As: %v", name, err)
		}
		if !strings.Contains(err.Error(), "node-a") || !strings.Contains(err.Error(), "Forbidden") {
			t.Errorf("%s: error = %v", name, err)
		}
	}
	if err := c.SetCacheCapacity(context.Background(), "", 1); err == nil {
		t.Error("an empty node name was accepted")
	}
}

func TestCacheCapacityReads(t *testing.T) {
	t.Parallel()
	withCap := func(v any) map[string]any {
		n := node("n", nil, "True")
		n["status"].(map[string]any)["capacity"] = map[string]any{"cpu": "4", volume.CacheResource: v}
		return n
	}
	noKey := node("n", nil, "True")
	noKey["status"].(map[string]any)["capacity"] = map[string]any{"cpu": "4"}
	noCap := node("n", nil, "True")
	tests := []struct {
		name    string
		code    int
		reply   any
		want    int64
		present bool
		wantErr bool
	}{
		{"binary", 200, withCap("30Gi"), 30 << 30, true, false},
		{"plain bytes", 200, withCap("1048576"), 1 << 20, true, false},
		{"zero", 200, withCap("0"), 0, true, false},
		{"missing key", 200, noKey, 0, false, false},
		{"no capacity map", 200, noCap, 0, false, false},
		{"not a quantity", 200, withCap("lots"), 0, false, true},
		{"fractional", 200, withCap("1500m"), 0, false, true},
		{"a number, not a string", 200, withCap(map[string]any{"a": 1}), 0, false, true},
		{"server error", 500, map[string]any{"kind": "Status", "apiVersion": "v1", "status": "Failure", "code": 500, "reason": "InternalError"}, 0, false, true},
		{"node not found", 404, map[string]any{"kind": "Status", "apiVersion": "v1", "status": "Failure", "code": 404, "reason": "NotFound", "details": map[string]any{"name": "n", "kind": "nodes"}}, 0, false, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, _ := recordingServer(t, tc.code, tc.reply)
			got, present, err := c.CacheCapacity(context.Background(), "n")
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, want error %v", err, tc.wantErr)
			}
			if err != nil {
				if present {
					t.Error("present is true beside an error")
				}
				if strings.Contains(err.Error(), "127.0.0.1") {
					t.Errorf("error leaks the address: %v", err)
				}
				return
			}
			if got != tc.want || present != tc.present {
				t.Errorf("got (%d, %v), want (%d, %v)", got, present, tc.want, tc.present)
			}
		})
	}
}

func TestCacheCapacityDeadline(t *testing.T) {
	t.Parallel()
	block := make(chan struct{})
	cfg := apiServer(t, func(http.ResponseWriter, *http.Request) { <-block })
	t.Cleanup(func() { close(block) })
	c, err := NewClient(cfg, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.SetCacheCapacity(context.Background(), "n", 1<<20); err == nil || errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want a timeout", err)
	}
}

func statusBody(code int, reason, msg string) map[string]any {
	return map[string]any{"kind": "Status", "apiVersion": "v1", "status": "Failure", "code": code, "reason": reason, "message": msg}
}

// probeServer answers the n-th request with replies[n] and records the bodies.
func probeServer(t *testing.T, replies []func(w http.ResponseWriter)) (*Client, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var bodies []string
	cfg := apiServer(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		i := len(bodies)
		bodies = append(bodies, string(b))
		mu.Unlock()
		if i >= len(replies) {
			writeJSON(w, 500, statusBody(500, "InternalError", "unexpected request"))
			return
		}
		replies[i](w)
	})
	c, err := NewClient(cfg, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return c, func() []string { mu.Lock(); defer mu.Unlock(); return append([]string(nil), bodies...) }
}

func TestProbeFence(t *testing.T) {
	t.Parallel()
	ok := func(w http.ResponseWriter) { writeJSON(w, 200, node("n", nil, "True")) }
	fail := func(code int, reason, msg string) func(http.ResponseWriter) {
		return func(w http.ResponseWriter) { writeJSON(w, code, statusBody(code, reason, msg)) }
	}
	const denial = `nodes "n" is forbidden: ValidatingAdmissionPolicy 'miami-node-capacity' with binding 'miami-node-capacity' denied request: [capacity] the node plugin may change only the cache key`
	probeAdd := `[{"op":"add","path":"/status/capacity/miami.csi.greghaskins.com~1fence-probe","value":"1"}]`
	probeRemove := `[{"op":"remove","path":"/status/capacity/miami.csi.greghaskins.com~1fence-probe"}]`
	tests := []struct {
		name    string
		replies []func(http.ResponseWriter)
		want    cachecap.FenceState
		wantErr bool
		bodies  []string
	}{
		{"denied by the policy (403)", []func(http.ResponseWriter){fail(403, "Forbidden", denial)}, cachecap.Fenced, false, []string{probeAdd}},
		{"a 422 is not this policy's answer", []func(http.ResponseWriter){fail(422, "Invalid", denial)}, cachecap.FenceUnknown, true, []string{probeAdd}},
		{"a webhook quoting the policy's text", []func(http.ResponseWriter){fail(403, "Forbidden", `admission webhook "w.example.com" denied the request: `+denial)}, cachecap.FenceUnknown, true, []string{probeAdd}},
		{"another rule of the policy (not [capacity])", []func(http.ResponseWriter){fail(403, "Forbidden", `ValidatingAdmissionPolicy 'miami-node-capacity' with binding 'miami-node-capacity' denied request: [node-name] no`)}, cachecap.FenceUnknown, true, []string{probeAdd}},
		{"a message that merely contains the name", []func(http.ResponseWriter){fail(403, "Forbidden", `webhook "miami-node-capacity.example.com" denied the request: [capacity]`)}, cachecap.FenceUnknown, true, []string{probeAdd}},
		{"a renamed policy", []func(http.ResponseWriter){fail(403, "Forbidden", `ValidatingAdmissionPolicy 'prod-miami-node-capacity' with binding 'prod-miami-node-capacity' denied request: [capacity] no`)}, cachecap.FenceUnknown, true, []string{probeAdd}},
		{"a 500 carrying the policy's text", []func(http.ResponseWriter){fail(500, "InternalError", denial)}, cachecap.FenceUnknown, true, []string{probeAdd}},
		{"denied by RBAC, not the policy", []func(http.ResponseWriter){fail(403, "Forbidden", `User "x" cannot patch resource "nodes/status"`)}, cachecap.FenceUnknown, true, []string{probeAdd}},
		{"server error", []func(http.ResponseWriter){fail(500, "InternalError", "boom")}, cachecap.FenceUnknown, true, []string{probeAdd}},
		{"allowed, then removed", []func(http.ResponseWriter){ok, ok}, cachecap.Unfenced, false, []string{probeAdd, probeRemove}},
		{"allowed, removal fails", []func(http.ResponseWriter){ok, fail(500, "InternalError", "boom")}, cachecap.Unfenced, true, []string{probeAdd, probeRemove}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, bodies := probeServer(t, tc.replies)
			got, err := c.ProbeFence(context.Background(), "n")
			if got != tc.want || (err != nil) != tc.wantErr {
				t.Errorf("got (%v, %v), want (%v, error %v)", got, err, tc.want, tc.wantErr)
			}
			if b := bodies(); !slices.Equal(b, tc.bodies) {
				t.Errorf("requests = %v, want %v", b, tc.bodies)
			}
			if err != nil && strings.Contains(err.Error(), "127.0.0.1") {
				t.Errorf("error leaks the address: %v", err)
			}
		})
	}
}

func TestClientSubject(t *testing.T) {
	t.Parallel()
	jwt := "e30." + base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"system:serviceaccount:storage:prod-miami-node"}`)) + ".sig"
	c, err := NewClient(&rest.Config{Host: "http://127.0.0.1:1", BearerToken: jwt}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got := c.Subject(); got != "system:serviceaccount:storage:prod-miami-node" {
		t.Errorf("Subject = %q", got)
	}
	c, _ = NewClient(&rest.Config{Host: "http://127.0.0.1:1"}, time.Second)
	if got := c.Subject(); got != "unknown" {
		t.Errorf("Subject without a token = %q", got)
	}
}

// TestPluginPatchesTouchOnlyItsTwoKeys pins the invariant that keeps the
// policy's hold on other managers' managedFields entries safe: the plugin's
// patches (set, remove, probe and its cleanup) touch exactly two leaf keys
// under status.capacity, with add and remove only, and name the field manager.
func TestPluginPatchesTouchOnlyItsTwoKeys(t *testing.T) {
	t.Parallel()
	type req struct{ body, manager string }
	var mu sync.Mutex
	var reqs []req
	cfg := apiServer(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if r.Method == http.MethodPatch {
			mu.Lock()
			reqs = append(reqs, req{string(b), r.URL.Query().Get("fieldManager")})
			mu.Unlock()
		}
		n := node("n", nil, "True")
		n["status"].(map[string]any)["capacity"] = map[string]any{"miami.csi.greghaskins.com/fence-probe": "1"}
		writeJSON(w, 200, n)
	})
	c, err := NewClient(cfg, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := c.SetCacheCapacity(ctx, "n", 1<<30); err != nil {
		t.Fatal(err)
	}
	if err := c.RemoveCacheCapacity(ctx, "n"); err != nil {
		t.Fatal(err)
	}
	if st, err := c.ProbeFence(ctx, "n"); st != cachecap.Unfenced || err != nil {
		t.Fatalf("probe = %v, %v", st, err)
	}
	if err := c.RemoveFenceProbe(ctx, "n"); err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{
		"/status/capacity/miami.csi.greghaskins.com~1cache":       true,
		"/status/capacity/miami.csi.greghaskins.com~1fence-probe": true,
	}
	seen := map[string]bool{}
	if len(reqs) != 5 {
		t.Fatalf("%d patches, want 5 (set, remove, probe add and remove, cleanup)", len(reqs))
	}
	for _, rq := range reqs {
		var ops []patchOp
		if err := json.Unmarshal([]byte(rq.body), &ops); err != nil {
			t.Fatal(err)
		}
		if len(ops) != 1 || (ops[0].Op != "add" && ops[0].Op != "remove") || !allowed[ops[0].Path] {
			t.Errorf("a patch strays from the two keys: %s", rq.body)
		}
		seen[ops[0].Path] = true
		if rq.manager != "miami-node" {
			t.Errorf("field manager = %q, want miami-node (%s)", rq.manager, rq.body)
		}
	}
	if len(seen) != 2 {
		t.Errorf("paths touched = %v, want both keys", seen)
	}
}

func TestRemoveFenceProbeOnlyWhenPresent(t *testing.T) {
	t.Parallel()
	var patches int
	cfg := apiServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPatch {
			patches++
		}
		writeJSON(w, 200, node("n", nil, "True"))
	})
	c, _ := NewClient(cfg, 5*time.Second)
	if err := c.RemoveFenceProbe(context.Background(), "n"); err != nil || patches != 0 {
		t.Errorf("err %v, patches %d; want none for a node without the key", err, patches)
	}
}
