package kube

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

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
