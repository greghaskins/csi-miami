package kube

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/client-go/rest"
)

const (
	pvName   = "pvc-00000000-0000-4000-8000-000000000001"
	volumeID = "directory:" + pvName
)

// eventsAPI is an API server that has PVs and records events.
type eventsAPI struct {
	mu      sync.Mutex
	pvs     map[string]string // name → uid
	events  map[string]map[string]any
	posts   int
	failOn  string        // "create" makes POST answer 500
	created chan struct{} // a token for each event stored
}

func newEventsAPI() *eventsAPI {
	return &eventsAPI{pvs: map[string]string{pvName: "uid-1"}, events: map[string]map[string]any{}, created: make(chan struct{}, 16)}
}

func (a *eventsAPI) config(t *testing.T) *rest.Config {
	return apiServer(t, func(w http.ResponseWriter, r *http.Request) {
		a.mu.Lock()
		defer a.mu.Unlock()
		const evPath = "/api/v1/namespaces/default/events"
		switch {
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v1/persistentvolumes/"):
			name := strings.TrimPrefix(r.URL.Path, "/api/v1/persistentvolumes/")
			uid, ok := a.pvs[name]
			if !ok {
				writeJSON(w, 404, metav1Status(404, "NotFound", "persistentvolumes", name))
				return
			}
			writeJSON(w, 200, map[string]any{"apiVersion": "v1", "kind": "PersistentVolume", "metadata": map[string]any{"name": name, "uid": uid}})
		case r.Method == http.MethodPost && r.URL.Path == evPath:
			a.posts++
			if a.failOn == "create" {
				writeJSON(w, 500, metav1Status(500, "InternalError", "events", ""))
				return
			}
			var ev map[string]any
			_ = json.NewDecoder(r.Body).Decode(&ev)
			name := ev["metadata"].(map[string]any)["name"].(string)
			if _, dup := a.events[name]; dup {
				writeJSON(w, 409, metav1Status(409, "AlreadyExists", "events", name))
				return
			}
			a.events[name] = ev
			a.created <- struct{}{}
			writeJSON(w, 201, ev)
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, evPath+"/"):
			ev, ok := a.events[strings.TrimPrefix(r.URL.Path, evPath+"/")]
			if !ok {
				writeJSON(w, 404, metav1Status(404, "NotFound", "events", ""))
				return
			}
			writeJSON(w, 200, ev)
		case r.Method == http.MethodPatch && strings.HasPrefix(r.URL.Path, evPath+"/"):
			ev, ok := a.events[strings.TrimPrefix(r.URL.Path, evPath+"/")]
			if !ok {
				writeJSON(w, 404, metav1Status(404, "NotFound", "events", ""))
				return
			}
			body, _ := io.ReadAll(r.Body)
			var patch map[string]any
			_ = json.Unmarshal(body, &patch)
			for k, v := range patch {
				ev[k] = v
			}
			writeJSON(w, 200, ev)
		default:
			http.NotFound(w, r)
		}
	})
}

func metav1Status(code int, reason, kind, name string) map[string]any {
	return map[string]any{
		"apiVersion": "v1", "kind": "Status", "status": "Failure", "code": code, "reason": reason,
		"details": map[string]any{"kind": kind, "name": name},
	}
}

func newRecorder(t *testing.T, a *eventsAPI, buffer int) *EventRecorder {
	t.Helper()
	r, err := NewEventRecorder(a.config(t), RecorderOptions{
		Component: "test.example.com", Node: "node-a", Deadline: 2 * time.Second, Buffer: buffer,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now: func() time.Time { return time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// drain sends everything queued, in the caller's goroutine, then returns.
func drain(r *EventRecorder) {
	for len(r.queue) > 0 {
		r.send(context.Background(), <-r.queue)
	}
}

// Run sends events and stops with its context.
func TestRunSendsUntilItsContextEnds(t *testing.T) {
	t.Parallel()
	a := newEventsAPI()
	r := newRecorder(t, a, 0)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	r.Event(volumeID, "MountFailed", "x")
	select {
	case <-a.created:
	case <-time.After(10 * time.Second):
		t.Fatal("the event was not sent")
	}
	cancel()
	<-done
}

func TestEventIsCreatedOnThePV(t *testing.T) {
	t.Parallel()
	a := newEventsAPI()
	r := newRecorder(t, a, 0)
	r.Event(volumeID, "MountFailed", "the stage path is mounted from something else")
	r.Event(volumeID, "GarbageUnmounted", "unmounted a stray mount")
	drain(r)

	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.events) != 2 {
		t.Fatalf("%d events, want 2", len(a.events))
	}
	for name, ev := range a.events {
		if !strings.HasPrefix(name, pvName+".") {
			t.Errorf("event name %q", name)
		}
		obj := ev["involvedObject"].(map[string]any)
		if obj["kind"] != "PersistentVolume" || obj["name"] != pvName || obj["uid"] != "uid-1" || obj["apiVersion"] != "v1" {
			t.Errorf("involvedObject = %v", obj)
		}
		src := ev["source"].(map[string]any)
		if src["component"] != "test.example.com" || src["host"] != "node-a" {
			t.Errorf("source = %v", src)
		}
		switch ev["reason"] {
		case "MountFailed":
			if ev["type"] != "Warning" {
				t.Errorf("MountFailed type = %v", ev["type"])
			}
		case "GarbageUnmounted":
			if ev["type"] != "Normal" {
				t.Errorf("GarbageUnmounted type = %v", ev["type"])
			}
		default:
			t.Errorf("reason %v", ev["reason"])
		}
	}
}

func TestARepeatedEventCountsInsteadOfDuplicating(t *testing.T) {
	t.Parallel()
	a := newEventsAPI()
	r := newRecorder(t, a, 0)
	for range 3 {
		r.Event(volumeID, "TransportUnavailable", "the server did not answer")
	}
	drain(r)
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.events) != 1 {
		t.Fatalf("%d events, want 1", len(a.events))
	}
	for _, ev := range a.events {
		if c, _ := ev["count"].(float64); c != 3 {
			t.Errorf("count = %v, want 3", ev["count"])
		}
	}
}

// A PV deleted and made again under the same name has another UID: its event
// is a new one, not a bump of the old PV's.
func TestARecreatedPVGetsItsOwnEvent(t *testing.T) {
	t.Parallel()
	a := newEventsAPI()
	r := newRecorder(t, a, 0)
	r.Event(volumeID, "MountFailed", "the same message")
	drain(r)
	a.mu.Lock()
	a.pvs[pvName] = "uid-2"
	a.mu.Unlock()
	r.Event(volumeID, "MountFailed", "the same message")
	drain(r)
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.events) != 2 {
		t.Fatalf("%d events, want 2", len(a.events))
	}
	uids := map[any]float64{}
	for _, ev := range a.events {
		c, _ := ev["count"].(float64)
		uids[ev["involvedObject"].(map[string]any)["uid"]] = c
	}
	if uids["uid-1"] != 1 || uids["uid-2"] != 1 {
		t.Errorf("counts by UID: %v, want 1 each", uids)
	}
}

func TestEventsThatCannotBeRecordedAreDropped(t *testing.T) {
	t.Parallel()
	t.Run("no volume, or not one of ours", func(t *testing.T) {
		t.Parallel()
		a := newEventsAPI()
		r := newRecorder(t, a, 0)
		r.Event("", "MountFailed", "x")
		r.Event("not-an-id", "MountFailed", "x")
		if len(r.queue) != 0 {
			t.Errorf("%d events were queued", len(r.queue))
		}
	})
	t.Run("the PV does not exist", func(t *testing.T) {
		t.Parallel()
		a := newEventsAPI()
		delete(a.pvs, pvName)
		r := newRecorder(t, a, 0)
		r.Event(volumeID, "MountFailed", "x")
		drain(r)
		if a.posts != 0 {
			t.Errorf("%d events were posted", a.posts)
		}
	})
	t.Run("the API server refuses", func(t *testing.T) {
		t.Parallel()
		a := newEventsAPI()
		a.failOn = "create"
		r := newRecorder(t, a, 0)
		r.Event(volumeID, "MountFailed", "x")
		drain(r) // must return, not retry forever
		if a.posts != 1 {
			t.Errorf("%d posts, want 1", a.posts)
		}
	})
	t.Run("the queue is full", func(t *testing.T) {
		t.Parallel()
		a := newEventsAPI()
		r := newRecorder(t, a, 2)
		for i := range 5 {
			r.Event(volumeID, "MountFailed", strings.Repeat("x", i+1))
		}
		if len(r.queue) != 2 {
			t.Errorf("%d events queued, want 2 (the rest dropped)", len(r.queue))
		}
	})
}

func TestTruncateKeepsValidUTF8(t *testing.T) {
	t.Parallel()
	if got := truncate("short", 10); got != "short" {
		t.Errorf("got %q", got)
	}
	got := truncate(strings.Repeat("é", 10), 5) // 2 bytes each
	if len(got) > 5 || got != "éé" {
		t.Errorf("got %q", got)
	}
}

func TestNewEventRecorderNeedsAConfig(t *testing.T) {
	t.Parallel()
	if _, err := NewEventRecorder(nil, RecorderOptions{}); err == nil {
		t.Error("no error for a nil config")
	}
}
