package kube

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"github.com/greghaskins/csi-miami/internal/reconcile"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// Event reasons that are Warning events; every other reason is Normal.
var warningReasons = map[string]bool{
	reconcile.ReasonMountFailed:          true,
	reconcile.ReasonTransportUnavailable: true,
}

var eventResource = schema.GroupVersionResource{Version: "v1", Resource: "events"}

const (
	// eventNamespace is where the events of a cluster-scoped object go.
	eventNamespace = "default"

	// maxEventMessage is the longest message the API server accepts.
	maxEventMessage = 1024

	// DefaultEventBuffer is the number of events RecorderOptions.Buffer allows
	// to wait for the API server when it is not positive.
	DefaultEventBuffer = 64
)

// RecorderOptions configure an EventRecorder.
type RecorderOptions struct {
	// Component is the event source, for example the driver name.
	Component string
	// Node is the node name, recorded as the event's host.
	Node string
	// Deadline bounds each API request. Zero or negative means DefaultDeadline.
	Deadline time.Duration
	// Buffer is how many events may wait for the API server; beyond it events
	// are dropped. Zero or negative means DefaultEventBuffer.
	Buffer int
	// Log receives the recorder's messages. Nil means slog.Default().
	Log *slog.Logger
	// Now is the clock. Nil means time.Now.
	Now func() time.Time
}

// EventRecorder implements reconcile.Events by creating Kubernetes events on
// the PersistentVolume that a volume ID names, which is where `kubectl
// describe pv` looks. It is deliberately small: the reconciler already
// reports a condition once per change, so there is no aggregator or spam
// filter, only a deterministic event name per (PV, reason, message) whose
// repeats update the count instead of creating a second event.
//
// Event only queues; the API server is spoken to by Run, in its own
// goroutine, so the converge loop never waits for it. When the queue is full
// the event is dropped and logged: an event is a courtesy, never state.
// Failures are logged and dropped, never retried.
type EventRecorder struct {
	events dynamic.ResourceInterface
	pvs    dynamic.NamespaceableResourceInterface
	opts   RecorderOptions
	log    *slog.Logger
	now    func() time.Time
	queue  chan queued
}

var _ reconcile.Events = (*EventRecorder)(nil)

type queued struct{ pv, reason, message string }

// NewEventRecorder returns an EventRecorder for the API server at cfg. cfg is
// not modified.
func NewEventRecorder(cfg *rest.Config, o RecorderOptions) (*EventRecorder, error) {
	if cfg == nil {
		return nil, errors.New("kube: a rest config is required")
	}
	c := rest.CopyConfig(cfg)
	if c.QPS == 0 {
		c.QPS = clientQPS
	}
	if c.Burst == 0 {
		c.Burst = clientBurst
	}
	if c.UserAgent == "" {
		c.UserAgent = "csi-miami-node"
	}
	dyn, err := dynamic.NewForConfig(c)
	if err != nil {
		return nil, fmt.Errorf("kube: %w", err)
	}
	return NewEventRecorderFor(dyn, o), nil
}

// NewEventRecorderFor returns an EventRecorder over an existing dynamic
// client, for tests.
func NewEventRecorderFor(dyn dynamic.Interface, o RecorderOptions) *EventRecorder {
	if o.Deadline <= 0 {
		o.Deadline = DefaultDeadline
	}
	if o.Buffer <= 0 {
		o.Buffer = DefaultEventBuffer
	}
	r := &EventRecorder{
		events: dyn.Resource(eventResource).Namespace(eventNamespace),
		pvs:    dyn.Resource(pvResource),
		opts:   o,
		log:    o.Log,
		now:    o.Now,
		queue:  make(chan queued, o.Buffer),
	}
	if r.log == nil {
		r.log = slog.Default()
	}
	if r.now == nil {
		r.now = time.Now
	}
	return r
}

// Event implements reconcile.Events. An event for no volume (a stray mount) or
// for a volume ID this driver could not have issued has no PV to attach to
// and is dropped. It never blocks.
func (r *EventRecorder) Event(volumeID, reason, message string) {
	id, err := volume.ParseID(volumeID)
	if err != nil {
		return
	}
	select {
	case r.queue <- queued{pv: id.Name, reason: reason, message: message}:
	default:
		r.log.Warn("event dropped: the queue is full", "reason", reason)
	}
}

// Run sends the queued events until ctx ends. Events still queued then are
// dropped.
func (r *EventRecorder) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case q := <-r.queue:
			r.send(ctx, q)
		}
	}
}

// send records one event, or logs why it could not.
func (r *EventRecorder) send(ctx context.Context, q queued) {
	ctx, cancel := context.WithTimeout(ctx, r.opts.Deadline)
	defer cancel()
	pv, err := r.pvs.Get(ctx, q.pv, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			r.log.Debug("event dropped: no such PV", "reason", q.reason)
		} else {
			r.log.Warn("event dropped: the PV could not be read", "reason", q.reason, "error", reasonOf(err))
		}
		return
	}
	now := metav1.NewTime(r.now())
	msg := truncate(q.message, maxEventMessage)
	ev := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Event",
		"metadata": map[string]any{
			"name":      eventName(q.pv, string(pv.GetUID()), q.reason, msg),
			"namespace": eventNamespace,
		},
		"involvedObject": map[string]any{
			"apiVersion": "v1",
			"kind":       "PersistentVolume",
			"name":       q.pv,
			"uid":        string(pv.GetUID()),
		},
		"reason":             q.reason,
		"message":            msg,
		"type":               eventType(q.reason),
		"count":              int64(1),
		"firstTimestamp":     now.UTC().Format(time.RFC3339),
		"lastTimestamp":      now.UTC().Format(time.RFC3339),
		"source":             map[string]any{"component": r.opts.Component, "host": r.opts.Node},
		"reportingComponent": r.opts.Component,
		"reportingInstance":  r.opts.Node,
	}}
	_, err = r.events.Create(ctx, ev, metav1.CreateOptions{})
	switch {
	case err == nil:
	case apierrors.IsAlreadyExists(err):
		r.bump(ctx, ev.GetName(), now)
	default:
		r.log.Warn("event dropped: it could not be created", "reason", q.reason, "error", reasonOf(err))
	}
}

// bump counts one more occurrence of an event that exists.
func (r *EventRecorder) bump(ctx context.Context, name string, now metav1.Time) {
	cur, err := r.events.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		r.log.Warn("event not updated: it could not be read", "error", reasonOf(err))
		return
	}
	count, _, _ := unstructured.NestedInt64(cur.Object, "count")
	patch, err := json.Marshal(map[string]any{"count": count + 1, "lastTimestamp": now.UTC().Format(time.RFC3339)})
	if err != nil {
		return
	}
	if _, err := r.events.Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		r.log.Warn("event not updated: the patch failed", "error", reasonOf(err))
	}
}

func eventType(reason string) string {
	if warningReasons[reason] {
		return "Warning"
	}
	return "Normal"
}

// eventName is deterministic in (PV, its UID, reason, message), so that a
// repeat finds the event it repeats, and a PV deleted and made again under the
// same name does not bump an event whose involvedObject is the old one.
func eventName(pv, uid, reason, message string) string {
	sum := sha256.Sum256([]byte(uid + "\x00" + reason + "\x00" + message))
	return pv + "." + hex.EncodeToString(sum[:8])
}

// truncate cuts s to at most n bytes without splitting a UTF-8 sequence.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	s = s[:n]
	for len(s) > 0 && !validEnd(s) {
		s = s[:len(s)-1]
	}
	return strings.TrimSpace(s)
}

func validEnd(s string) bool {
	return strings.ToValidUTF8(s, "") == s
}
