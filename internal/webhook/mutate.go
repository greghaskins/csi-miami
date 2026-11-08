package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/greghaskins/csi-miami/internal/kube"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// Metrics counts what the webhook did; the registry built with
// metrics.WithWebhook implements it.
type Metrics interface {
	// Outcome counts one pod: mutated, unchanged, stripped or error.
	Outcome(outcome string)
	// Unresolved counts one pod volume the webhook could not account for.
	Unresolved()
}

// Outcomes of one pod.
const (
	outcomeMutated   = "mutated"   // the cache request was set or changed
	outcomeUnchanged = "unchanged" // nothing to do
	outcomeStripped  = "stripped"  // no volume of ours, and a stray request was removed
	outcomeError     = "error"     // our own failure; the pod was allowed unchanged
)

// Mutator turns a pod CREATE into the JSON patch that requests the pod's
// cache footprint. It holds no state and is safe for concurrent use.
type Mutator struct {
	Claims  Claims
	Metrics Metrics
	Log     *slog.Logger
}

func (m *Mutator) log() *slog.Logger {
	if m.Log != nil {
		return m.Log
	}
	return slog.Default()
}

// Mutate answers an admission request for a pod. It always allows the pod: the
// webhook only accounts, and a failure of its own (a lookup, a pod it cannot
// read, a bug) is counted and leaves the pod as it came.
func (m *Mutator) Mutate(ctx context.Context, req *admissionv1.AdmissionRequest) (resp *admissionv1.AdmissionResponse) {
	if req == nil {
		return &admissionv1.AdmissionResponse{Allowed: true}
	}
	resp = &admissionv1.AdmissionResponse{UID: req.UID, Allowed: true}
	defer func() {
		if r := recover(); r != nil {
			m.log().Error("the cache webhook panicked, so the pod is admitted unchanged", "panic", fmt.Sprint(r))
			m.Metrics.Outcome(outcomeError)
			resp = &admissionv1.AdmissionResponse{UID: req.UID, Allowed: true}
		}
	}()

	if req.Operation != admissionv1.Create || req.SubResource != "" || req.Kind.Kind != "Pod" || req.Kind.Group != "" {
		m.Metrics.Outcome(outcomeUnchanged)
		return resp
	}
	outcome, patch, err := m.patchFor(ctx, req)
	if err != nil {
		// The error text is for the log only: the response carries none.
		m.log().Warn("the cache webhook could not account for a pod, so it is admitted unchanged", "namespace", req.Namespace, "error", err)
		m.Metrics.Outcome(outcomeError)
		return resp
	}
	m.Metrics.Outcome(outcome)
	if len(patch) > 0 {
		pt := admissionv1.PatchTypeJSONPatch
		resp.Patch, resp.PatchType = patch, &pt
	}
	return resp
}

// patchFor decodes the pod, works out its footprint and builds the patch. An
// empty patch means nothing to change.
func (m *Mutator) patchFor(ctx context.Context, req *admissionv1.AdmissionRequest) (string, []byte, error) {
	var pod corev1.Pod
	if err := json.Unmarshal(req.Object.Raw, &pod); err != nil {
		return "", nil, fmt.Errorf("decoding the pod: %w", err)
	}
	var raw map[string]any
	if err := json.Unmarshal(req.Object.Raw, &raw); err != nil {
		return "", nil, fmt.Errorf("decoding the pod: %w", err)
	}
	if len(pod.Spec.Containers) == 0 {
		return outcomeUnchanged, nil, nil
	}
	ns := req.Namespace
	if ns == "" {
		ns = pod.Namespace
	}
	r := &resolver{m: m, ns: ns, classes: map[string]*kube.StorageClass{}, known: map[string]bool{}}
	sum, err := r.footprint(ctx, &pod)
	if err != nil {
		return "", nil, err
	}
	want := ""
	if sum > 0 {
		want = resource.NewQuantity(sum, resource.BinarySI).String()
	}
	ops := patchOps(raw, want)
	if len(ops) == 0 {
		return outcomeUnchanged, nil, nil
	}
	patch, err := json.Marshal(ops)
	if err != nil {
		return "", nil, err
	}
	if want == "" {
		return outcomeStripped, patch, nil
	}
	return outcomeMutated, patch, nil
}

// op is one JSON patch operation.
type op struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value,omitempty"`
}

// resourceKey is the resource's name as a JSON pointer token.
var resourceKey = pointerToken(volume.CacheResource)

func pointerToken(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "~", "~0"), "/", "~1")
}

// patchOps is the patch that leaves want (a quantity, or "" for none) on the
// first regular container as both request and limit, and the resource nowhere
// else. It reads the pod as the API server sent it, so it knows which of
// resources, requests and limits exist, and touches only the resource's key
// (or adds a map that was not there, holding only that key).
func patchOps(pod map[string]any, want string) []op {
	spec, _ := pod["spec"].(map[string]any)
	var ops []op
	for _, list := range []string{"initContainers", "containers", "ephemeralContainers"} {
		cs, _ := spec[list].([]any)
		for i, c := range cs {
			cm, _ := c.(map[string]any)
			w := ""
			if list == "containers" && i == 0 {
				w = want
			}
			base := "/spec/" + list + "/" + strconv.Itoa(i) + "/resources"
			ops = append(ops, containerOps(base, cm["resources"], w)...)
		}
	}
	// A pod-level resources block, where present, is cleaned too.
	if r, ok := spec["resources"]; ok {
		ops = append(ops, containerOps("/spec/resources", r, "")...)
	}
	return ops
}

// containerOps is the operations for one resources object (nil when absent or
// null) that leave want in requests and limits, or nothing when want is "".
func containerOps(base string, resources any, want string) []op {
	rm, hasRes := resources.(map[string]any)
	if !hasRes {
		if want == "" {
			return nil
		}
		return []op{{Op: "add", Path: base, Value: map[string]any{
			"requests": map[string]any{volume.CacheResource: want},
			"limits":   map[string]any{volume.CacheResource: want},
		}}}
	}
	var ops []op
	for _, field := range []string{"requests", "limits"} {
		m, hasMap := rm[field].(map[string]any)
		cur, hasCur := m[volume.CacheResource]
		path := base + "/" + field
		switch {
		case !hasCur && want == "":
		case hasCur && want == "":
			ops = append(ops, op{Op: "remove", Path: path + "/" + resourceKey})
		case hasCur && sameQuantity(cur, want):
		case hasCur:
			ops = append(ops, op{Op: "replace", Path: path + "/" + resourceKey, Value: want})
		case hasMap:
			ops = append(ops, op{Op: "add", Path: path + "/" + resourceKey, Value: want})
		default:
			ops = append(ops, op{Op: "add", Path: path, Value: map[string]any{volume.CacheResource: want}})
		}
	}
	return ops
}

// sameQuantity reports whether the value in the pod is the quantity want.
func sameQuantity(cur any, want string) bool {
	var s string
	switch v := cur.(type) {
	case string:
		s = v
	case float64:
		s = strconv.FormatFloat(v, 'f', -1, 64)
	default:
		return false
	}
	a, err := resource.ParseQuantity(s)
	if err != nil {
		return false
	}
	b, err := resource.ParseQuantity(want)
	return err == nil && a.Cmp(b) == 0
}
