package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"

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

const (
	// mutateDeadline bounds one Mutate call, whatever its Claims do. The
	// webhook configuration gives the API server timeoutSeconds: 10 and then
	// admits the pod as sent (failurePolicy Ignore); finishing at 7s leaves
	// time to answer, so the strip patch below still reaches the API server.
	mutateDeadline = 7 * time.Second

	// maxClaimLookups caps the distinct claims (and claim templates) one pod
	// makes the webhook look up, so a pod with thousands of volumes cannot
	// turn one CREATE into thousands of API reads.
	maxClaimLookups = 64
)

// errTooManyClaims is the pod having more claims than maxClaimLookups.
var errTooManyClaims = errors.New("the pod has more claims than the webhook will look up")

// Mutator turns a pod CREATE into the JSON patch that requests the pod's
// cache footprint. It holds no state and is safe for concurrent use.
type Mutator struct {
	Claims  Claims
	Metrics Metrics
	Log     *slog.Logger
	// Deadline bounds one Mutate call; zero means mutateDeadline.
	Deadline time.Duration
}

// NewMutator returns a Mutator, or an error when it would have nothing to
// look claims up with or nowhere to count. A nil log means slog.Default.
func NewMutator(claims Claims, metrics Metrics, log *slog.Logger) (*Mutator, error) {
	if claims == nil {
		return nil, errors.New("webhook: Claims is required")
	}
	if metrics == nil {
		return nil, errors.New("webhook: Metrics is required")
	}
	return &Mutator{Claims: claims, Metrics: metrics, Log: log}, nil
}

func (m *Mutator) log() *slog.Logger {
	if m.Log != nil {
		return m.Log
	}
	return slog.Default()
}

func (m *Mutator) outcome(o string) {
	if m.Metrics != nil {
		m.Metrics.Outcome(o)
	}
}

func (m *Mutator) unresolvedCount() {
	if m.Metrics != nil {
		m.Metrics.Unresolved()
	}
}

// Mutate answers an admission request for a pod. It always allows the pod: the
// webhook only accounts, and a failure of its own (a lookup, a deadline, a
// pod with too many claims, a bug) is counted as error. On such a failure the
// pod still has the cache resource removed from every container, so a value
// written by hand cannot survive by making the webhook fail; only a pod that
// cannot be decoded at all is allowed as it came.
func (m *Mutator) Mutate(ctx context.Context, req *admissionv1.AdmissionRequest) (resp *admissionv1.AdmissionResponse) {
	if req == nil {
		return &admissionv1.AdmissionResponse{Allowed: true}
	}
	resp = &admissionv1.AdmissionResponse{UID: req.UID, Allowed: true}
	var raw map[string]any
	defer func() {
		if r := recover(); r != nil {
			m.log().Error("the cache webhook panicked, so the pod is admitted with the resource stripped", "panic", fmt.Sprint(r))
			m.outcome(outcomeError)
			resp = stripResponse(req.UID, raw)
		}
	}()

	if req.Operation != admissionv1.Create || req.SubResource != "" || req.Kind.Kind != "Pod" || req.Kind.Group != "" {
		m.outcome(outcomeUnchanged)
		return resp
	}
	var pod corev1.Pod
	if err := json.Unmarshal(req.Object.Raw, &pod); err != nil {
		return m.failed(resp, nil, req, fmt.Errorf("decoding the pod: %w", err))
	}
	if err := json.Unmarshal(req.Object.Raw, &raw); err != nil {
		return m.failed(resp, nil, req, fmt.Errorf("decoding the pod: %w", err))
	}
	// The claims live in the request's namespace. The API server sets it for
	// every namespaced CREATE; the pod's own metadata is user-controlled and is
	// never consulted.
	if req.Namespace == "" || len(pod.Spec.Containers) == 0 {
		m.outcome(outcomeUnchanged)
		return resp
	}

	sum, err := m.footprint(ctx, req.Namespace, &pod)
	if err != nil {
		return m.failed(resp, raw, req, err)
	}
	want := ""
	if sum > 0 {
		want = resource.NewQuantity(sum, resource.BinarySI).String()
	}
	ops := patchOps(raw, want)
	if len(ops) == 0 {
		m.outcome(outcomeUnchanged)
		return resp
	}
	patch, err := json.Marshal(ops)
	if err != nil {
		return m.failed(resp, raw, req, err)
	}
	if want == "" {
		m.outcome(outcomeStripped)
	} else {
		m.outcome(outcomeMutated)
	}
	pt := admissionv1.PatchTypeJSONPatch
	resp.Patch, resp.PatchType = patch, &pt
	return resp
}

// failed counts an error and answers with the strip patch (none when raw is
// nil, the pod being unreadable). The error text goes to the log only.
func (m *Mutator) failed(resp *admissionv1.AdmissionResponse, raw map[string]any, req *admissionv1.AdmissionRequest, err error) *admissionv1.AdmissionResponse {
	m.log().Warn("the cache webhook could not account for a pod, so it is admitted with the resource stripped", "namespace", req.Namespace, "error", err)
	m.outcome(outcomeError)
	return stripResponse(resp.UID, raw)
}

// stripResponse allows the pod and removes the cache resource from every
// container that carries it.
func stripResponse(uid types.UID, raw map[string]any) *admissionv1.AdmissionResponse {
	resp := &admissionv1.AdmissionResponse{UID: uid, Allowed: true}
	if raw == nil {
		return resp
	}
	if ops := patchOps(raw, ""); len(ops) > 0 {
		if patch, err := json.Marshal(ops); err == nil {
			pt := admissionv1.PatchTypeJSONPatch
			resp.Patch, resp.PatchType = patch, &pt
		}
	}
	return resp
}

// footprint resolves the pod's volumes under the call's deadline. The
// resolver runs in its own goroutine so that a Claims that ignores its context
// still cannot hold the call past the deadline; its answer is then dropped.
func (m *Mutator) footprint(ctx context.Context, ns string, pod *corev1.Pod) (int64, error) {
	d := m.Deadline
	if d <= 0 {
		d = mutateDeadline
	}
	ctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	type result struct {
		sum int64
		err error
	}
	ch := make(chan result, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				ch <- result{err: fmt.Errorf("panic while resolving volumes: %v", r)}
			}
		}()
		r := &resolver{m: m, ns: ns, classes: map[string]*kube.StorageClass{}, known: map[string]bool{}}
		sum, err := r.footprint(ctx, pod)
		ch <- result{sum, err}
	}()
	select {
	case res := <-ch:
		return res.sum, res.err
	case <-ctx.Done():
		return 0, fmt.Errorf("resolving volumes: %w", ctx.Err())
	}
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
