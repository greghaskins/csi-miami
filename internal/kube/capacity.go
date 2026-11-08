package kube

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"

	"github.com/greghaskins/csi-miami/internal/cachecap"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// FieldManager is the field manager of every patch the node plugin makes, set
// explicitly instead of derived from the user agent.
const FieldManager = "miami-node"

// capacityPath is the JSON-pointer path of the node's cache resource in the
// node's status; the "/" of the resource name is escaped as "~1" (RFC 6901).
var capacityPath = "/status/capacity/" + strings.ReplaceAll(volume.CacheResource, "/", "~1")

type patchOp struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value string `json:"value,omitempty"`
}

// CacheCapacity reads the cache resource the node advertises in
// status.capacity: its value in bytes, and whether the node carries the key at
// all. A value that is not a whole-number quantity is an error, never "absent",
// and so is any answer that is not the node.
func (c *Client) CacheCapacity(ctx context.Context, node string) (int64, bool, error) {
	if node == "" {
		return 0, false, errors.New("kube: a node name is required")
	}
	ctx, cancel := context.WithTimeout(ctx, c.deadline)
	defer cancel()
	obj, err := c.nodes.Get(ctx, node, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) && isNotFoundOf(err, "nodes", node) {
			return 0, false, fmt.Errorf("%w: %q", ErrNodeNotFound, node)
		}
		return 0, false, &nodeReadError{node: node, reason: reasonOf(err), err: err}
	}
	v, found, err := unstructured.NestedFieldNoCopy(obj.Object, "status", "capacity", volume.CacheResource)
	if err != nil {
		return 0, false, fmt.Errorf("kube: node %q: %w", node, err)
	}
	if !found {
		return 0, false, nil
	}
	s, ok := v.(string)
	if !ok {
		return 0, false, fmt.Errorf("kube: node %q: %s is not a quantity", node, volume.CacheResource)
	}
	q, err := resource.ParseQuantity(s)
	if err != nil || q.Sign() < 0 || q.MilliValue() != q.Value()*1000 {
		return 0, false, fmt.Errorf("kube: node %q: %s is not a whole number of bytes", node, volume.CacheResource)
	}
	return q.Value(), true, nil
}

// SetCacheCapacity sets the cache resource in the node's status.capacity to
// bytes, with one JSON patch of the status subresource. "add" replaces an
// existing member, so one request covers both a first write and a change.
func (c *Client) SetCacheCapacity(ctx context.Context, node string, bytes int64) error {
	return c.patchCapacity(ctx, node, patchOp{
		Op: "add", Path: capacityPath, Value: resource.NewQuantity(bytes, resource.BinarySI).String(),
	})
}

// RemoveCacheCapacity removes the cache resource from the node's
// status.capacity. It is an error if the node does not carry it.
func (c *Client) RemoveCacheCapacity(ctx context.Context, node string) error {
	return c.patchCapacity(ctx, node, patchOp{Op: "remove", Path: capacityPath})
}

// fenceProbeResource is the capacity key ProbeFence tries to add. It is not
// ours to write, so the policy must deny it.
const fenceProbeResource = "miami.csi.greghaskins.com/fence-probe"

var fenceProbePath = "/status/capacity/" + strings.ReplaceAll(fenceProbeResource, "/", "~1")

// policyDenial is the API server's own text for a ValidatingAdmissionPolicy
// denial by this policy's [capacity] rule, anchored at the start of the
// message so that a webhook quoting it does not count. The policy's reason is
// Forbidden, so it can only ever answer 403. Only that counts as proof of the fence: a
// message that merely contains the policy's name does not.
var policyDenial = regexp.MustCompile(`^nodes "[^"]+" is forbidden: ValidatingAdmissionPolicy '` + regexp.QuoteMeta(cachecap.FencePolicy) + `' with binding '[^']+' denied request: \[capacity\]`)

// RemoveFenceProbe removes the probe's key from the node's status.capacity if
// the node carries it.
func (c *Client) RemoveFenceProbe(ctx context.Context, node string) error {
	if node == "" {
		return errors.New("kube: a node name is required")
	}
	gctx, cancel := context.WithTimeout(ctx, c.deadline)
	obj, err := c.nodes.Get(gctx, node, metav1.GetOptions{})
	cancel()
	if err != nil {
		return &nodeReadError{node: node, reason: reasonOf(err), err: err}
	}
	if _, found, _ := unstructured.NestedFieldNoCopy(obj.Object, "status", "capacity", fenceProbeResource); !found {
		return nil
	}
	return c.patchCapacity(ctx, node, patchOp{Op: "remove", Path: fenceProbePath})
}

// ProbeFence tries to add a capacity key the admission policy must refuse, on
// the node's own node. A denial that names the policy (FencePolicy) means the
// policy is in force; a patch that goes through means it is not, and the key is
// removed at once. Anything else (a timeout, a server error, a denial by RBAC
// or by something other than the policy) leaves the answer unknown.
func (c *Client) ProbeFence(ctx context.Context, node string) (cachecap.FenceState, error) {
	err := c.patchCapacity(ctx, node, patchOp{Op: "add", Path: fenceProbePath, Value: "1"})
	if err == nil {
		if rerr := c.patchCapacity(ctx, node, patchOp{Op: "remove", Path: fenceProbePath}); rerr != nil {
			return cachecap.Unfenced, fmt.Errorf("kube: the fence probe key was left on node %q: %w", node, rerr)
		}
		return cachecap.Unfenced, nil
	}
	var st apierrors.APIStatus
	if apierrors.IsForbidden(err) && errors.As(err, &st) && policyDenial.MatchString(st.Status().Message) {
		return cachecap.Fenced, nil
	}
	return cachecap.FenceUnknown, err
}

func (c *Client) patchCapacity(ctx context.Context, node string, op patchOp) error {
	if node == "" {
		return errors.New("kube: a node name is required")
	}
	body, err := json.Marshal([]patchOp{op})
	if err != nil {
		return fmt.Errorf("kube: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, c.deadline)
	defer cancel()
	if _, err := c.nodes.Patch(ctx, node, types.JSONPatchType, body, metav1.PatchOptions{FieldManager: FieldManager}, "status"); err != nil {
		return &nodePatchError{node: node, reason: reasonOf(err), err: err}
	}
	return nil
}

// nodePatchError is nodeReadError for a write: it names the node and says why,
// without client-go's message, which carries the server's URL.
type nodePatchError struct {
	node, reason string
	err          error
}

func (e *nodePatchError) Error() string {
	return fmt.Sprintf("kube: patching the capacity of node %q: %s", e.node, e.reason)
}
func (e *nodePatchError) Unwrap() error { return e.err }

// Subject is the identity the client's token names (for a service account,
// system:serviceaccount:<namespace>:<name>), read from the token's own claims
// without asking the API server, or "unknown". It is for log lines.
func (c *Client) Subject() string {
	if c.subject == "" {
		return "unknown"
	}
	return c.subject
}

// tokenSubject returns the sub claim of the bearer token in cfg, or "".
func tokenSubject(cfg *rest.Config) string {
	tok := cfg.BearerToken
	if tok == "" && cfg.BearerTokenFile != "" {
		b, err := os.ReadFile(cfg.BearerTokenFile)
		if err != nil {
			return ""
		}
		tok = strings.TrimSpace(string(b))
	}
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		return ""
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return ""
	}
	var claims struct {
		Sub string `json:"sub"`
	}
	if json.Unmarshal(payload, &claims) != nil {
		return ""
	}
	return claims.Sub
}
