package kube

import (
	"context"
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/greghaskins/csi-miami/internal/volume"
)

// OutOfServiceTaint is the standard taint an operator puts on a node that is
// shut down and not coming back (non-graceful node shutdown). Under decision
// 52 it is one of the two ways to abandon what a node holds; any effect counts.
const OutOfServiceTaint = volume.OutOfServiceTaint

// ErrNodeNotFound is wrapped by the error NodeStatus returns when the API
// server answers that no node has the name.
var ErrNodeNotFound = errors.New("kube: node not found")

var nodeResource = schema.GroupVersionResource{Version: "v1", Resource: "nodes"}

// NodeInfo is the part of a Node the node agent reads.
type NodeInfo struct {
	// Ready is the Ready condition being True.
	Ready bool
	// OutOfService is the node carrying OutOfServiceTaint, with any effect.
	OutOfService bool
}

// NodeStatus reads one node. An unreachable API server, a denied request or an
// answer that cannot be decoded is an error, never a zero NodeInfo: a caller
// must not read "could not tell" as "not out of service" or as "ready".
func (c *Client) NodeStatus(ctx context.Context, name string) (NodeInfo, error) {
	if name == "" {
		return NodeInfo{}, errors.New("kube: a node name is required")
	}
	ctx, cancel := context.WithTimeout(ctx, c.deadline)
	defer cancel()
	obj, err := c.nodes.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) && isNotFoundOf(err, "nodes", name) {
			return NodeInfo{}, fmt.Errorf("%w: %q", ErrNodeNotFound, name)
		}
		return NodeInfo{}, &nodeReadError{node: name, reason: reasonOf(err), err: err}
	}
	return decodeNode(obj)
}

// nodeReadError is ReadError for a node: its text names the node and says why
// in a few words, without client-go's message, which carries the server's URL.
type nodeReadError struct {
	node, reason string
	err          error
}

func (e *nodeReadError) Error() string {
	return fmt.Sprintf("kube: reading node %q: %s", e.node, e.reason)
}
func (e *nodeReadError) Unwrap() error { return e.err }

func decodeNode(obj *unstructured.Unstructured) (NodeInfo, error) {
	var info NodeInfo
	taints, _, err := unstructured.NestedSlice(obj.Object, "spec", "taints")
	if err != nil {
		return NodeInfo{}, fmt.Errorf("kube: node %q: %w", obj.GetName(), err)
	}
	for _, t := range taints {
		m, ok := t.(map[string]any)
		if !ok {
			return NodeInfo{}, fmt.Errorf("kube: node %q: a taint is not an object", obj.GetName())
		}
		if key, _ := m["key"].(string); key == OutOfServiceTaint {
			info.OutOfService = true
		}
	}
	conds, _, err := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if err != nil {
		return NodeInfo{}, fmt.Errorf("kube: node %q: %w", obj.GetName(), err)
	}
	for _, c := range conds {
		m, ok := c.(map[string]any)
		if !ok {
			continue
		}
		if m["type"] == "Ready" && m["status"] == "True" {
			info.Ready = true
		}
	}
	return info, nil
}

// ForceDetach returns the value of the force-detach annotation on the PV
// called name (AnnotationForceDetach) and whether it is set. A PV that does
// not exist has no annotation: ("", false, nil). Any other failure to read it
// is an error.
func (c *Client) ForceDetach(ctx context.Context, name string) (string, bool, error) {
	pv, err := c.GetPV(ctx, name)
	switch {
	case errors.Is(err, ErrNotFound):
		return "", false, nil
	case err != nil:
		return "", false, err
	}
	v, ok := pv.Annotations[AnnotationForceDetach]
	return v, ok, nil
}

// NodeOutOfService reports whether the node carries OutOfServiceTaint.
func (c *Client) NodeOutOfService(ctx context.Context, name string) (bool, error) {
	info, err := c.NodeStatus(ctx, name)
	return info.OutOfService, err
}
