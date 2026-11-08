package kube

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	"github.com/greghaskins/csi-miami/internal/volume"
)

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
	if _, err := c.nodes.Patch(ctx, node, types.JSONPatchType, body, metav1.PatchOptions{}, "status"); err != nil {
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
