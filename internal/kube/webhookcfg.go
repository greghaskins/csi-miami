package kube

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// The reads and writes behind the webhook's TLS: the Secret that holds the
// serving material (in the driver's namespace) and the caBundle of the
// MutatingWebhookConfiguration. Same discipline as GetPV: ErrNotFound only for
// the API server's own NotFound about the object asked for, and no error text
// that carries the server's URL.

var (
	// ErrAlreadyExists is wrapped by the error CreateSecret returns when the
	// Secret exists.
	ErrAlreadyExists = errors.New("kube: already exists")
	// ErrConflict is wrapped by the error UpdateSecret returns when the Secret
	// changed since it was read (its resourceVersion no longer matches).
	ErrConflict = errors.New("kube: the object changed since it was read")
)

var (
	secretResource  = schema.GroupVersionResource{Version: "v1", Resource: "secrets"}
	mutatingWebhook = schema.GroupVersionResource{Group: "admissionregistration.k8s.io", Version: "v1", Resource: "mutatingwebhookconfigurations"}
)

// SecretTypeTLS is the type of the webhook's Secret.
const SecretTypeTLS = "kubernetes.io/tls" //nolint:gosec // a Secret type, not a credential

// Secret is a Secret's type, data and resourceVersion; the metadata beyond
// that is not read.
type Secret struct {
	Namespace, Name string
	Type            string
	Data            map[string][]byte
	// ResourceVersion is set by GetSecret and is what UpdateSecret sends, so
	// an update of a Secret someone else changed is refused with ErrConflict.
	ResourceVersion string
}

// GetSecret returns the Secret. If there is none, the error wraps ErrNotFound.
func (c *Client) GetSecret(ctx context.Context, namespace, name string) (*Secret, error) {
	if namespace == "" || name == "" {
		return nil, errors.New("kube: a Secret namespace and name are required")
	}
	ctx, cancel := context.WithTimeout(ctx, c.deadline)
	defer cancel()
	obj, err := c.dyn.Resource(secretResource).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, readFailure(err, "secrets", name, "Secret")
	}
	s := &Secret{Namespace: namespace, Name: name, ResourceVersion: obj.GetResourceVersion(), Data: map[string][]byte{}}
	if s.Type, _, err = unstructured.NestedString(obj.Object, "type"); err != nil {
		return nil, fmt.Errorf("kube: Secret %q: %w", name, err)
	}
	data, _, err := unstructured.NestedStringMap(obj.Object, "data")
	if err != nil {
		return nil, fmt.Errorf("kube: Secret %q: %w", name, err)
	}
	for k, v := range data {
		b, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return nil, fmt.Errorf("kube: Secret %q: key %q is not base64", name, k)
		}
		s.Data[k] = b
	}
	return s, nil
}

func secretObject(s *Secret) *unstructured.Unstructured {
	data := make(map[string]any, len(s.Data))
	for k, v := range s.Data {
		data[k] = base64.StdEncoding.EncodeToString(v)
	}
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Secret", "type": s.Type, "data": data,
		"metadata": map[string]any{"name": s.Name, "namespace": s.Namespace},
	}}
	if s.ResourceVersion != "" {
		obj.SetResourceVersion(s.ResourceVersion)
	}
	return obj
}

// CreateSecret creates the Secret; an existing one is an error wrapping
// ErrAlreadyExists and is never replaced.
func (c *Client) CreateSecret(ctx context.Context, s *Secret) error {
	ctx, cancel := context.WithTimeout(ctx, c.deadline)
	defer cancel()
	_, err := c.dyn.Resource(secretResource).Namespace(s.Namespace).Create(ctx, secretObject(s), metav1.CreateOptions{FieldManager: ControllerFieldManager})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("%w: Secret %q", ErrAlreadyExists, s.Name)
		}
		return &writeError{what: "Secret " + fmt.Sprintf("%q", s.Name), verb: "creating", reason: reasonOf(err), err: err}
	}
	return nil
}

// UpdateSecret replaces the Secret's data at the resourceVersion read; a
// Secret changed since is an error wrapping ErrConflict. A Secret with no
// ResourceVersion is refused: an unconditional replace would overwrite
// whatever another replica wrote.
func (c *Client) UpdateSecret(ctx context.Context, s *Secret) error {
	if s.ResourceVersion == "" {
		return errors.New("kube: updating a Secret needs the resourceVersion it was read at")
	}
	ctx, cancel := context.WithTimeout(ctx, c.deadline)
	defer cancel()
	_, err := c.dyn.Resource(secretResource).Namespace(s.Namespace).Update(ctx, secretObject(s), metav1.UpdateOptions{FieldManager: ControllerFieldManager})
	if err != nil {
		if apierrors.IsConflict(err) {
			return fmt.Errorf("%w: Secret %q", ErrConflict, s.Name)
		}
		return &writeError{what: "Secret " + fmt.Sprintf("%q", s.Name), verb: "updating", reason: reasonOf(err), err: err}
	}
	return nil
}

// ControllerFieldManager is the field manager of the controller's writes.
const ControllerFieldManager = "miami-controller"

// WebhookConfig is the part of a MutatingWebhookConfiguration the controller
// reads: the caBundle of each of its webhooks, in order.
type WebhookConfig struct {
	Name      string
	CABundles [][]byte
}

// GetMutatingWebhookConfiguration returns the configuration. If there is none,
// the error wraps ErrNotFound.
func (c *Client) GetMutatingWebhookConfiguration(ctx context.Context, name string) (*WebhookConfig, error) {
	if name == "" {
		return nil, errors.New("kube: a webhook configuration name is required")
	}
	ctx, cancel := context.WithTimeout(ctx, c.deadline)
	defer cancel()
	obj, err := c.dyn.Resource(mutatingWebhook).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, readFailure(err, "mutatingwebhookconfigurations", name, "MutatingWebhookConfiguration")
	}
	whs, _, err := unstructured.NestedSlice(obj.Object, "webhooks")
	if err != nil {
		return nil, fmt.Errorf("kube: MutatingWebhookConfiguration %q: %w", name, err)
	}
	cfg := &WebhookConfig{Name: name}
	for i, w := range whs {
		m, ok := w.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("kube: MutatingWebhookConfiguration %q: webhooks[%d] is not an object", name, i)
		}
		s, _, err := unstructured.NestedString(m, "clientConfig", "caBundle")
		if err != nil {
			return nil, fmt.Errorf("kube: MutatingWebhookConfiguration %q: webhooks[%d]: %w", name, i, err)
		}
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, fmt.Errorf("kube: MutatingWebhookConfiguration %q: webhooks[%d]: caBundle is not base64", name, i)
		}
		cfg.CABundles = append(cfg.CABundles, b)
	}
	return cfg, nil
}

type bundleOp struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value string `json:"value"`
}

// PatchCABundle sets clientConfig.caBundle of the webhooks at the given
// indexes to bundle with one JSON patch: a replace per index and nothing else,
// so no other field of the configuration is touched.
func (c *Client) PatchCABundle(ctx context.Context, name string, indexes []int, bundle []byte) error {
	if name == "" {
		return errors.New("kube: a webhook configuration name is required")
	}
	if len(indexes) == 0 {
		return nil
	}
	v := base64.StdEncoding.EncodeToString(bundle)
	ops := make([]bundleOp, 0, len(indexes))
	for _, i := range indexes {
		ops = append(ops, bundleOp{Op: "replace", Path: fmt.Sprintf("/webhooks/%d/clientConfig/caBundle", i), Value: v})
	}
	body, err := json.Marshal(ops)
	if err != nil {
		return fmt.Errorf("kube: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, c.deadline)
	defer cancel()
	if _, err := c.dyn.Resource(mutatingWebhook).Patch(ctx, name, types.JSONPatchType, body, metav1.PatchOptions{FieldManager: ControllerFieldManager}); err != nil {
		return &writeError{verb: "patching", what: "MutatingWebhookConfiguration " + fmt.Sprintf("%q", name), reason: reasonOf(err), err: err}
	}
	return nil
}

// writeError names the object and says why in a few words, without client-go's
// message, which carries the server's URL.
type writeError struct {
	what, verb, reason string
	err                error
}

func (e *writeError) Error() string { return fmt.Sprintf("kube: %s %s: %s", e.verb, e.what, e.reason) }
func (e *writeError) Unwrap() error { return e.err }
