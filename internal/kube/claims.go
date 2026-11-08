package kube

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// The claim reads of the admission webhook: PersistentVolumeClaims and
// StorageClasses, with the same discipline as GetPV. Only the fields the
// webhook needs are decoded, and ErrNotFound is returned only for the API
// server's own NotFound about the object asked for.

var (
	pvcResource   = schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumeclaims"}
	classResource = schema.GroupVersionResource{Group: "storage.k8s.io", Version: "v1", Resource: "storageclasses"}
)

// Annotations that mark a StorageClass as the cluster default, the second
// being the older spelling the API server still honours.
const (
	annotationDefaultClass     = "storageclass.kubernetes.io/is-default-class"
	annotationDefaultClassBeta = "storageclass.beta.kubernetes.io/is-default-class"
)

// PVC is the part of a PersistentVolumeClaim the webhook reads.
type PVC struct {
	Namespace, Name string
	// StorageClassName is spec.storageClassName; StorageClassSet says whether
	// the field was present, because an unset name means the cluster default
	// and an empty one means no class at all.
	StorageClassName string
	StorageClassSet  bool
	VolumeName       string   // spec.volumeName: the PV it is bound (or pre-bound) to
	RequestBytes     int64    // spec.resources.requests.storage
	AccessModes      []string // spec.accessModes
	Phase            string   // status.phase: Pending, Bound or Lost
	Terminating      bool     // metadata.deletionTimestamp is set
}

// StorageClass is the part of a StorageClass the webhook reads.
type StorageClass struct {
	Name, Provisioner string
	Parameters        map[string]string
}

// GetPVC returns the claim. If there is none, the error wraps ErrNotFound; any
// other error means the answer is unknown.
func (c *Client) GetPVC(ctx context.Context, namespace, name string) (*PVC, error) {
	if namespace == "" || name == "" {
		return nil, errors.New("kube: a PVC namespace and name are required")
	}
	ctx, cancel := context.WithTimeout(ctx, c.deadline)
	defer cancel()
	obj, err := c.dyn.Resource(pvcResource).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, readFailure(err, "persistentvolumeclaims", name, "PVC")
	}
	return decodePVC(obj)
}

// GetStorageClass returns the class. If there is none, the error wraps
// ErrNotFound.
func (c *Client) GetStorageClass(ctx context.Context, name string) (*StorageClass, error) {
	if name == "" {
		return nil, errors.New("kube: a StorageClass name is required")
	}
	ctx, cancel := context.WithTimeout(ctx, c.deadline)
	defer cancel()
	obj, err := c.dyn.Resource(classResource).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, readFailure(err, "storageclasses", name, "StorageClass")
	}
	return decodeClass(obj)
}

// DefaultStorageClass returns the cluster's default class, or nil, nil when
// there is none. With several marked default it picks the newest by creation
// time, as the API server's own defaulting does (the name breaks a tie, so
// the answer is stable).
func (c *Client) DefaultStorageClass(ctx context.Context) (*StorageClass, error) {
	ctx, cancel := context.WithTimeout(ctx, c.deadline)
	defer cancel()
	list, err := c.dyn.Resource(classResource).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, &claimReadError{what: "StorageClass list", reason: reasonOf(err), err: err}
	}
	var defaults []unstructured.Unstructured
	for _, o := range list.Items {
		a := o.GetAnnotations()
		if a[annotationDefaultClass] == "true" || a[annotationDefaultClassBeta] == "true" {
			defaults = append(defaults, o)
		}
	}
	if len(defaults) == 0 {
		return nil, nil
	}
	sort.Slice(defaults, func(i, j int) bool {
		ti, tj := defaults[i].GetCreationTimestamp(), defaults[j].GetCreationTimestamp()
		if !ti.Equal(&tj) {
			return tj.Before(&ti)
		}
		return defaults[i].GetName() < defaults[j].GetName()
	})
	return decodeClass(&defaults[0])
}

// readFailure maps a failed GET of resource/name to ErrNotFound (only for the
// API server's own NotFound about that object) or to a claimReadError.
func readFailure(err error, resource, name, what string) error {
	if apierrors.IsNotFound(err) && isNotFoundOf(err, resource, name) {
		return fmt.Errorf("%w: %s %q", ErrNotFound, what, name)
	}
	return &claimReadError{what: what + " " + fmt.Sprintf("%q", name), reason: reasonOf(err), err: err}
}

// claimReadError is ReadError for the claim reads: its text names the object
// and says why in a few words, without client-go's message, which carries the
// server's URL.
type claimReadError struct {
	what, reason string
	err          error
}

func (e *claimReadError) Error() string { return fmt.Sprintf("kube: reading %s: %s", e.what, e.reason) }
func (e *claimReadError) Unwrap() error { return e.err }

func decodePVC(obj *unstructured.Unstructured) (*PVC, error) {
	p := &PVC{Namespace: obj.GetNamespace(), Name: obj.GetName(), Terminating: obj.GetDeletionTimestamp() != nil}
	fail := func(err error) (*PVC, error) {
		return nil, fmt.Errorf("kube: PVC %q: %w", p.Name, err)
	}
	var err error
	if v, found, err := unstructured.NestedFieldNoCopy(obj.Object, "spec", "storageClassName"); err != nil {
		return fail(err)
	} else if found && v != nil {
		s, ok := v.(string)
		if !ok {
			return fail(errors.New("spec.storageClassName is not a string"))
		}
		p.StorageClassName, p.StorageClassSet = s, true
	}
	if p.VolumeName, _, err = unstructured.NestedString(obj.Object, "spec", "volumeName"); err != nil {
		return fail(err)
	}
	if p.Phase, _, err = unstructured.NestedString(obj.Object, "status", "phase"); err != nil {
		return fail(err)
	}
	if p.AccessModes, _, err = unstructured.NestedStringSlice(obj.Object, "spec", "accessModes"); err != nil {
		return fail(err)
	}
	if p.RequestBytes, err = nestedQuantity(obj.Object, "spec", "resources", "requests", "storage"); err != nil {
		return fail(err)
	}
	return p, nil
}

func decodeClass(obj *unstructured.Unstructured) (*StorageClass, error) {
	sc := &StorageClass{Name: obj.GetName()}
	var err error
	if sc.Provisioner, _, err = unstructured.NestedString(obj.Object, "provisioner"); err != nil {
		return nil, fmt.Errorf("kube: StorageClass %q: %w", sc.Name, err)
	}
	if sc.Parameters, _, err = unstructured.NestedStringMap(obj.Object, "parameters"); err != nil {
		return nil, fmt.Errorf("kube: StorageClass %q: %w", sc.Name, err)
	}
	return sc, nil
}

// nestedQuantity reads a resource.Quantity field as whole bytes, rounded up;
// 0 when the field is absent. A quantity beyond int64 is MaxInt64, which no
// caller can satisfy, rather than an overflowed small number.
func nestedQuantity(obj map[string]any, fields ...string) (int64, error) {
	s, found, err := unstructured.NestedString(obj, fields...)
	if err != nil {
		return 0, err
	}
	if !found || s == "" {
		return 0, nil
	}
	q, err := resource.ParseQuantity(s)
	if err != nil {
		return 0, fmt.Errorf("%v is not a quantity: %w", fields, err)
	}
	if q.Sign() < 0 {
		return 0, fmt.Errorf("%v is negative", fields)
	}
	if q.Cmp(*resource.NewQuantity(math.MaxInt64, resource.BinarySI)) >= 0 {
		return math.MaxInt64, nil
	}
	return q.Value(), nil
}
