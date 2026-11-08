package kube

import (
	"context"
	"errors"
	"math"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func notFound(resource, name string) metav1.Status {
	return metav1.Status{
		TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}, Status: metav1.StatusFailure,
		Code: 404, Reason: metav1.StatusReasonNotFound, Message: "x",
		Details: &metav1.StatusDetails{Name: name, Kind: resource},
	}
}

func class(name string, anns map[string]any, created, provisioner string, params map[string]any) map[string]any {
	meta := map[string]any{"name": name, "creationTimestamp": created}
	if anns != nil {
		meta["annotations"] = anns
	}
	o := map[string]any{"apiVersion": "storage.k8s.io/v1", "kind": "StorageClass", "metadata": meta, "provisioner": provisioner}
	if params != nil {
		o["parameters"] = params
	}
	return o
}

func TestClaimsGetPVC(t *testing.T) {
	t.Parallel()
	bound := map[string]any{
		"apiVersion": "v1", "kind": "PersistentVolumeClaim",
		"metadata": map[string]any{"name": "data", "namespace": "ns"},
		"spec": map[string]any{
			"storageClassName": "fast", "volumeName": "pvc-1", "accessModes": []any{"ReadWriteOnce"},
			"resources": map[string]any{"requests": map[string]any{"storage": "10Gi"}},
		},
		"status": map[string]any{"phase": "Bound"},
	}
	noClass := map[string]any{
		"apiVersion": "v1", "kind": "PersistentVolumeClaim",
		"metadata": map[string]any{"name": "nc", "namespace": "ns", "deletionTimestamp": "2026-01-01T00:00:00Z"},
		"spec":     map[string]any{"resources": map[string]any{"requests": map[string]any{"storage": "1Gi"}}},
	}
	emptyClass := map[string]any{
		"apiVersion": "v1", "kind": "PersistentVolumeClaim",
		"metadata": map[string]any{"name": "ec", "namespace": "ns"},
		"spec":     map[string]any{"storageClassName": ""},
	}
	badQty := map[string]any{
		"apiVersion": "v1", "kind": "PersistentVolumeClaim",
		"metadata": map[string]any{"name": "bq", "namespace": "ns"},
		"spec":     map[string]any{"resources": map[string]any{"requests": map[string]any{"storage": "lots"}}},
	}
	huge := map[string]any{
		"apiVersion": "v1", "kind": "PersistentVolumeClaim",
		"metadata": map[string]any{"name": "huge", "namespace": "ns"},
		"spec":     map[string]any{"resources": map[string]any{"requests": map[string]any{"storage": "100Ei"}}},
	}
	cfg := apiServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/namespaces/ns/persistentvolumeclaims/data":
			writeJSON(w, 200, bound)
		case "/api/v1/namespaces/ns/persistentvolumeclaims/nc":
			writeJSON(w, 200, noClass)
		case "/api/v1/namespaces/ns/persistentvolumeclaims/ec":
			writeJSON(w, 200, emptyClass)
		case "/api/v1/namespaces/ns/persistentvolumeclaims/bq":
			writeJSON(w, 200, badQty)
		case "/api/v1/namespaces/ns/persistentvolumeclaims/huge":
			writeJSON(w, 200, huge)
		case "/api/v1/namespaces/ns/persistentvolumeclaims/gone":
			writeJSON(w, 404, notFound("persistentvolumeclaims", "gone"))
		case "/api/v1/namespaces/ns/persistentvolumeclaims/other":
			// A NotFound about something else is not "no such PVC".
			writeJSON(w, 404, notFound("persistentvolumeclaims", "different"))
		case "/api/v1/namespaces/ns/persistentvolumeclaims/page":
			http.Error(w, "<html>404</html>", 404)
		case "/api/v1/namespaces/ns/persistentvolumeclaims/denied":
			writeJSON(w, 403, metav1.Status{TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}, Status: metav1.StatusFailure, Code: 403, Reason: metav1.StatusReasonForbidden})
		default:
			http.NotFound(w, r)
		}
	})
	c, err := NewClient(cfg, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	got, err := c.GetPVC(ctx, "ns", "data")
	if err != nil {
		t.Fatal(err)
	}
	want := &PVC{Namespace: "ns", Name: "data", StorageClassName: "fast", StorageClassSet: true, VolumeName: "pvc-1",
		RequestBytes: 10 << 30, AccessModes: []string{"ReadWriteOnce"}, Phase: "Bound"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GetPVC = %+v, want %+v", got, want)
	}

	got, err = c.GetPVC(ctx, "ns", "nc")
	if err != nil {
		t.Fatal(err)
	}
	if got.StorageClassSet || got.StorageClassName != "" || !got.Terminating || got.RequestBytes != 1<<30 {
		t.Fatalf("unset class, terminating: %+v", got)
	}
	got, err = c.GetPVC(ctx, "ns", "ec")
	if err != nil || !got.StorageClassSet || got.StorageClassName != "" {
		t.Fatalf("empty class must be set and empty: %+v, %v", got, err)
	}
	got, err = c.GetPVC(ctx, "ns", "huge")
	if err != nil || got.RequestBytes != math.MaxInt64 {
		t.Fatalf("a request beyond int64 saturates: %+v, %v", got, err)
	}
	if _, err := c.GetPVC(ctx, "ns", "bq"); err == nil {
		t.Fatal("a request that is not a quantity must be an error")
	}

	if _, err := c.GetPVC(ctx, "ns", "gone"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("API server NotFound: %v, want ErrNotFound", err)
	}
	for _, name := range []string{"other", "page", "denied", "missing-route"} {
		_, err := c.GetPVC(ctx, "ns", name)
		if err == nil || errors.Is(err, ErrNotFound) {
			t.Errorf("%s: %v, want an error that is not ErrNotFound", name, err)
		}
		if err != nil && strings.Contains(err.Error(), "http") {
			t.Errorf("%s: the error leaks an address: %v", name, err)
		}
	}
	if _, err := c.GetPVC(ctx, "", "x"); err == nil {
		t.Fatal("empty namespace must be an error")
	}
}

func TestClaimsStorageClasses(t *testing.T) {
	t.Parallel()
	def := map[string]any{annotationDefaultClass: "true"}
	var items []any
	cfg := apiServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/apis/storage.k8s.io/v1/storageclasses":
			writeJSON(w, 200, map[string]any{"apiVersion": "storage.k8s.io/v1", "kind": "StorageClassList", "items": items})
		case "/apis/storage.k8s.io/v1/storageclasses/fast":
			writeJSON(w, 200, class("fast", nil, "2026-01-01T00:00:00Z", "miami.csi.example", map[string]any{"cacheSize": "8Gi"}))
		case "/apis/storage.k8s.io/v1/storageclasses/gone":
			writeJSON(w, 404, notFound("storageclasses", "gone"))
		default:
			http.NotFound(w, r)
		}
	})
	c, err := NewClient(cfg, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	got, err := c.GetStorageClass(ctx, "fast")
	if err != nil || got.Name != "fast" || got.Provisioner != "miami.csi.example" || got.Parameters["cacheSize"] != "8Gi" {
		t.Fatalf("GetStorageClass = %+v, %v", got, err)
	}
	if _, err := c.GetStorageClass(ctx, "gone"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("NotFound: %v", err)
	}
	if _, err := c.GetStorageClass(ctx, "other"); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("a bare 404 is not ErrNotFound: %v", err)
	}

	// No classes, and classes with no default: nil, nil.
	if got, err := c.DefaultStorageClass(ctx); got != nil || err != nil {
		t.Fatalf("no classes: %v, %v", got, err)
	}
	items = []any{class("a", nil, "2026-01-01T00:00:00Z", "p", nil), class("b", map[string]any{annotationDefaultClass: "false"}, "2026-01-02T00:00:00Z", "p", nil)}
	if got, err := c.DefaultStorageClass(ctx); got != nil || err != nil {
		t.Fatalf("no default: %v, %v", got, err)
	}
	// One default; the beta annotation counts.
	items = []any{class("a", nil, "2026-01-01T00:00:00Z", "p", nil), class("b", def, "2026-01-02T00:00:00Z", "p", nil)}
	if got, err := c.DefaultStorageClass(ctx); err != nil || got == nil || got.Name != "b" {
		t.Fatalf("one default: %+v, %v", got, err)
	}
	items = []any{class("beta", map[string]any{annotationDefaultClassBeta: "true"}, "2026-01-02T00:00:00Z", "p", nil)}
	if got, err := c.DefaultStorageClass(ctx); err != nil || got == nil || got.Name != "beta" {
		t.Fatalf("beta default: %+v, %v", got, err)
	}
	// Two defaults: the newest wins, whatever the list order; a tie goes to the lower name.
	items = []any{class("old", def, "2026-01-01T00:00:00Z", "p", nil), class("new", def, "2026-03-01T00:00:00Z", "p", nil), class("mid", def, "2026-02-01T00:00:00Z", "p", nil)}
	if got, err := c.DefaultStorageClass(ctx); err != nil || got == nil || got.Name != "new" {
		t.Fatalf("two defaults: %+v, %v", got, err)
	}
	items = []any{class("zed", def, "2026-03-01T00:00:00Z", "p", nil), class("abe", def, "2026-03-01T00:00:00Z", "p", nil)}
	if got, err := c.DefaultStorageClass(ctx); err != nil || got == nil || got.Name != "abe" {
		t.Fatalf("tied defaults: %+v, %v", got, err)
	}
}

func TestClaimsPVCapacity(t *testing.T) {
	t.Parallel()
	pv := func(storage any) map[string]any {
		spec := map[string]any{"csi": map[string]any{"driver": "d", "volumeHandle": "h"}}
		if storage != nil {
			spec["capacity"] = map[string]any{"storage": storage}
		}
		return map[string]any{"apiVersion": "v1", "kind": "PersistentVolume", "metadata": map[string]any{"name": "pv"}, "spec": spec}
	}
	for _, tc := range []struct {
		name    string
		storage any
		want    int64
		wantErr bool
	}{
		{"10Gi", "10Gi", 10 << 30, false},
		{"absent", nil, 0, false},
		{"not a quantity", "lots", 0, true},
		{"wrong type", 7, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decodePV(toUnstructured(pv(tc.storage)))
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v", err)
			}
			if err == nil && got.CapacityBytes != tc.want {
				t.Fatalf("CapacityBytes = %d, want %d", got.CapacityBytes, tc.want)
			}
		})
	}
}

func toUnstructured(o map[string]any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: o}
}
