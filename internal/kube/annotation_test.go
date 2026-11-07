package kube

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestPVAnnotation(t *testing.T) {
	t.Parallel()
	cfg := apiServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/persistentvolumes/pvc-1" {
			writeJSON(w, 404, metav1Status(404, "NotFound", "persistentvolumes", strings.TrimPrefix(r.URL.Path, "/api/v1/persistentvolumes/")))
			return
		}
		writeJSON(w, 200, map[string]any{
			"apiVersion": "v1", "kind": "PersistentVolume",
			"metadata": map[string]any{"name": "pvc-1", "annotations": map[string]any{"miami.csi.greghaskins.com/publish": "7"}},
		})
	})
	c, err := NewClient(cfg, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if got, err := c.PVAnnotation(ctx, "pvc-1", "miami.csi.greghaskins.com/publish"); err != nil || got != "7" {
		t.Errorf("PVAnnotation = %q, %v; want 7", got, err)
	}
	if got, err := c.PVAnnotation(ctx, "pvc-1", "absent"); err != nil || got != "" {
		t.Errorf("PVAnnotation(absent) = %q, %v; want empty", got, err)
	}
	if _, err := c.PVAnnotation(ctx, "pvc-2", "x"); !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}
