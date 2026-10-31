// Package fake is a kube.PVReader for other packages' tests. It holds PVs in
// memory, records every lookup, and returns errors configured per name.
package fake

import (
	"context"
	"fmt"
	"sync"

	corev1 "k8s.io/api/core/v1"

	"github.com/greghaskins/csi-miami/internal/kube"
)

// Reader implements kube.PVReader. The zero value is ready to use and safe for
// concurrent use.
type Reader struct {
	mu    sync.Mutex
	pvs   map[string]*corev1.PersistentVolume
	errs  map[string]error
	calls []string
}

var _ kube.PVReader = (*Reader)(nil)

// Add stores a deep copy of pv under its name, replacing any PV of that name.
func (r *Reader) Add(pv *corev1.PersistentVolume) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pvs == nil {
		r.pvs = map[string]*corev1.PersistentVolume{}
	}
	r.pvs[pv.Name] = pv.DeepCopy()
}

// Remove forgets the PV with the given name.
func (r *Reader) Remove(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.pvs, name)
}

// FailOn makes GetPV(name) return err, even if the PV exists, until cleared
// with a nil err.
func (r *Reader) FailOn(name string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err == nil {
		delete(r.errs, name)
		return
	}
	if r.errs == nil {
		r.errs = map[string]error{}
	}
	r.errs[name] = err
}

// Calls returns the names looked up so far, in order.
func (r *Reader) Calls() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

// GetPV returns a deep copy of the stored PV, the configured error, or an
// error wrapping kube.ErrNotFound. It honours a context that is already done.
func (r *Reader) GetPV(ctx context.Context, name string) (*corev1.PersistentVolume, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, name)
	if err, ok := r.errs[name]; ok {
		return nil, err
	}
	pv, ok := r.pvs[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", kube.ErrNotFound, name)
	}
	return pv.DeepCopy(), nil
}
