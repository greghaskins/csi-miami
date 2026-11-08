package webhook

import (
	"context"
	"errors"
	"slices"

	corev1 "k8s.io/api/core/v1"

	"github.com/greghaskins/csi-miami/internal/csi"
	"github.com/greghaskins/csi-miami/internal/kube"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// Claims is what the webhook reads from the cluster; *kube.Client implements
// it. A missing object is an error wrapping kube.ErrNotFound; any other error
// means the answer is unknown.
type Claims interface {
	GetPVC(ctx context.Context, namespace, name string) (*kube.PVC, error)
	GetPV(ctx context.Context, name string) (*kube.PV, error)
	GetStorageClass(ctx context.Context, name string) (*kube.StorageClass, error)
	// DefaultStorageClass is nil, nil when the cluster has none.
	DefaultStorageClass(ctx context.Context) (*kube.StorageClass, error)
}

var _ Claims = (*kube.Client)(nil)

// resolver works out the footprint of one pod's volumes. Class lookups are
// remembered for the one pod, so a pod with many claims of one class asks once.
type resolver struct {
	m       *Mutator
	ns      string
	classes map[string]*kube.StorageClass // by name; "" is the default class
	known   map[string]bool
}

// footprint is the summed footprint of the pod's volumes. A volume that is not
// ours, or that cannot be accounted for, adds nothing; only a lookup error
// whose answer is unknown is returned.
func (r *resolver) footprint(ctx context.Context, pod *corev1.Pod) (int64, error) {
	var sum int64
	seen := map[string]bool{}
	for _, v := range pod.Spec.Volumes {
		var n int64
		var err error
		switch {
		case v.PersistentVolumeClaim != nil:
			name := v.PersistentVolumeClaim.ClaimName
			if seen[name] {
				continue // one claim, one cache, however many mounts
			}
			seen[name] = true
			n, err = r.fromClaim(ctx, name)
		case v.Ephemeral != nil && v.Ephemeral.VolumeClaimTemplate != nil:
			n, err = r.fromTemplate(ctx, pod, v)
		}
		if err != nil {
			return 0, err
		}
		sum = volume.AddFootprint(sum, n)
	}
	return sum, nil
}

// unresolved records a volume the webhook could not account for, and gives it
// no footprint.
func (r *resolver) unresolved(what string) (int64, error) {
	r.m.Metrics.Unresolved()
	r.m.log().Info("a volume could not be accounted for, so the pod is admitted without it", "namespace", r.ns, "reason", what)
	return 0, nil
}

func (r *resolver) fromClaim(ctx context.Context, name string) (int64, error) {
	pvc, err := r.m.Claims.GetPVC(ctx, r.ns, name)
	if errors.Is(err, kube.ErrNotFound) {
		return r.unresolved("the claim does not exist")
	}
	if err != nil {
		return 0, err
	}
	if pvc.Terminating || pvc.Phase == "Lost" || manyNode(pvc.AccessModes) {
		return 0, nil
	}
	if pvc.VolumeName != "" && pvc.Phase == "Bound" {
		return r.fromPV(ctx, pvc.VolumeName)
	}
	return r.fromClass(ctx, pvc.StorageClassName, pvc.StorageClassSet, pvc.RequestBytes, r.ns, name)
}

func (r *resolver) fromTemplate(ctx context.Context, pod *corev1.Pod, v corev1.Volume) (int64, error) {
	spec := v.Ephemeral.VolumeClaimTemplate.Spec
	modes := make([]string, len(spec.AccessModes))
	for i, m := range spec.AccessModes {
		modes[i] = string(m)
	}
	if manyNode(modes) {
		return 0, nil
	}
	var class string
	if spec.StorageClassName != nil {
		class = *spec.StorageClassName
	}
	// The claim's name is not known before the pod is: the controller names it
	// <pod>-<volume>. Only the subdir template's tokens use it, never the size.
	base := pod.Name
	if base == "" {
		base = pod.GenerateName
	}
	claim := v.Name
	if base != "" {
		claim = base + "-" + v.Name
	}
	return r.fromClass(ctx, class, spec.StorageClassName != nil, spec.Resources.Requests.Storage().Value(), r.ns, claim)
}

// fromPV is the footprint of a bound volume: the PV's own context and its
// served capacity.
func (r *resolver) fromPV(ctx context.Context, name string) (int64, error) {
	pv, err := r.m.Claims.GetPV(ctx, name)
	if errors.Is(err, kube.ErrNotFound) {
		return r.unresolved("the bound volume does not exist")
	}
	if err != nil {
		return 0, err
	}
	if pv.Driver != csi.DriverName {
		return 0, nil
	}
	c, err := volume.DecodeContext(pv.VolumeAttributes)
	if err != nil {
		return r.unresolved("the volume's context is not usable")
	}
	if c.Kind != volume.KindVolume {
		return 0, nil
	}
	// The same rule the node's stage check uses for a volume that was grown.
	return volume.ServedFootprint(*c.Volume, pv.CapacityBytes), nil
}

// fromClass is the footprint of a claim with no volume yet: the class's
// parameters and the request, through the same WithSize the controller uses
// at CreateVolume. Nothing on the claim overrides a class parameter.
func (r *resolver) fromClass(ctx context.Context, name string, set bool, request int64, claimNS, claimName string) (int64, error) {
	if set && name == "" {
		return 0, nil // an explicit empty class: no dynamic provisioning
	}
	class, err := r.class(ctx, name)
	if errors.Is(err, kube.ErrNotFound) {
		return r.unresolved("the class does not exist")
	}
	if err != nil {
		return 0, err
	}
	if class == nil || class.Provisioner != csi.DriverName {
		return 0, nil
	}
	// The provisioner adds the claim's name and namespace to the parameters
	// (they fill the subdir template); so does this, over any value the class
	// carries under those keys.
	p := make(map[string]string, len(class.Parameters)+2)
	for k, v := range class.Parameters {
		p[k] = v
	}
	p["csi.storage.k8s.io/pvc/name"] = claimName
	p["csi.storage.k8s.io/pvc/namespace"] = claimNS
	c, err := volume.NewContext(p)
	if err != nil {
		return r.unresolved("the class's parameters are not usable")
	}
	if c.Kind != volume.KindVolume {
		return 0, nil
	}
	if c, err = c.WithSize(request); err != nil {
		return r.unresolved("the claim's request is not usable")
	}
	return volume.CacheFootprint(*c.Volume), nil
}

// class returns the named class, or the cluster default for "", remembering
// the answer for the pod. The default may be nil.
func (r *resolver) class(ctx context.Context, name string) (*kube.StorageClass, error) {
	if r.known[name] {
		return r.classes[name], nil
	}
	var c *kube.StorageClass
	var err error
	if name == "" {
		c, err = r.m.Claims.DefaultStorageClass(ctx)
	} else {
		c, err = r.m.Claims.GetStorageClass(ctx, name)
	}
	if err != nil {
		return nil, err
	}
	r.classes[name], r.known[name] = c, true
	return c, nil
}

// manyNode reports whether the modes include one that lets several nodes
// mount the volume: out of scope (design 4.7).
func manyNode(modes []string) bool {
	return slices.Contains(modes, string(corev1.ReadWriteMany)) || slices.Contains(modes, string(corev1.ReadOnlyMany))
}
