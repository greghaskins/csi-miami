package deploy

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/kustomize/api/krusty"
	"sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/kustomize/kyaml/filesys"
	"sigs.k8s.io/yaml"

	"github.com/greghaskins/csi-miami/internal/volume"
)

var update = flag.Bool("update", false, "rewrite testdata/base.golden.yaml")

const (
	driverName  = "miami.csi.greghaskins.com"
	driverImage = "ghcr.io/greghaskins/csi-miami"
	kubeletDir  = "/var/lib/kubelet"
)

// object is one rendered resource.
type object struct {
	kind, name string
	json       []byte
}

// rendered is the output of a kustomization.
type rendered struct {
	objects []object
	yaml    []byte
}

// kustomizeMu serialises renders: kustomize initialises its OpenAPI schema
// lazily into package state, and two parallel tests rendering at once race on
// it (the race detector reports it).
var kustomizeMu sync.Mutex

func render(t *testing.T, dir string) rendered {
	t.Helper()
	k := krusty.MakeKustomizer(krusty.MakeDefaultOptions())
	kustomizeMu.Lock()
	defer kustomizeMu.Unlock()
	m, err := k.Run(filesys.MakeFsOnDisk(), dir)
	if err != nil {
		t.Fatalf("render %s: %v", dir, err)
	}
	out, err := m.AsYaml()
	if err != nil {
		t.Fatalf("render %s: %v", dir, err)
	}
	r := rendered{yaml: out}
	for _, res := range m.Resources() {
		js, err := res.MarshalJSON()
		if err != nil {
			t.Fatalf("render %s: %v", dir, err)
		}
		r.objects = append(r.objects, object{kind: res.GetKind(), name: res.GetName(), json: js})
	}
	return r
}

// get decodes the one object of the kind and name into out, strictly.
func (r rendered) get(t *testing.T, kind, name string, out any) {
	t.Helper()
	var found []object
	for _, o := range r.objects {
		if o.kind == kind && o.name == name {
			found = append(found, o)
		}
	}
	if len(found) != 1 {
		t.Fatalf("want exactly one %s %q, found %d", kind, name, len(found))
	}
	if err := yaml.UnmarshalStrict(found[0].json, out); err != nil {
		t.Fatalf("decode %s %q: %v", kind, name, err)
	}
}

func (r rendered) names(kind string) []string {
	var n []string
	for _, o := range r.objects {
		if o.kind == kind {
			n = append(n, o.name)
		}
	}
	sort.Strings(n)
	return n
}

func base(t *testing.T) rendered    { t.Helper(); return render(t, "kustomize/base") }
func overlay(t *testing.T) rendered { t.Helper(); return render(t, "kustomize/overlays/e2e") }

func controller(t *testing.T, r rendered) appsv1.Deployment {
	t.Helper()
	var d appsv1.Deployment
	r.get(t, "Deployment", "miami-controller", &d)
	return d
}

func node(t *testing.T, r rendered) appsv1.DaemonSet {
	t.Helper()
	var d appsv1.DaemonSet
	r.get(t, "DaemonSet", "miami-node", &d)
	return d
}

func container(t *testing.T, spec corev1.PodSpec, name string) corev1.Container {
	t.Helper()
	for _, c := range spec.Containers {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("no container %q", name)
	return corev1.Container{}
}

// flagValue returns the value of --name=value among args.
func flagValue(t *testing.T, args []string, name string) string {
	t.Helper()
	prefix := "--" + name + "="
	var vals []string
	for _, a := range args {
		if strings.HasPrefix(a, prefix) {
			vals = append(vals, strings.TrimPrefix(a, prefix))
		}
	}
	if len(vals) != 1 {
		t.Fatalf("want exactly one %s in %v, found %d", prefix, args, len(vals))
	}
	return vals[0]
}

func hasArg(args []string, a string) bool {
	for _, x := range args {
		if x == a {
			return true
		}
	}
	return false
}

// ruleSet flattens rules to "group/resource:verb" strings.
func ruleSet(rules []rbacv1.PolicyRule) map[string]bool {
	set := map[string]bool{}
	for _, r := range rules {
		for _, g := range r.APIGroups {
			for _, res := range r.Resources {
				for _, v := range r.Verbs {
					set[g+"/"+res+":"+v] = true
				}
			}
		}
	}
	return set
}

type rule struct {
	group, resource string
	verbs           []string
}

// The generation timers read the publish annotation of a PV with a get, and
// that is all the node may do to PVs: no list or watch (it asks for one
// by name) and no write (an annotation is the operator's request, never the
// node's).
func TestNodeRoleReadsPVAnnotationsOnly(t *testing.T) {
	t.Parallel()
	var cr rbacv1.ClusterRole
	base(t).get(t, "ClusterRole", "miami-node", &cr)
	got := ruleSet(cr.Rules)
	if !got["/persistentvolumes:get"] {
		t.Error("the node cannot get PVs, so it cannot read the publish annotation")
	}
	for _, verb := range []string{"list", "watch", "create", "update", "patch", "delete", "*"} {
		if got["/persistentvolumes:"+verb] {
			t.Errorf("the node may %s PVs", verb)
		}
	}
}

func TestRBACRuleSets(t *testing.T) {
	t.Parallel()
	// The lists below are plan section 12. A change to either side must change
	// both.
	tests := []struct {
		role  string
		rules []rule
	}{
		{"miami-controller", []rule{
			{"", "persistentvolumes", []string{"get", "list", "watch", "create", "patch", "delete"}},
			{"", "persistentvolumeclaims", []string{"get", "list", "watch", "update"}},
			// csi-resizer's only grant the others do not cover.
			{"", "persistentvolumeclaims/status", []string{"patch"}},
			{"storage.k8s.io", "storageclasses", []string{"get", "list", "watch"}},
			{"storage.k8s.io", "volumeattachments", []string{"get", "list", "watch", "patch"}},
			{"storage.k8s.io", "volumeattachments/status", []string{"patch"}},
			{"", "nodes", []string{"get", "list", "watch"}},
			{"storage.k8s.io", "csinodes", []string{"get", "list", "watch"}},
			{"", "events", []string{"list", "watch", "create", "update", "patch"}},
			// The webhook's reads (PVCs, PVs, StorageClasses) are covered above;
			// this is its one write, narrowed to one name below.
			{"admissionregistration.k8s.io", "mutatingwebhookconfigurations", []string{"get", "patch"}},
		}},
		{"miami-node", []rule{
			// The events' PV lookup, and the emergency detach's annotation read.
			{"", "persistentvolumes", []string{"get"}},
			// The taint of a holder's node (decision 52): read, never written.
			{"", "nodes", []string{"get"}},
			// The node's own cache capacity (decision 66), narrowed by the
			// admission policy miami-node-capacity.
			{"", "nodes/status", []string{"patch"}},
		}},
	}
	r := base(t)
	for _, tc := range tests {
		t.Run(tc.role, func(t *testing.T) {
			t.Parallel()
			var cr rbacv1.ClusterRole
			r.get(t, "ClusterRole", tc.role, &cr)
			want := map[string]bool{}
			for _, ru := range tc.rules {
				for _, v := range ru.verbs {
					want[ru.group+"/"+ru.resource+":"+v] = true
				}
			}
			got := ruleSet(cr.Rules)
			for k := range want {
				if !got[k] {
					t.Errorf("missing %s", k)
				}
			}
			for k := range got {
				if !want[k] {
					t.Errorf("extra %s", k)
				}
			}
			for _, ru := range cr.Rules {
				if len(ru.NonResourceURLs) > 0 {
					t.Errorf("unexpected nonResourceURLs in %+v", ru)
				}
				// Only the webhook configuration rule is limited by name, and
				// it must be.
				named := len(ru.ResourceNames) > 0
				isMWC := len(ru.Resources) == 1 && ru.Resources[0] == "mutatingwebhookconfigurations"
				if named != isMWC {
					t.Errorf("resourceNames on the wrong rules: %+v", ru)
				}
				for _, res := range ru.Resources {
					if res == "secrets" || res == "*" {
						t.Errorf("%s grants %s cluster-wide", tc.role, res)
					}
				}
			}

			var crb rbacv1.ClusterRoleBinding
			r.get(t, "ClusterRoleBinding", tc.role, &crb)
			if crb.RoleRef.Kind != "ClusterRole" || crb.RoleRef.Name != tc.role {
				t.Errorf("binding refers to %v", crb.RoleRef)
			}
			wantSubject := rbacv1.Subject{Kind: "ServiceAccount", Name: tc.role, Namespace: "miami-system"}
			if len(crb.Subjects) != 1 || crb.Subjects[0] != wantSubject {
				t.Errorf("binding subjects = %v, want [%v]", crb.Subjects, wantSubject)
			}
			var sa corev1.ServiceAccount
			r.get(t, "ServiceAccount", tc.role, &sa)
		})
	}
}

// TestControllerLeaseRole: leader election needs Leases in the driver's own
// namespace and nowhere else, so the grant is a namespaced Role and no
// ClusterRole carries a lease rule.
func TestControllerLeaseRole(t *testing.T) {
	t.Parallel()
	r := base(t)
	var role rbacv1.Role
	r.get(t, "Role", "miami-controller", &role)
	if role.Namespace != "miami-system" {
		t.Errorf("role namespace = %q, want miami-system", role.Namespace)
	}
	// The Role is exact: leases, and the webhook's Secret. Get and update
	// are limited to miami-webhook-tls; create cannot be limited by name, so
	// it is the one namespace-wide verb (the review checklist names it).
	wantRules := []rbacv1.PolicyRule{
		{APIGroups: []string{"coordination.k8s.io"}, Resources: []string{"leases"}, Verbs: []string{"get", "create", "update", "delete"}},
		{APIGroups: []string{""}, Resources: []string{"secrets"}, ResourceNames: []string{"miami-webhook-tls"}, Verbs: []string{"get", "update"}},
		{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"create"}},
	}
	if !reflect.DeepEqual(role.Rules, wantRules) {
		t.Errorf("role rules = %+v, want %+v", role.Rules, wantRules)
	}

	var rb rbacv1.RoleBinding
	r.get(t, "RoleBinding", "miami-controller", &rb)
	if rb.Namespace != "miami-system" {
		t.Errorf("binding namespace = %q, want miami-system", rb.Namespace)
	}
	if rb.RoleRef.Kind != "Role" || rb.RoleRef.Name != "miami-controller" {
		t.Errorf("binding refers to %v", rb.RoleRef)
	}
	wantSubject := rbacv1.Subject{Kind: "ServiceAccount", Name: "miami-controller", Namespace: "miami-system"}
	if len(rb.Subjects) != 1 || rb.Subjects[0] != wantSubject {
		t.Errorf("binding subjects = %v, want [%v]", rb.Subjects, wantSubject)
	}

	for _, o := range r.objects {
		if o.kind != "ClusterRole" {
			continue
		}
		var cr rbacv1.ClusterRole
		r.get(t, "ClusterRole", o.name, &cr)
		for _, ru := range cr.Rules {
			for _, res := range ru.Resources {
				if res == "leases" || res == "secrets" {
					t.Errorf("ClusterRole %s grants %s cluster-wide", o.name, res)
				}
			}
		}
	}
}

// TestNodeEventsRole: the node's recorder writes events to the default
// namespace only, so the grant is a namespaced Role there; the only
// cluster-wide grant the node holds is the PV read (a PV is cluster-scoped),
// and no ClusterRole grants events to the node.
func TestNodeEventsRole(t *testing.T) {
	t.Parallel()
	r := base(t)
	var role rbacv1.Role
	r.get(t, "Role", "miami-node", &role)
	if role.Namespace != "default" {
		t.Errorf("role namespace = %q, want default", role.Namespace)
	}
	want := map[string]bool{}
	for _, v := range []string{"get", "create", "patch"} {
		want["/events:"+v] = true
	}
	if got := ruleSet(role.Rules); !reflect.DeepEqual(got, want) {
		t.Errorf("role grants %v, want %v", got, want)
	}
	for _, ru := range role.Rules {
		if len(ru.ResourceNames) > 0 || len(ru.NonResourceURLs) > 0 {
			t.Errorf("unexpected resourceNames or nonResourceURLs in %+v", ru)
		}
	}

	var rb rbacv1.RoleBinding
	r.get(t, "RoleBinding", "miami-node", &rb)
	if rb.Namespace != "default" {
		t.Errorf("binding namespace = %q, want default", rb.Namespace)
	}
	if rb.RoleRef.Kind != "Role" || rb.RoleRef.Name != "miami-node" {
		t.Errorf("binding refers to %v", rb.RoleRef)
	}
	wantSubject := rbacv1.Subject{Kind: "ServiceAccount", Name: "miami-node", Namespace: "miami-system"}
	if len(rb.Subjects) != 1 || rb.Subjects[0] != wantSubject {
		t.Errorf("binding subjects = %v, want [%v]", rb.Subjects, wantSubject)
	}

	var crb rbacv1.ClusterRoleBinding
	r.get(t, "ClusterRoleBinding", "miami-node", &crb)
	var cr rbacv1.ClusterRole
	r.get(t, "ClusterRole", crb.RoleRef.Name, &cr)
	for _, ru := range cr.Rules {
		for _, res := range ru.Resources {
			if res == "events" || res == "*" {
				t.Errorf("ClusterRole %s, bound to the node, grants %s cluster-wide", cr.Name, res)
			}
		}
	}
}

func TestCSIDriver(t *testing.T) {
	t.Parallel()
	var d storagev1.CSIDriver
	base(t).get(t, "CSIDriver", driverName, &d)
	if d.Spec.AttachRequired == nil || !*d.Spec.AttachRequired {
		t.Error("attachRequired must be true")
	}
	if d.Spec.PodInfoOnMount == nil || *d.Spec.PodInfoOnMount {
		t.Error("podInfoOnMount must be false")
	}
	if d.Spec.FSGroupPolicy == nil || *d.Spec.FSGroupPolicy != storagev1.ReadWriteOnceWithFSTypeFSGroupPolicy {
		t.Errorf("fsGroupPolicy = %v, want ReadWriteOnceWithFSType", d.Spec.FSGroupPolicy)
	}
	if !reflect.DeepEqual(d.Spec.VolumeLifecycleModes, []storagev1.VolumeLifecycleMode{storagev1.VolumeLifecyclePersistent}) {
		t.Errorf("volumeLifecycleModes = %v, want [Persistent]", d.Spec.VolumeLifecycleModes)
	}
	if d.Spec.RequiresRepublish == nil || *d.Spec.RequiresRepublish {
		t.Error("requiresRepublish must be false")
	}
	if d.Spec.StorageCapacity == nil || *d.Spec.StorageCapacity {
		t.Error("storageCapacity must be false")
	}
}

// TestNamespaceLabels: the namespace sets the enforce level and its version
// and nothing else; warn and audit are the cluster's choice.
func TestNamespaceLabels(t *testing.T) {
	t.Parallel()
	var ns corev1.Namespace
	base(t).get(t, "Namespace", "miami-system", &ns)
	want := map[string]string{
		"pod-security.kubernetes.io/enforce":         "privileged",
		"pod-security.kubernetes.io/enforce-version": "latest",
	}
	if !reflect.DeepEqual(ns.Labels, want) {
		t.Errorf("namespace labels = %v, want %v", ns.Labels, want)
	}
}

func TestBaseCreatesNoStorageClass(t *testing.T) {
	t.Parallel()
	if n := base(t).names("StorageClass"); len(n) != 0 {
		t.Errorf("the base creates StorageClasses %v; they belong in examples", n)
	}
}

// checkNoControlPlaneAffinity asserts the required node affinity of decision
// 35: one term whose expressions exclude the control-plane and master roles.
func checkNoControlPlaneAffinity(t *testing.T, a *corev1.Affinity) {
	t.Helper()
	if a == nil || a.NodeAffinity == nil || a.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		t.Errorf("affinity = %v, want a required node affinity", a)
		return
	}
	want := []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{
		{Key: "node-role.kubernetes.io/control-plane", Operator: corev1.NodeSelectorOpDoesNotExist},
		{Key: "node-role.kubernetes.io/master", Operator: corev1.NodeSelectorOpDoesNotExist},
	}}}
	if got := a.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms; !reflect.DeepEqual(got, want) {
		t.Errorf("node selector terms = %v, want %v", got, want)
	}
	if len(a.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution) != 0 || a.PodAffinity != nil || a.PodAntiAffinity != nil {
		t.Errorf("affinity = %v, want only the required node affinity", a)
	}
}

func TestNodeDaemonSet(t *testing.T) {
	t.Parallel()
	ds := node(t, base(t))
	spec := ds.Spec.Template.Spec

	if ds.Spec.UpdateStrategy.Type != appsv1.RollingUpdateDaemonSetStrategyType ||
		ds.Spec.UpdateStrategy.RollingUpdate == nil ||
		ds.Spec.UpdateStrategy.RollingUpdate.MaxUnavailable == nil ||
		ds.Spec.UpdateStrategy.RollingUpdate.MaxUnavailable.String() != "1" {
		t.Errorf("updateStrategy = %+v, want RollingUpdate with maxUnavailable 1", ds.Spec.UpdateStrategy)
	}
	if !spec.HostNetwork || spec.DNSPolicy != corev1.DNSClusterFirstWithHostNet {
		t.Errorf("hostNetwork=%v dnsPolicy=%v", spec.HostNetwork, spec.DNSPolicy)
	}
	if spec.HostPID || spec.HostIPC {
		t.Error("hostPID and hostIPC must be off")
	}
	if spec.ServiceAccountName != "miami-node" || spec.AutomountServiceAccountToken == nil || !*spec.AutomountServiceAccountToken {
		t.Errorf("service account %q automount %v", spec.ServiceAccountName, spec.AutomountServiceAccountToken)
	}
	if spec.PriorityClassName != "system-node-critical" {
		t.Errorf("priorityClassName = %q", spec.PriorityClassName)
	}
	if !reflect.DeepEqual(spec.Tolerations, []corev1.Toleration{{Operator: corev1.TolerationOpExists}}) {
		t.Errorf("tolerations = %v, want to tolerate everything", spec.Tolerations)
	}
	checkNoControlPlaneAffinity(t, spec.Affinity)

	// Privileged on the miami container only; read-only root everywhere.
	for _, c := range spec.Containers {
		sc := c.SecurityContext
		if sc == nil {
			t.Errorf("%s has no securityContext", c.Name)
			continue
		}
		priv := sc.Privileged != nil && *sc.Privileged
		if priv != (c.Name == "miami") {
			t.Errorf("%s: privileged = %v", c.Name, priv)
		}
		if sc.ReadOnlyRootFilesystem == nil || !*sc.ReadOnlyRootFilesystem {
			t.Errorf("%s: readOnlyRootFilesystem must be true", c.Name)
		}
		if sc.SeccompProfile == nil || sc.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
			t.Errorf("%s: seccompProfile must be RuntimeDefault", c.Name)
		}
		if sc.RunAsUser == nil || *sc.RunAsUser != 0 {
			t.Errorf("%s: runAsUser must be 0", c.Name)
		}
		if c.Name == "miami" {
			// Privileged already implies every capability and no AppArmor
			// confinement (the container runtime runs it unconfined), and the
			// API rejects allowPrivilegeEscalation: false beside it. So the
			// manifest sets none of the three, and none may appear.
			if sc.Capabilities != nil {
				t.Errorf("miami: capabilities = %+v, want none (it is privileged)", sc.Capabilities)
			}
			if sc.AllowPrivilegeEscalation != nil && !*sc.AllowPrivilegeEscalation {
				t.Error("miami: allowPrivilegeEscalation false is invalid beside privileged")
			}
			if sc.AppArmorProfile != nil && sc.AppArmorProfile.Type != corev1.AppArmorProfileTypeUnconfined {
				t.Errorf("miami: appArmorProfile = %+v, want unset or Unconfined", sc.AppArmorProfile)
			}
			continue
		}
		// The sidecars share the pod with a privileged container and a
		// root-owned kubelet subPath; they get nothing.
		if sc.AllowPrivilegeEscalation == nil || *sc.AllowPrivilegeEscalation {
			t.Errorf("%s: allowPrivilegeEscalation must be false", c.Name)
		}
		if sc.AppArmorProfile != nil {
			t.Errorf("%s: appArmorProfile = %+v, want the runtime default", c.Name, sc.AppArmorProfile)
		}
		if sc.Capabilities == nil || !reflect.DeepEqual(sc.Capabilities.Drop, []corev1.Capability{"ALL"}) {
			t.Errorf("%s: capabilities must drop ALL", c.Name)
		} else if len(sc.Capabilities.Add) != 0 {
			t.Errorf("%s: capabilities add = %v, want none", c.Name, sc.Capabilities.Add)
		}
	}
	names := []string{}
	for _, c := range spec.Containers {
		names = append(names, c.Name)
	}
	if want := []string{"miami", "csi-node-driver-registrar", "livenessprobe"}; !reflect.DeepEqual(names, want) {
		t.Errorf("containers = %v, want %v", names, want)
	}

	m := container(t, spec, "miami")
	if !hasArg(m.Args, "node") {
		t.Errorf("miami args = %v, want the node subcommand", m.Args)
	}
	if got := flagValue(t, m.Args, "node-id"); got != "$(NODE_NAME)" {
		t.Errorf("--node-id = %q", got)
	}
	var env *corev1.EnvVar
	for i := range m.Env {
		if m.Env[i].Name == "NODE_NAME" {
			env = &m.Env[i]
		}
	}
	if env == nil || env.ValueFrom == nil || env.ValueFrom.FieldRef == nil || env.ValueFrom.FieldRef.FieldPath != "spec.nodeName" {
		t.Errorf("NODE_NAME must come from the downward API field spec.nodeName, got %+v", env)
	}

	// The host mounts, and the one emptyDir, are held exactly by
	// TestNodeHostPaths.
	for _, v := range spec.Volumes {
		if v.HostPath == nil && v.Name != cryptsetupLock {
			t.Errorf("volume %s is not a hostPath; the node needs no other kind", v.Name)
		}
	}
	// The sidecars' mounts carry no propagation.
	for _, c := range spec.Containers {
		if c.Name == "miami" {
			continue
		}
		for _, vm := range c.VolumeMounts {
			if vm.MountPropagation != nil && *vm.MountPropagation != corev1.MountPropagationNone {
				t.Errorf("%s: mount %s has propagation %v", c.Name, vm.Name, *vm.MountPropagation)
			}
		}
	}
}

// expand applies Kubernetes' $(VAR) expansion for one variable.
func expand(s, name, value string) string {
	return strings.ReplaceAll(s, "$("+name+")", value)
}

// checkKubeletDir asserts that the one kubelet directory dir is what every
// field of the node DaemonSet that names it says: the fields Kubernetes cannot
// expand (the env values, the hostPath and the miami container's mount path) and every
// argument, which derives from $(KUBELET_DIR).
func checkKubeletDir(t *testing.T, spec corev1.PodSpec, dir string) {
	t.Helper()
	driverDir := dir + "/plugins/" + driverName
	sock := "unix://" + driverDir + "/csi.sock"
	wantArgs := map[string]map[string]string{
		"miami": {
			"endpoint":    sock,
			"kubelet-dir": dir,
		},
		"csi-node-driver-registrar": {
			"csi-address":               "/csi/csi.sock",
			"kubelet-registration-path": driverDir + "/csi.sock",
			"plugin-registration-path":  "/registration",
		},
		"livenessprobe": {
			"csi-address": "/csi/csi.sock",
		},
	}
	// Only the miami container mounts the whole kubelet directory; each
	// sidecar mounts just the subPaths it uses.
	type vm struct{ mountPath, subPath string }
	wantMounts := map[string][]vm{
		"miami":                     {{dir, ""}},
		"csi-node-driver-registrar": {{"/csi", "plugins/" + driverName}, {"/registration", "plugins_registry"}},
		"livenessprobe":             {{"/csi", "plugins/" + driverName}},
	}
	// Only the miami and registrar containers use $(KUBELET_DIR).
	wantEnv := map[string]bool{"miami": true, "csi-node-driver-registrar": true}
	for _, c := range spec.Containers {
		var vals []string
		for _, e := range c.Env {
			if e.Name == "KUBELET_DIR" {
				vals = append(vals, e.Value)
			}
		}
		switch {
		case wantEnv[c.Name] && (len(vals) != 1 || vals[0] != dir):
			t.Errorf("%s: KUBELET_DIR env = %v, want exactly [%s]", c.Name, vals, dir)
		case !wantEnv[c.Name] && len(vals) != 0:
			t.Errorf("%s: KUBELET_DIR env = %v, want none", c.Name, vals)
		}
		for _, a := range c.Args {
			if strings.Contains(a, "kubelet") && !strings.Contains(a, "$(KUBELET_DIR)") && !strings.HasPrefix(a, "--kubelet-") {
				t.Errorf("%s: argument %q names a kubelet path without deriving it from $(KUBELET_DIR)", c.Name, a)
			}
		}
		for name, want := range wantArgs[c.Name] {
			if got := expand(flagValue(t, c.Args, name), "KUBELET_DIR", dir); got != want {
				t.Errorf("%s: --%s expands to %q, want %q", c.Name, name, got, want)
			}
		}
		var mounts []vm
		for _, m := range c.VolumeMounts {
			if m.Name != "kubelet-dir" {
				continue // the volume kind's host mounts, TestNodeHostPaths
			}
			mounts = append(mounts, vm{m.MountPath, m.SubPath})
		}
		if !reflect.DeepEqual(mounts, wantMounts[c.Name]) {
			t.Errorf("%s: kubelet-dir mounts = %v, want %v", c.Name, mounts, wantMounts[c.Name])
		}
		if c.Name != "miami" {
			for _, m := range c.VolumeMounts {
				if m.MountPath == dir || m.SubPath == "" {
					t.Errorf("%s: mounts the kubelet root (%+v); a sidecar mounts only subPaths", c.Name, m)
				}
			}
		}
	}
	if v := volumeNamed(spec, "kubelet-dir"); v == nil || v.HostPath == nil || v.HostPath.Path != dir {
		t.Errorf("volume kubelet-dir = %+v, want the hostPath %s", v, dir)
	}
	// The node's defaults for the state, stage and transports roots live under
	// --kubelet-dir; none may be overridden.
	m := container(t, spec, "miami")
	for _, f := range []string{"state-dir", "stage-root", "transports-root"} {
		for _, a := range m.Args {
			if strings.HasPrefix(a, "--"+f+"=") {
				t.Errorf("--%s is set (%s); the default under the kubelet directory is the contract", f, a)
			}
		}
	}
	// No literal copy of the directory in any argument.
	for _, c := range spec.Containers {
		for _, a := range c.Args {
			if strings.Contains(a, dir) {
				t.Errorf("%s: argument %q holds the kubelet directory literally", c.Name, a)
			}
		}
	}
}

// TestKubeletDirAgreement checks that the base's one kubelet directory is what
// the env values, the hostPath, the mount paths and every argument say.
func TestKubeletDirAgreement(t *testing.T) {
	t.Parallel()
	checkKubeletDir(t, node(t, base(t)).Spec.Template.Spec, kubeletDir)
}

// replacementBlock returns the base's replacements entry as YAML for an
// overlay to repeat, read from the base so the test cannot drift from it. The
// documented snippet in the base kustomization is checked against it.
func replacementBlock(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("kustomize", "base", "kustomization.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var k struct {
		Replacements []any `json:"replacements"`
	}
	if err := yaml.Unmarshal(raw, &k); err != nil {
		t.Fatal(err)
	}
	out, err := yaml.Marshal(k.Replacements)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

// TestKubeletDirSnippetMatches asserts the overlay snippet documented in the
// base kustomization is the base's own replacement, so a reader who pastes it
// gets the one that is tested.
func TestKubeletDirSnippetMatches(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile(filepath.Join("kustomize", "base", "kustomization.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var snippet []string
	in := false
	for _, line := range strings.Split(string(raw), "\n") {
		if line == "#   replacements:" {
			in = true
		}
		if in {
			if !strings.HasPrefix(line, "#") || line == "#" {
				break
			}
			snippet = append(snippet, strings.TrimPrefix(strings.TrimPrefix(line, "#"), " "))
		}
	}
	if len(snippet) == 0 {
		t.Fatal("no documented replacements snippet in the base kustomization")
	}
	var doc, live struct {
		Replacements []any `json:"replacements"`
	}
	if err := yaml.Unmarshal([]byte(strings.Join(snippet, "\n")), &doc); err != nil {
		t.Fatalf("the documented snippet does not parse: %v", err)
	}
	if err := yaml.Unmarshal(raw, &live); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(doc.Replacements, live.Replacements) {
		t.Errorf("the documented replacements differ from the base's:\n doc  %v\n base %v", doc.Replacements, live.Replacements)
	}
}

// TestKubeletDirOverlay renders a throwaway overlay that sets the ConfigMap
// literal and nothing else, and asserts every field follows.
func TestKubeletDirOverlay(t *testing.T) {
	t.Parallel()
	const custom = "/var/lib/k0s/kubelet"
	dir := t.TempDir()
	baseAbs, err := filepath.Abs("kustomize/base")
	if err != nil {
		t.Fatal(err)
	}
	rel, err := filepath.Rel(dir, baseAbs)
	if err != nil {
		t.Fatal(err)
	}
	kz := `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ` + rel + `
configMapGenerator:
  - name: miami-deploy
    namespace: miami-system
    behavior: merge
    literals:
      - kubeletDir=` + custom + `
replacements:
` + replacementBlock(t)
	if err := os.WriteFile(filepath.Join(dir, "kustomization.yaml"), []byte(kz), 0o600); err != nil {
		t.Fatal(err)
	}
	checkKubeletDir(t, node(t, render(t, dir)).Spec.Template.Spec, custom)
}

func TestControllerDeployment(t *testing.T) {
	t.Parallel()
	d := controller(t, base(t))
	spec := d.Spec.Template.Spec
	checkNoControlPlaneAffinity(t, spec.Affinity)

	if d.Spec.Replicas == nil || *d.Spec.Replicas != 1 {
		t.Errorf("replicas = %v, want 1", d.Spec.Replicas)
	}
	if d.Spec.Strategy.Type != appsv1.RecreateDeploymentStrategyType {
		t.Errorf("strategy = %v, want Recreate", d.Spec.Strategy.Type)
	}
	if !spec.HostNetwork || spec.DNSPolicy != corev1.DNSClusterFirstWithHostNet {
		t.Errorf("hostNetwork=%v dnsPolicy=%v", spec.HostNetwork, spec.DNSPolicy)
	}
	if spec.HostPID || spec.HostIPC {
		t.Error("hostPID and hostIPC must be off")
	}
	if spec.ServiceAccountName != "miami-controller" || spec.AutomountServiceAccountToken == nil || !*spec.AutomountServiceAccountToken {
		t.Errorf("service account %q automount %v", spec.ServiceAccountName, spec.AutomountServiceAccountToken)
	}
	if len(spec.NodeSelector) != 0 {
		t.Error("the controller has no node selector")
	}

	var names []string
	for _, c := range spec.Containers {
		names = append(names, c.Name)
		sc := c.SecurityContext
		if sc == nil {
			t.Errorf("%s has no securityContext", c.Name)
			continue
		}
		if sc.Privileged != nil && *sc.Privileged {
			t.Errorf("%s is privileged", c.Name)
		}
		if sc.ReadOnlyRootFilesystem == nil || !*sc.ReadOnlyRootFilesystem {
			t.Errorf("%s: readOnlyRootFilesystem must be true", c.Name)
		}
		if sc.AllowPrivilegeEscalation == nil || *sc.AllowPrivilegeEscalation {
			t.Errorf("%s: allowPrivilegeEscalation must be false", c.Name)
		}
		if sc.SeccompProfile == nil || sc.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
			t.Errorf("%s: seccompProfile must be RuntimeDefault", c.Name)
		}
		if sc.RunAsUser == nil || *sc.RunAsUser != 0 {
			t.Errorf("%s: runAsUser must be 0", c.Name)
		}
		// containerd's default AppArmor profile denies mount(2); only the
		// container that mounts is unconfined.
		unconfined := sc.AppArmorProfile != nil && sc.AppArmorProfile.Type == corev1.AppArmorProfileTypeUnconfined
		if unconfined != (c.Name == "miami") {
			t.Errorf("%s: appArmorProfile = %+v, want Unconfined on miami only", c.Name, sc.AppArmorProfile)
		}
		if sc.Capabilities == nil || !reflect.DeepEqual(sc.Capabilities.Drop, []corev1.Capability{"ALL"}) {
			t.Errorf("%s: capabilities must drop ALL", c.Name)
			continue
		}
		var add []corev1.Capability
		if c.Name == "miami" {
			add = []corev1.Capability{"SYS_ADMIN"}
		}
		if !reflect.DeepEqual(sc.Capabilities.Add, add) {
			t.Errorf("%s: capabilities add = %v, want %v", c.Name, sc.Capabilities.Add, add)
		}
	}
	if want := []string{"miami", "csi-provisioner", "csi-attacher", "csi-resizer", "livenessprobe"}; !reflect.DeepEqual(names, want) {
		t.Errorf("containers = %v, want %v", names, want)
	}

	m := container(t, spec, "miami")
	if !hasArg(m.Args, "controller") {
		t.Errorf("miami args = %v, want the controller subcommand", m.Args)
	}
	if got := flagValue(t, m.Args, "endpoint"); got != "unix:///csi/csi.sock" {
		t.Errorf("--endpoint = %q", got)
	}
	if got := flagValue(t, m.Args, "transports-root"); got != "/var/lib/miami" {
		t.Errorf("--transports-root = %q", got)
	}
	for _, a := range []string{"--webhook-addr=:9814", "--webhook-config=miami-cache"} {
		if !hasArg(m.Args, a) {
			t.Errorf("miami lacks %s", a)
		}
	}
	// Self-signed is the base's mode: no --webhook-tls-secret.
	for _, a := range m.Args {
		if strings.HasPrefix(a, "--webhook-tls-secret") {
			t.Errorf("the base sets %s; only the cert-manager component does", a)
		}
	}
	wantEnv := []corev1.EnvVar{{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}}}
	if !reflect.DeepEqual(m.Env, wantEnv) {
		t.Errorf("miami env = %+v, want POD_NAMESPACE from the downward API", m.Env)
	}
	hasWebhookPort := false
	for _, cp := range m.Ports {
		if cp.Name == "webhook" && cp.ContainerPort == 9814 {
			hasWebhookPort = true
		}
	}
	if !hasWebhookPort {
		t.Errorf("miami ports = %+v, want webhook 9814", m.Ports)
	}
	p := container(t, spec, "csi-provisioner")
	for _, a := range []string{"--timeout=60s", "--extra-create-metadata", "--feature-gates=Topology=true", "--leader-election", "--csi-address=/csi/csi.sock"} {
		if !hasArg(p.Args, a) {
			t.Errorf("csi-provisioner lacks %s", a)
		}
	}
	a := container(t, spec, "csi-attacher")
	for _, x := range []string{"--leader-election", "--csi-address=/csi/csi.sock"} {
		if !hasArg(a.Args, x) {
			t.Errorf("csi-attacher lacks %s", x)
		}
	}

	r := container(t, spec, "csi-resizer")
	for _, x := range []string{"--leader-election", "--csi-address=/csi/csi.sock", "--handle-volume-inuse-error=false", "--feature-gates=VolumeAttributesClass=false"} {
		if !hasArg(r.Args, x) {
			t.Errorf("csi-resizer lacks %s", x)
		}
	}
	if !strings.HasPrefix(r.Image, "registry.k8s.io/sig-storage/csi-resizer:v2.3.0@sha256:") {
		t.Errorf("csi-resizer image = %q, want v2.3.0 pinned by digest", r.Image)
	}

	// Both volumes are private to the pod.
	for _, v := range spec.Volumes {
		if v.EmptyDir == nil {
			t.Errorf("volume %s is not an emptyDir", v.Name)
		}
	}
	mounts := map[string]string{}
	for _, vm := range m.VolumeMounts {
		mounts[vm.MountPath] = vm.Name
		if vm.MountPropagation != nil && *vm.MountPropagation != corev1.MountPropagationNone {
			t.Errorf("mount %s has propagation %v", vm.Name, *vm.MountPropagation)
		}
	}
	if len(mounts) != 2 || mounts["/csi"] == "" || mounts["/var/lib/miami"] == "" || mounts["/csi"] == mounts["/var/lib/miami"] {
		t.Errorf("miami mounts = %v, want separate volumes at /csi and /var/lib/miami", mounts)
	}
}

func allContainers(t *testing.T, r rendered) map[string]corev1.PodSpec {
	t.Helper()
	return map[string]corev1.PodSpec{
		"controller": controller(t, r).Spec.Template.Spec,
		"node":       node(t, r).Spec.Template.Spec,
	}
}

func TestResourcesAndImages(t *testing.T) {
	t.Parallel()
	for role, spec := range allContainers(t, base(t)) {
		all := append(append([]corev1.Container{}, spec.InitContainers...), spec.Containers...)
		for _, c := range all {
			id := role + "/" + c.Name
			for _, res := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
				if _, ok := c.Resources.Requests[res]; !ok {
					t.Errorf("%s: no %s request", id, res)
				}
				if _, ok := c.Resources.Limits[res]; !ok {
					t.Errorf("%s: no %s limit", id, res)
				}
			}
			switch {
			case c.Name == "miami":
				if c.Image != driverImage+":unset" {
					t.Errorf("%s: image = %q, want the placeholder %s:unset", id, c.Image, driverImage)
				}
			case !strings.HasPrefix(c.Image, "registry.k8s.io/sig-storage/"):
				t.Errorf("%s: image %q is not a sig-storage sidecar", id, c.Image)
			default:
				ref, digest, ok := strings.Cut(c.Image, "@")
				if !ok || len(digest) != len("sha256:")+64 || !strings.HasPrefix(digest, "sha256:") {
					t.Errorf("%s: image %q carries no sha256 digest", id, c.Image)
				}
				if !strings.Contains(ref[strings.LastIndex(ref, "/"):], ":v") {
					t.Errorf("%s: image %q carries no version tag beside the digest", id, c.Image)
				}
			}
		}
	}
}

func TestSidecarsAgree(t *testing.T) {
	t.Parallel()
	// The livenessprobe is the same image in both pods.
	imgs := map[string]string{}
	for role, spec := range allContainers(t, base(t)) {
		imgs[role] = container(t, spec, "livenessprobe").Image
	}
	if imgs["controller"] != imgs["node"] {
		t.Errorf("livenessprobe images differ: %v", imgs)
	}
}

// TestPortsDistinct: both pods are on the host network, so a port is taken
// once per node.
func TestPortsDistinct(t *testing.T) {
	t.Parallel()
	r := base(t)
	used := map[int]string{}
	claim := func(port int, who string) {
		t.Helper()
		if other, ok := used[port]; ok {
			t.Errorf("port %d is used by %s and %s", port, other, who)
		}
		used[port] = who
	}
	port := func(addr string) int {
		t.Helper()
		n, err := strconv.Atoi(strings.TrimPrefix(addr, ":"))
		if err != nil {
			t.Fatalf("address %q: %v", addr, err)
		}
		return n
	}
	for role, spec := range allContainers(t, r) {
		m := container(t, spec, "miami")
		metrics := port(flagValue(t, m.Args, "metrics-addr"))
		claim(metrics, role+" metrics")
		health := port(flagValue(t, container(t, spec, "livenessprobe").Args, "http-endpoint"))
		claim(health, role+" health")
		// The probe of the miami container targets the sidecar's port, and
		// the declared container ports match the flags.
		if m.LivenessProbe == nil || m.LivenessProbe.HTTPGet == nil || m.LivenessProbe.HTTPGet.Path != "/healthz" ||
			m.LivenessProbe.HTTPGet.Port.IntValue() != health {
			t.Errorf("%s: miami livenessProbe = %+v, want /healthz on port %d", role, m.LivenessProbe, health)
		}
		declared := map[int]bool{}
		for _, c := range spec.Containers {
			for _, p := range c.Ports {
				declared[int(p.ContainerPort)] = true
			}
		}
		want := map[int]bool{metrics: true, health: true}
		if role == "controller" {
			// The webhook shares the host network with everything else.
			wp := port(flagValue(t, m.Args, "webhook-addr"))
			claim(wp, role+" webhook")
			want[wp] = true
		}
		if !reflect.DeepEqual(declared, want) {
			t.Errorf("%s: declared ports %v, want %v", role, declared, want)
		}
	}
	if used[9810] != "node metrics" || used[9811] != "controller metrics" {
		t.Errorf("metrics ports = %v, want node 9810 and controller 9811 (internal/metrics)", used)
	}
	if used[9814] != "controller webhook" || used[9812] != "node health" || used[9813] != "controller health" {
		t.Errorf("ports = %v, want webhook 9814, node health 9812, controller health 9813", used)
	}
}

func volumeNamed(spec corev1.PodSpec, name string) *corev1.Volume {
	for i := range spec.Volumes {
		if spec.Volumes[i].Name == name {
			return &spec.Volumes[i]
		}
	}
	return nil
}

// TestNodeHostPaths holds every host path the node mounts, and with each the
// type, the mount path in the miami container, the propagation and whether it
// is read-only. It is the privileged diff of the volume kind: a path added
// here is a path the node plugin can reach on every host.
func TestNodeHostPaths(t *testing.T) {
	t.Parallel()
	spec := node(t, base(t)).Spec.Template.Spec
	type hp struct {
		path      string
		typ       corev1.HostPathType
		mountPath string
		prop      corev1.MountPropagationMode
		readOnly  bool
	}
	want := map[string]hp{
		// The kubelet directory, mounted whole; the cache and the binary copies
		// live under it, so they need no hostPath of their own.
		"kubelet-dir": {kubeletDir, corev1.HostPathDirectory, kubeletDir, corev1.MountPropagationBidirectional, false},
		// nbd and device-mapper nodes appear after start.
		"host-dev": {"/dev", corev1.HostPathDirectory, "/dev", corev1.MountPropagationHostToContainer, false},
		// systemd's private socket, for the block servers' transient units.
		"host-run-systemd": {"/run/systemd", corev1.HostPathDirectoryOrCreate, "/run/systemd", corev1.MountPropagationNone, true},
		// The nbd devices' sysfs entries.
		"host-sys": {"/sys", corev1.HostPathDirectory, "/sys", corev1.MountPropagationNone, true},
	}

	got := map[string]hp{}
	for _, v := range spec.Volumes {
		if v.Name == cryptsetupLock {
			// Not a hostPath: checked below.
			if v.EmptyDir == nil || v.EmptyDir.Medium != "" || v.EmptyDir.SizeLimit != nil {
				t.Errorf("volume %s must be a plain emptyDir (container-local, on the node's disk): %+v", v.Name, v.VolumeSource)
			}
			continue
		}
		if v.HostPath == nil {
			t.Errorf("volume %s is not a hostPath", v.Name)
			continue
		}
		typ := corev1.HostPathUnset
		if v.HostPath.Type != nil {
			typ = *v.HostPath.Type
		}
		got[v.Name] = hp{path: v.HostPath.Path, typ: typ}
	}
	m := container(t, spec, "miami")
	lockMounts := 0
	for _, vm := range m.VolumeMounts {
		if vm.Name == cryptsetupLock {
			lockMounts++
			if vm.MountPath != "/run/cryptsetup" || vm.ReadOnly || vm.SubPath != "" || vm.MountPropagation != nil {
				t.Errorf("miami mounts %s wrongly: %+v (want a plain writable mount at /run/cryptsetup, cryptsetup's lock directory)", vm.Name, vm)
			}
			continue
		}
		h, ok := got[vm.Name]
		if !ok {
			t.Errorf("miami mounts unknown volume %s", vm.Name)
			continue
		}
		h.mountPath, h.prop, h.readOnly = vm.MountPath, corev1.MountPropagationNone, vm.ReadOnly
		if vm.MountPropagation != nil {
			h.prop = *vm.MountPropagation
		}
		if vm.SubPath != "" {
			t.Errorf("miami mount %s has subPath %q", vm.Name, vm.SubPath)
		}
		got[vm.Name] = h
	}
	if lockMounts != 1 {
		t.Errorf("miami mounts %s %d times, want once", cryptsetupLock, lockMounts)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("node host paths:\n got %+v\nwant %+v", got, want)
	}

	// The volume kind's mounts reach the miami container only, and the host's
	// process namespaces stay out of reach.
	for _, c := range spec.Containers {
		if c.Name == "miami" {
			continue
		}
		for _, vm := range c.VolumeMounts {
			if vm.Name != "kubelet-dir" {
				t.Errorf("%s mounts %s; only the miami container mounts the host's /dev, /sys and /run/systemd", c.Name, vm.Name)
			}
		}
	}
	if spec.HostPID || spec.HostIPC {
		t.Error("hostPID and hostIPC must stay off (decision 41: the block server is a systemd unit)")
	}

	// The cache and bin directories default under the kubelet directory, which
	// is mounted at the same path in the container as on the host. Setting
	// either would need a hostPath of its own, which this deploy does not have.
	for _, f := range []string{"cache-dir", "bin-dir", "shared-dir", "systemd-socket"} {
		for _, a := range m.Args {
			if strings.HasPrefix(a, "--"+f+"=") {
				t.Errorf("--%s is set (%s); the default is the contract", f, a)
			}
		}
	}

	// Privileged already, and nothing added: the volume kind's CAP_SYS_ADMIN
	// comes from it.
	sc := m.SecurityContext
	if sc == nil || sc.Privileged == nil || !*sc.Privileged || sc.Capabilities != nil {
		t.Errorf("miami securityContext = %+v, want privileged with no capabilities field", sc)
	}
}

// TestNodeRBACExact: the node reads two cluster-scoped objects and writes
// events in one namespace. Failures are reported, never remediated (decision
// 53), so it has no say over pods, and no admission policy for remediation is
// shipped.
func TestNodeRBACExact(t *testing.T) {
	t.Parallel()
	r := base(t)
	var cr rbacv1.ClusterRole
	r.get(t, "ClusterRole", "miami-node", &cr)
	var role rbacv1.Role
	r.get(t, "Role", "miami-node", &role)
	all := ruleSet(append(append([]rbacv1.PolicyRule{}, cr.Rules...), role.Rules...))
	want := map[string]bool{
		"/persistentvolumes:get": true,
		"/nodes:get":             true,
		"/nodes/status:patch":    true,
		"/events:get":            true,
		"/events:create":         true,
		"/events:patch":          true,
	}
	if !reflect.DeepEqual(all, want) {
		t.Errorf("node grants %v, want %v", all, want)
	}
	for k := range all {
		if strings.Contains(k, "pods") {
			t.Errorf("node grants %s", k)
		}
	}
	// The one policy the base ships fences the node's status patch; the
	// remediation policies were dropped.
	if n := r.names("MutatingAdmissionPolicy"); len(n) != 0 {
		t.Errorf("the base ships MutatingAdmissionPolicy %v; remediation was dropped", n)
	}
	for _, kind := range []string{"ValidatingAdmissionPolicy", "ValidatingAdmissionPolicyBinding"} {
		if n := r.names(kind); !reflect.DeepEqual(n, []string{"miami-node-capacity"}) {
			t.Errorf("the base ships %s %v, want only miami-node-capacity", kind, n)
		}
	}

	// The controller carries csi-resizer's rules (T16): its PVC update and
	// status patch.
	var ctl rbacv1.ClusterRole
	r.get(t, "ClusterRole", "miami-controller", &ctl)
	got := ruleSet(ctl.Rules)
	for _, k := range []string{"/persistentvolumeclaims:update", "/persistentvolumeclaims/status:patch"} {
		if !got[k] {
			t.Errorf("controller lacks %s", k)
		}
	}
}

// exampleClass returns the example StorageClasses by name.
func exampleClass(t *testing.T, name string) storagev1.StorageClass {
	t.Helper()
	var sc storagev1.StorageClass
	render(t, "kustomize/examples").get(t, "StorageClass", name, &sc)
	return sc
}

// TestExampleVolClassesSetBothSecretRefs: the encrypted volume class names its
// key Secret for node-stage and node-publish alike, in the claim's namespace,
// so a restage from a publish still has the key; the plain class names none.
func TestExampleVolClassesSetBothSecretRefs(t *testing.T) {
	t.Parallel()
	enc := exampleClass(t, "miami-remote-vol-rwo").Parameters
	for _, op := range []string{"node-stage", "node-publish"} {
		if got := enc["csi.storage.k8s.io/"+op+"-secret-name"]; got == "" {
			t.Errorf("encrypted class sets no %s secret name", op)
		}
		if got := enc["csi.storage.k8s.io/"+op+"-secret-namespace"]; got != "${pvc.namespace}" {
			t.Errorf("%s secret namespace = %q, want ${pvc.namespace}", op, got)
		}
	}
	if enc["csi.storage.k8s.io/node-stage-secret-name"] != enc["csi.storage.k8s.io/node-publish-secret-name"] {
		t.Error("node-stage and node-publish name different Secrets")
	}
	if enc["encryption"] == "off" {
		t.Error("the encrypted class turns encryption off")
	}
	plain := exampleClass(t, "miami-remote-vol-rwo-plain").Parameters
	if plain["encryption"] != "off" {
		t.Errorf("plain class encryption = %q, want off", plain["encryption"])
	}
	for k := range plain {
		if strings.Contains(k, "secret") {
			t.Errorf("plain class sets %s", k)
		}
	}
}

// TestExampleVolClassesSetExt4FsType: kubelet applies a pod's fsGroup only to a
// PV with an fsType, and the provisioner sets one only from this parameter.
// A directory has no filesystem to name, so its classes must not set it. The
// volume classes allow expansion and set no takeoverTimeout (the node refuses
// it).
func TestExampleVolClassesSetExt4FsType(t *testing.T) {
	t.Parallel()
	r := render(t, "kustomize/examples")
	for _, name := range r.names("StorageClass") {
		sc := exampleClass(t, name)
		fs, has := sc.Parameters["csi.storage.k8s.io/fstype"]
		if sc.Parameters["kind"] == "volume" {
			if fs != "ext4" {
				t.Errorf("%s: fstype = %q, want ext4", name, fs)
			}
			if sc.AllowVolumeExpansion == nil || !*sc.AllowVolumeExpansion {
				t.Errorf("%s: allowVolumeExpansion must be true", name)
			}
			if _, set := sc.Parameters["takeoverTimeout"]; set {
				t.Errorf("%s sets takeoverTimeout", name)
			}
			if sc.Parameters["transport.kind"] != "nfs3" {
				t.Errorf("%s: transport.kind = %q, want nfs3", name, sc.Parameters["transport.kind"])
			}
		} else if has {
			t.Errorf("%s (a directory class) sets fstype %q", name, fs)
		}
	}
}

var exampleClassNames = []string{"miami-local-dir-rwo", "miami-remote-dir-rwx", "miami-remote-vol-rwo", "miami-remote-vol-rwo-plain"}

func TestExampleStorageClasses(t *testing.T) {
	t.Parallel()
	r := render(t, "kustomize/examples")
	if got, want := r.names("StorageClass"), exampleClassNames; !reflect.DeepEqual(got, want) {
		t.Fatalf("storage classes = %v, want %v", got, want)
	}
	tests := []struct {
		name    string
		binding storagev1.VolumeBindingMode
		params  map[string]string
	}{
		{"miami-local-dir-rwo", storagev1.VolumeBindingWaitForFirstConsumer, map[string]string{
			"kind": "directory", "transport.kind": "local", "transport.path": "/export/data"}},
		{"miami-remote-dir-rwx", storagev1.VolumeBindingImmediate, map[string]string{
			"kind": "directory", "transport.kind": "nfs3", "transport.server": "192.0.2.10",
			"transport.export": "/export", "subdir": "${pvc.namespace}/${pvc.name}"}},
		//nolint:gosec // G101: Secret references of a StorageClass, not credentials.
		{"miami-remote-vol-rwo", storagev1.VolumeBindingImmediate, map[string]string{
			"kind": "volume", "transport.kind": "nfs3", "transport.server": "192.0.2.10",
			"transport.export": "/export", "subdir": "${pvc.namespace}/${pvc.name}",
			"csi.storage.k8s.io/fstype":                        "ext4",
			"csi.storage.k8s.io/node-stage-secret-name":        "miami-volume-key",
			"csi.storage.k8s.io/node-stage-secret-namespace":   "${pvc.namespace}",
			"csi.storage.k8s.io/node-publish-secret-name":      "miami-volume-key",
			"csi.storage.k8s.io/node-publish-secret-namespace": "${pvc.namespace}"}},
		{"miami-remote-vol-rwo-plain", storagev1.VolumeBindingImmediate, map[string]string{
			"kind": "volume", "encryption": "off", "transport.kind": "nfs3", "transport.server": "192.0.2.10",
			"transport.export": "/export", "subdir": "${pvc.namespace}/${pvc.name}",
			"csi.storage.k8s.io/fstype": "ext4"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var sc storagev1.StorageClass
			r.get(t, "StorageClass", tc.name, &sc)
			if sc.Provisioner != driverName {
				t.Errorf("provisioner = %q", sc.Provisioner)
			}
			if sc.VolumeBindingMode == nil || *sc.VolumeBindingMode != tc.binding {
				t.Errorf("volumeBindingMode = %v, want %v", sc.VolumeBindingMode, tc.binding)
			}
			if sc.ReclaimPolicy == nil || *sc.ReclaimPolicy != corev1.PersistentVolumeReclaimDelete {
				t.Errorf("reclaimPolicy = %v, want Delete", sc.ReclaimPolicy)
			}
			if !reflect.DeepEqual(sc.Parameters, tc.params) {
				t.Errorf("parameters = %v, want %v", sc.Parameters, tc.params)
			}
			// The driver itself accepts them.
			if _, err := volume.ParseParameters(sc.Parameters); err != nil {
				t.Errorf("volume.ParseParameters: %v", err)
			}
		})
	}
}

// TestE2EOverlay: the overlay renders, adds the two example StorageClasses,
// and otherwise differs from the base only in the driver image and its pull
// policy, and the node plugin's mount of the host's /export for the example
// local class.
func TestE2EOverlay(t *testing.T) {
	t.Parallel()
	b, o := base(t), overlay(t)

	if got, want := o.names("StorageClass"), exampleClassNames; !reflect.DeepEqual(got, want) {
		t.Errorf("overlay storage classes = %v, want %v", got, want)
	}
	norm := func(r rendered) map[string]any {
		out := map[string]any{}
		for _, ob := range r.objects {
			if ob.kind == "StorageClass" {
				continue
			}
			var v map[string]any
			if err := json.Unmarshal(ob.json, &v); err != nil {
				t.Fatal(err)
			}
			if ob.kind == "Deployment" || ob.kind == "DaemonSet" {
				containers, _, _ := nestedSlice(v, "spec", "template", "spec", "containers")
				for _, c := range containers {
					cm := c.(map[string]any)
					if img, _ := cm["image"].(string); strings.HasPrefix(img, driverImage) {
						delete(cm, "image")
						delete(cm, "imagePullPolicy")
						cm["volumeMounts"] = without(cm["volumeMounts"], localExport)
					}
				}
				if ob.kind == "DaemonSet" {
					spec := v["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
					spec["volumes"] = without(spec["volumes"], localExport)
					dropPluginOff(spec)
				}
			}
			out[ob.kind+"/"+ob.name] = v
		}
		return out
	}
	if !reflect.DeepEqual(norm(b), norm(o)) {
		t.Error("the overlay differs from the base beyond the driver image, its pull policy and the StorageClasses")
	}
	for role, spec := range allContainers(t, o) {
		c := container(t, spec, "miami")
		if c.ImagePullPolicy != corev1.PullIfNotPresent {
			t.Errorf("%s: imagePullPolicy = %q, want IfNotPresent", role, c.ImagePullPolicy)
		}
		if !strings.HasPrefix(c.Image, driverImage+":") {
			t.Errorf("%s: image = %q", role, c.Image)
		}
	}
}

// cryptsetupLock is the name of the node plugin's emptyDir for cryptsetup's
// lock directory.
const cryptsetupLock = "cryptsetup-lock"

// pluginOffKey is the node label whose presence keeps the node plugin off a
// node in the e2e overlay (the volume tests stop one plugin with it).
const pluginOffKey = "miami-e2e/plugin-off"

// dropPluginOff removes the e2e overlay's one extra node affinity expression
// from a pod spec, and fails the comparison by leaving anything else.
func dropPluginOff(spec map[string]any) {
	terms, _, _ := nestedSlice(spec, "affinity", "nodeAffinity", "requiredDuringSchedulingIgnoredDuringExecution", "nodeSelectorTerms")
	for _, tm := range terms {
		term := tm.(map[string]any)
		var keep []any
		for _, ex := range term["matchExpressions"].([]any) {
			if ex.(map[string]any)["key"] != pluginOffKey {
				keep = append(keep, ex)
			}
		}
		term["matchExpressions"] = keep
	}
}

// localExport is the name of the volume the e2e overlay adds to the node
// plugin for the example local class.
const localExport = "local-export"

// without returns the list of volumes or mounts minus the one named name.
func without(list any, name string) []any {
	var out []any
	items, _ := list.([]any)
	for _, it := range items {
		if m, ok := it.(map[string]any); ok && m["name"] == name {
			continue
		}
		out = append(out, it)
	}
	return out
}

func nestedSlice(m map[string]any, keys ...string) ([]any, bool, error) {
	var cur any = m
	for _, k := range keys {
		mm, ok := cur.(map[string]any)
		if !ok {
			return nil, false, fmt.Errorf("%s is not a map", k)
		}
		cur = mm[k]
	}
	s, ok := cur.([]any)
	return s, ok, nil
}

func TestBaseGolden(t *testing.T) {
	t.Parallel()
	got := base(t).yaml
	path := filepath.Join("testdata", "base.golden.yaml")
	if *update {
		if err := os.WriteFile(path, got, 0o600); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path) //nolint:gosec // a fixed path in testdata
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("the rendered base differs from %s; review the change and regenerate it with `go test ./deploy -update`", path)
	}
}

// TestPolicyAndBindingShip: a policy with no binding enforces nothing, and a
// policy that fails open would let a compromised node plugin write what the
// policy exists to stop. Taking either file out of the kustomization fails
// here, because the render then lacks the object.
func TestPolicyAndBindingShip(t *testing.T) {
	t.Parallel()
	r := base(t)
	var pol admissionv1.ValidatingAdmissionPolicy
	r.get(t, "ValidatingAdmissionPolicy", "miami-node-capacity", &pol)
	var bind admissionv1.ValidatingAdmissionPolicyBinding
	r.get(t, "ValidatingAdmissionPolicyBinding", "miami-node-capacity", &bind)

	if bind.Spec.PolicyName != pol.Name {
		t.Errorf("the binding names policy %q, want %q", bind.Spec.PolicyName, pol.Name)
	}
	if !reflect.DeepEqual(bind.Spec.ValidationActions, []admissionv1.ValidationAction{admissionv1.Deny}) {
		t.Errorf("validationActions = %v, want [Deny]", bind.Spec.ValidationActions)
	}
	if bind.Spec.MatchResources != nil || bind.Spec.ParamRef != nil {
		t.Errorf("the binding narrows the policy: %+v", bind.Spec)
	}
	if pol.Spec.FailurePolicy == nil || *pol.Spec.FailurePolicy != admissionv1.Fail {
		t.Errorf("failurePolicy = %v, want Fail", pol.Spec.FailurePolicy)
	}
}

// TestNodePolicyScope pins what the policy matches: UPDATE of nodes/status
// only, and only for the node plugin's ServiceAccount (the one the node
// ClusterRoleBinding names). Anything wider would put the kubelet and the
// admins under rules written for the plugin, and anything narrower would let
// the plugin's grant through unchecked.
func TestNodePolicyScope(t *testing.T) {
	t.Parallel()
	r := base(t)
	var pol admissionv1.ValidatingAdmissionPolicy
	r.get(t, "ValidatingAdmissionPolicy", "miami-node-capacity", &pol)
	if pol.Spec.MatchConstraints == nil || len(pol.Spec.MatchConstraints.ResourceRules) != 1 {
		t.Fatalf("matchConstraints = %+v", pol.Spec.MatchConstraints)
	}
	rr := pol.Spec.MatchConstraints.ResourceRules[0]
	if !reflect.DeepEqual(rr.Resources, []string{"nodes/status"}) ||
		!reflect.DeepEqual(rr.Operations, []admissionv1.OperationType{admissionv1.Update}) ||
		!reflect.DeepEqual(rr.APIGroups, []string{""}) {
		t.Errorf("the policy matches %+v", rr)
	}

	var crb rbacv1.ClusterRoleBinding
	r.get(t, "ClusterRoleBinding", "miami-node", &crb)
	if len(crb.Subjects) != 1 {
		t.Fatalf("subjects = %v", crb.Subjects)
	}
	user := "system:serviceaccount:" + crb.Subjects[0].Namespace + ":" + crb.Subjects[0].Name
	if len(pol.Spec.MatchConditions) != 1 {
		t.Fatalf("matchConditions = %v", pol.Spec.MatchConditions)
	}
	if got := squash(pol.Spec.MatchConditions[0].Expression); got != nodePluginMatch {
		t.Errorf("matchCondition = %q, want %q", got, nodePluginMatch)
	}
	if !matchesPlugin(user) {
		t.Errorf("the condition does not match the binding's subject %s", user)
	}

	// The tags the envtest matrix asserts on are the policy's messages.
	var msgs []string
	for _, v := range pol.Spec.Validations {
		msgs = append(msgs, v.Message)
	}
	for _, tag := range []string{"[node-name]", "[capacity]", "[status]", "[metadata]"} {
		n := 0
		for _, m := range msgs {
			if strings.Contains(m, tag) {
				n++
			}
		}
		if n != 1 {
			t.Errorf("%d validations carry %s, want 1", n, tag)
		}
	}
}

// TestNodeStatusFieldsCovered: the [status] and [metadata] rules name the
// fields they hold still, so a field the API gains would be writable by the
// plugin until it is added there. Every field of NodeStatus but capacity must
// appear in the status rule.
func TestNodeStatusFieldsCovered(t *testing.T) {
	t.Parallel()
	var pol admissionv1.ValidatingAdmissionPolicy
	base(t).get(t, "ValidatingAdmissionPolicy", "miami-node-capacity", &pol)
	var rule string
	for _, v := range pol.Spec.Validations {
		if strings.Contains(v.Message, "[status]") {
			rule = v.Expression
		}
	}
	rt := reflect.TypeOf(corev1.NodeStatus{})
	for i := 0; i < rt.NumField(); i++ {
		name, _, _ := strings.Cut(rt.Field(i).Tag.Get("json"), ",")
		if name == "capacity" {
			continue
		}
		if !strings.Contains(rule, "object.status."+name+")") {
			t.Errorf("the [status] rule does not hold status.%s still", name)
		}
	}
}

// nodePluginMatch is the matchCondition, and matchesPlugin its meaning in Go.
const nodePluginMatch = "request.userInfo.username.startsWith('system:serviceaccount:') && request.userInfo.username.endsWith(':miami-node')"

func squash(s string) string { return strings.Join(strings.Fields(s), " ") }

func matchesPlugin(user string) bool {
	return strings.HasPrefix(user, "system:serviceaccount:") && strings.HasSuffix(user, ":miami-node")
}

// TestNodePolicyFollowsNamespaceOverlay: a kustomize `namespace:` rewrites the
// ClusterRoleBinding's subject but not CEL. A condition that named the
// namespace would then miss the plugin, which would hold nodes/status patch on
// every node with no fence at all.
func TestNodePolicyFollowsNamespaceOverlay(t *testing.T) {
	t.Parallel()
	r := renderOverlay(t, "namespace: storage\n")
	var crb rbacv1.ClusterRoleBinding
	r.get(t, "ClusterRoleBinding", "miami-node", &crb)
	if len(crb.Subjects) != 1 || crb.Subjects[0].Namespace != "storage" {
		t.Fatalf("the overlay did not move the subject: %v", crb.Subjects)
	}
	var pol admissionv1.ValidatingAdmissionPolicy
	r.get(t, "ValidatingAdmissionPolicy", "miami-node-capacity", &pol)
	if got := squash(pol.Spec.MatchConditions[0].Expression); got != nodePluginMatch {
		t.Fatalf("matchCondition = %q", got)
	}
	subject := "system:serviceaccount:" + crb.Subjects[0].Namespace + ":" + crb.Subjects[0].Name
	if !matchesPlugin(subject) {
		t.Errorf("the condition misses the moved subject %s: the grant would be unfenced", subject)
	}
	for _, other := range []string{"kubernetes-admin", "system:masters", "system:node:node-a", "system:serviceaccount:storage:miami-controller"} {
		if matchesPlugin(other) {
			t.Errorf("the condition matches %s", other)
		}
	}
	if strings.Contains(pol.Spec.MatchConditions[0].Expression, "miami-system") {
		t.Error("the condition names the base's namespace")
	}
}

// TestNodePolicyDetectsRenamingOverlays: namePrefix and nameSuffix rename the
// ServiceAccount, and no static string match follows every rename. The test
// pins that the mismatch is real, so that nobody takes the namespace test as a
// promise about renames: such an overlay must patch the matchCondition, and the
// node's runtime fence probe (cachecap.Runner) is the backstop that stops it
// advertising when it does not.
func TestNodePolicyDetectsRenamingOverlays(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, kust, wantSA string }{
		{"namePrefix", "namePrefix: prod-\n", "prod-miami-node"},
		{"nameSuffix", "nameSuffix: -prod\n", "miami-node-prod"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := renderOverlay(t, tc.kust)
			var crb rbacv1.ClusterRoleBinding
			r.get(t, "ClusterRoleBinding", tc.wantSA, &crb)
			if len(crb.Subjects) != 1 || crb.Subjects[0].Name != tc.wantSA {
				t.Fatalf("the subject was not renamed: %v", crb.Subjects)
			}
			subject := "system:serviceaccount:" + crb.Subjects[0].Namespace + ":" + crb.Subjects[0].Name
			if matchesPlugin(subject) {
				t.Errorf("the condition unexpectedly matches the renamed subject %s; if the policy now follows renames, say so here and in the policy comment", subject)
			}
		})
	}
}

// renderOverlay renders the base under an overlay whose kustomization carries
// extra after its resources.
func renderOverlay(t *testing.T, extra string) rendered {
	t.Helper()
	// A scratch directory beside the base: kustomize wants relative roots.
	dir, err := os.MkdirTemp(".", "nsprobe")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	kust := "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\n" + extra + "resources:\n  - ../kustomize/base\n"
	if err := os.WriteFile(filepath.Join(dir, "kustomization.yaml"), []byte(kust), 0o600); err != nil {
		t.Fatal(err)
	}
	opts := krusty.MakeDefaultOptions()
	opts.LoadRestrictions = types.LoadRestrictionsNone
	kustomizeMu.Lock()
	m, err := krusty.MakeKustomizer(opts).Run(filesys.MakeFsOnDisk(), dir)
	kustomizeMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	var r rendered
	for _, res := range m.Resources() {
		js, err := res.MarshalJSON()
		if err != nil {
			t.Fatal(err)
		}
		r.objects = append(r.objects, object{kind: res.GetKind(), name: res.GetName(), json: js})
	}
	return r
}

func TestWebhookConfiguration(t *testing.T) {
	t.Parallel()
	r := base(t)
	if n := r.names("MutatingWebhookConfiguration"); !reflect.DeepEqual(n, []string{"miami-cache"}) {
		t.Fatalf("MutatingWebhookConfigurations = %v, want only miami-cache", n)
	}
	var cfg admissionv1.MutatingWebhookConfiguration
	r.get(t, "MutatingWebhookConfiguration", "miami-cache", &cfg)
	if len(cfg.Webhooks) != 1 {
		t.Fatalf("webhooks = %d, want 1", len(cfg.Webhooks))
	}
	w := cfg.Webhooks[0]
	if w.FailurePolicy == nil || *w.FailurePolicy != admissionv1.Ignore {
		t.Errorf("failurePolicy = %v, want Ignore", w.FailurePolicy)
	}
	if w.SideEffects == nil || *w.SideEffects != admissionv1.SideEffectClassNone {
		t.Errorf("sideEffects = %v, want None", w.SideEffects)
	}
	if w.MatchPolicy == nil || *w.MatchPolicy != admissionv1.Equivalent {
		t.Errorf("matchPolicy = %v, want Equivalent", w.MatchPolicy)
	}
	if w.ReinvocationPolicy == nil || *w.ReinvocationPolicy != admissionv1.IfNeededReinvocationPolicy {
		t.Errorf("reinvocationPolicy = %v, want IfNeeded", w.ReinvocationPolicy)
	}
	if w.TimeoutSeconds == nil || *w.TimeoutSeconds != 10 {
		t.Errorf("timeoutSeconds = %v, want 10", w.TimeoutSeconds)
	}
	if !reflect.DeepEqual(w.AdmissionReviewVersions, []string{"v1"}) {
		t.Errorf("admissionReviewVersions = %v, want [v1]", w.AdmissionReviewVersions)
	}
	wantRules := []admissionv1.RuleWithOperations{{
		Operations: []admissionv1.OperationType{admissionv1.Create},
		Rule:       admissionv1.Rule{APIGroups: []string{""}, APIVersions: []string{"v1"}, Resources: []string{"pods"}},
	}}
	if !reflect.DeepEqual(w.Rules, wantRules) {
		t.Errorf("rules = %+v, want CREATE on pods only", w.Rules)
	}
	if w.NamespaceSelector == nil || len(w.NamespaceSelector.MatchLabels) != 0 {
		t.Fatalf("namespaceSelector = %+v, want matchExpressions only", w.NamespaceSelector)
	}
	wantExpr := []metav1.LabelSelectorRequirement{
		{Key: "kubernetes.io/metadata.name", Operator: metav1.LabelSelectorOpNotIn, Values: []string{"kube-system", "miami-system"}},
		{Key: "miami.csi.greghaskins.com/cache-accounting", Operator: metav1.LabelSelectorOpNotIn, Values: []string{"disabled"}},
	}
	if !reflect.DeepEqual(w.NamespaceSelector.MatchExpressions, wantExpr) {
		t.Errorf("namespaceSelector = %+v, want %+v", w.NamespaceSelector.MatchExpressions, wantExpr)
	}
	if w.ObjectSelector != nil {
		t.Errorf("objectSelector = %+v, want none", w.ObjectSelector)
	}
	cc := w.ClientConfig
	if cc.URL != nil || len(cc.CABundle) != 0 {
		t.Errorf("clientConfig url=%v caBundle=%d bytes; the base has a Service and an empty caBundle", cc.URL, len(cc.CABundle))
	}
	if cc.Service == nil || cc.Service.Name != "miami-webhook" || cc.Service.Namespace != "miami-system" ||
		cc.Service.Path == nil || *cc.Service.Path != "/mutate" || cc.Service.Port == nil || *cc.Service.Port != 443 {
		t.Errorf("clientConfig.service = %+v, want miami-webhook.miami-system /mutate :443", cc.Service)
	}
	if len(cfg.Annotations) != 0 {
		t.Errorf("annotations = %v; the base has no CA injection", cfg.Annotations)
	}

	// The Service: 443 to the controller's webhook port, the controller's two
	// labels, and nothing else.
	var svc corev1.Service
	r.get(t, "Service", "miami-webhook", &svc)
	if svc.Namespace != "miami-system" || len(svc.Spec.Ports) != 1 {
		t.Fatalf("service = %+v", svc)
	}
	sp := svc.Spec.Ports[0]
	if sp.Port != 443 || sp.TargetPort.IntValue() != 9814 || (sp.Protocol != "" && sp.Protocol != corev1.ProtocolTCP) {
		t.Errorf("service port = %+v, want 443 to 9814/TCP", sp)
	}
	wantSel := map[string]string{"app.kubernetes.io/name": "miami", "app.kubernetes.io/component": "controller"}
	if !reflect.DeepEqual(svc.Spec.Selector, wantSel) {
		t.Errorf("service selector = %v, want %v", svc.Spec.Selector, wantSel)
	}
	if svc.Spec.Type != "" && svc.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Errorf("service type = %v, want ClusterIP", svc.Spec.Type)
	}
	// The selector matches the controller's pod labels, and the target port is
	// the one the miami container serves on.
	d := controller(t, r)
	for k, v := range wantSel {
		if d.Spec.Template.Labels[k] != v {
			t.Errorf("controller pod label %s = %q, want %q", k, d.Spec.Template.Labels[k], v)
		}
	}
}

// TestWebhookRBACNarrow: the webhook adds no read and nothing with watch or
// on nodes; its two writes are named objects. The node's rights and T3's
// policy are as before.
func TestWebhookRBACNarrow(t *testing.T) {
	t.Parallel()
	r := base(t)
	var cr rbacv1.ClusterRole
	r.get(t, "ClusterRole", "miami-controller", &cr)
	var mwc []rbacv1.PolicyRule
	for _, ru := range cr.Rules {
		for _, g := range ru.APIGroups {
			if g == "admissionregistration.k8s.io" {
				mwc = append(mwc, ru)
			}
		}
	}
	want := []rbacv1.PolicyRule{{
		APIGroups: []string{"admissionregistration.k8s.io"}, Resources: []string{"mutatingwebhookconfigurations"},
		ResourceNames: []string{"miami-cache"}, Verbs: []string{"get", "patch"},
	}}
	if !reflect.DeepEqual(mwc, want) {
		t.Errorf("admissionregistration rules = %+v, want %+v", mwc, want)
	}
	// The pre-existing reads are what covers the webhook; the whole set of
	// ClusterRole rules is exact in TestRBACRuleSets.
	var node rbacv1.ClusterRole
	r.get(t, "ClusterRole", "miami-node", &node)
	wantNode := []rbacv1.PolicyRule{
		{APIGroups: []string{""}, Resources: []string{"persistentvolumes"}, Verbs: []string{"get"}},
		{APIGroups: []string{""}, Resources: []string{"nodes"}, Verbs: []string{"get"}},
		{APIGroups: []string{""}, Resources: []string{"nodes/status"}, Verbs: []string{"patch"}},
	}
	if !reflect.DeepEqual(node.Rules, wantNode) {
		t.Errorf("node ClusterRole = %+v, want %+v", node.Rules, wantNode)
	}
	for _, o := range r.objects {
		if o.kind != "Role" && o.kind != "ClusterRole" {
			continue
		}
		var rules []rbacv1.PolicyRule
		if o.kind == "Role" {
			var ro rbacv1.Role
			r.get(t, o.kind, o.name, &ro)
			rules = ro.Rules
		} else {
			var c rbacv1.ClusterRole
			r.get(t, o.kind, o.name, &c)
			rules = c.Rules
		}
		for _, ru := range rules {
			for _, res := range ru.Resources {
				if res == "secrets" || res == "mutatingwebhookconfigurations" {
					for _, v := range ru.Verbs {
						if v == "watch" || v == "list" || v == "delete" || v == "*" {
							t.Errorf("%s %s grants %s on %s", o.kind, o.name, v, res)
						}
					}
				}
			}
		}
	}
}

// TestCertManagerComponent: the component swaps the controller's own
// certificate for cert-manager's. The controller then holds a get on the one
// managed Secret, no write to any Secret, and no right on the webhook
// configuration.
func TestCertManagerComponent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	abs, err := filepath.Abs("kustomize")
	if err != nil {
		t.Fatal(err)
	}
	// Kustomize takes relative roots only.
	rel := func(p string) string {
		t.Helper()
		out, err := filepath.Rel(dir, filepath.Join(abs, p))
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	kust := "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - " +
		rel("base") + "\ncomponents:\n  - " + rel("components/cert-manager") + "\n"
	if err := os.WriteFile(filepath.Join(dir, "kustomization.yaml"), []byte(kust), 0o600); err != nil {
		t.Fatal(err)
	}
	r := render(t, dir)

	var role rbacv1.Role
	r.get(t, "Role", "miami-controller", &role)
	wantRules := []rbacv1.PolicyRule{
		{APIGroups: []string{"coordination.k8s.io"}, Resources: []string{"leases"}, Verbs: []string{"get", "create", "update", "delete"}},
		{APIGroups: []string{""}, Resources: []string{"secrets"}, ResourceNames: []string{"miami-webhook-tls-managed"}, Verbs: []string{"get"}},
	}
	if !reflect.DeepEqual(role.Rules, wantRules) {
		t.Errorf("role rules = %+v, want %+v", role.Rules, wantRules)
	}

	var cr rbacv1.ClusterRole
	r.get(t, "ClusterRole", "miami-controller", &cr)
	var baseCR rbacv1.ClusterRole
	base(t).get(t, "ClusterRole", "miami-controller", &baseCR)
	for _, ru := range cr.Rules {
		for _, g := range ru.APIGroups {
			if g == "admissionregistration.k8s.io" {
				t.Errorf("ClusterRole still has %+v", ru)
			}
		}
	}
	if !reflect.DeepEqual(cr.Rules, baseCR.Rules[:len(baseCR.Rules)-1]) {
		t.Errorf("ClusterRole = %+v, want the base's without its last (webhook configuration) rule", cr.Rules)
	}

	m := container(t, controller(t, r).Spec.Template.Spec, "miami")
	if got := flagValue(t, m.Args, "webhook-tls-secret"); got != "miami-webhook-tls-managed" {
		t.Errorf("--webhook-tls-secret = %q, want miami-webhook-tls-managed", got)
	}
	if got := flagValue(t, m.Args, "webhook-addr"); got != ":9814" {
		t.Errorf("--webhook-addr = %q", got)
	}

	var cfg admissionv1.MutatingWebhookConfiguration
	r.get(t, "MutatingWebhookConfiguration", "miami-cache", &cfg)
	if want := map[string]string{"cert-manager.io/inject-ca-from": "miami-system/miami-webhook"}; !reflect.DeepEqual(cfg.Annotations, want) {
		t.Errorf("annotations = %v, want %v", cfg.Annotations, want)
	}
	var cert struct {
		APIVersion string         `json:"apiVersion"`
		Kind       string         `json:"kind"`
		Metadata   map[string]any `json:"metadata"`
		Spec       struct {
			SecretName string `json:"secretName"`
			IssuerRef  struct {
				Kind string `json:"kind"`
				Name string `json:"name"`
			} `json:"issuerRef"`
			DNSNames []string `json:"dnsNames"`
		} `json:"spec"`
	}
	r.get(t, "Certificate", "miami-webhook", &cert)
	if cert.Spec.SecretName != "miami-webhook-tls-managed" {
		t.Errorf("secretName = %q", cert.Spec.SecretName)
	}
	if cert.Spec.IssuerRef.Kind != "Issuer" || len(r.names("Issuer")) != 1 || r.names("Issuer")[0] != cert.Spec.IssuerRef.Name {
		t.Errorf("issuerRef = %+v, want the component's own Issuer %v", cert.Spec.IssuerRef, r.names("Issuer"))
	}
	wantNames := []string{"miami-webhook.miami-system.svc", "miami-webhook.miami-system.svc.cluster.local"}
	if !reflect.DeepEqual(cert.Spec.DNSNames, wantNames) {
		t.Errorf("dnsNames = %v, want %v", cert.Spec.DNSNames, wantNames)
	}

	// The base alone carries no cert-manager object.
	if n := base(t).names("Certificate"); len(n) != 0 {
		t.Errorf("the base ships Certificates %v", n)
	}
}
