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
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	storagev1 "k8s.io/api/storage/v1"
	"sigs.k8s.io/kustomize/api/krusty"
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

func render(t *testing.T, dir string) rendered {
	t.Helper()
	k := krusty.MakeKustomizer(krusty.MakeDefaultOptions())
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
			{"storage.k8s.io", "storageclasses", []string{"get", "list", "watch"}},
			{"storage.k8s.io", "volumeattachments", []string{"get", "list", "watch", "update", "patch"}},
			{"storage.k8s.io", "volumeattachments/status", []string{"patch"}},
			{"", "nodes", []string{"get", "list", "watch"}},
			{"storage.k8s.io", "csinodes", []string{"get", "list", "watch"}},
			{"", "events", []string{"list", "watch", "create", "update", "patch"}},
			{"coordination.k8s.io", "leases", []string{"get", "create", "update", "delete"}},
		}},
		{"miami-node", []rule{
			{"", "persistentvolumes", []string{"get"}},
			{"", "events", []string{"get", "create", "patch"}},
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
				if len(ru.ResourceNames) > 0 || len(ru.NonResourceURLs) > 0 {
					t.Errorf("unexpected resourceNames or nonResourceURLs in %+v", ru)
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

type mount struct {
	hostPath    string
	hostType    corev1.HostPathType
	mountPath   string
	propagation corev1.MountPropagationMode
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

	// The node has exactly one hostPath: the kubelet directory, mounted whole
	// and Bidirectional into the miami container.
	if len(spec.Volumes) != 1 {
		t.Errorf("the node has %d volumes, want only the kubelet directory", len(spec.Volumes))
	}
	hostPaths := map[string]corev1.Volume{}
	for _, v := range spec.Volumes {
		if v.HostPath == nil {
			t.Errorf("volume %s is not a hostPath; the node needs no other kind", v.Name)
			continue
		}
		hostPaths[v.Name] = v
	}
	got := map[string]mount{}
	for _, vm := range m.VolumeMounts {
		v, ok := hostPaths[vm.Name]
		if !ok {
			t.Errorf("miami mounts unknown volume %s", vm.Name)
			continue
		}
		prop := corev1.MountPropagationNone
		if vm.MountPropagation != nil {
			prop = *vm.MountPropagation
		}
		typ := corev1.HostPathUnset
		if v.HostPath.Type != nil {
			typ = *v.HostPath.Type
		}
		got[v.HostPath.Path] = mount{v.HostPath.Path, typ, vm.MountPath, prop}
	}
	want := map[string]mount{
		kubeletDir: {kubeletDir, corev1.HostPathDirectory, kubeletDir, corev1.MountPropagationBidirectional},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("miami host mounts:\n got %+v\nwant %+v", got, want)
	}
	for _, v := range spec.Volumes {
		if v.HostPath != nil && (v.HostPath.Path == "/dev" || strings.HasPrefix(v.HostPath.Path, "/dev/")) {
			t.Errorf("volume %s mounts the host /dev, which M1 does not need", v.Name)
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
				t.Errorf("%s: mounts unknown volume %s", c.Name, m.Name)
				continue
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
	if len(spec.Volumes) != 1 || spec.Volumes[0].Name != "kubelet-dir" || spec.Volumes[0].HostPath == nil ||
		spec.Volumes[0].HostPath.Path != dir {
		t.Errorf("volumes = %+v, want the one hostPath %s", spec.Volumes, dir)
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
	if len(spec.NodeSelector) != 0 || spec.Affinity != nil {
		t.Error("the controller has no node selection in M1")
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
	// No resizer in M1.
	if want := []string{"miami", "csi-provisioner", "csi-attacher", "livenessprobe"}; !reflect.DeepEqual(names, want) {
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
				if c.Image != driverImage+":latest" {
					t.Errorf("%s: image = %q, want the placeholder %s:latest", id, c.Image, driverImage)
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
		if !declared[metrics] || !declared[health] || len(declared) != 2 {
			t.Errorf("%s: declared ports %v, want exactly %d and %d", role, declared, metrics, health)
		}
	}
	if used[9810] != "node metrics" || used[9811] != "controller metrics" {
		t.Errorf("metrics ports = %v, want node 9810 and controller 9811 (internal/metrics)", used)
	}
}

func TestExampleStorageClasses(t *testing.T) {
	t.Parallel()
	r := render(t, "kustomize/examples")
	if got, want := r.names("StorageClass"), []string{"miami-local", "miami-nfs3"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("storage classes = %v, want %v", got, want)
	}
	tests := []struct {
		name    string
		binding storagev1.VolumeBindingMode
		params  map[string]string
	}{
		{"miami-local", storagev1.VolumeBindingWaitForFirstConsumer, map[string]string{
			"kind": "directory", "transport.kind": "local", "transport.path": "/export/data"}},
		{"miami-nfs3", storagev1.VolumeBindingImmediate, map[string]string{
			"kind": "directory", "transport.kind": "nfs3", "transport.server": "192.0.2.10",
			"transport.export": "/export", "subdir": "${pvc.namespace}/${pvc.name}"}},
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

	if got, want := o.names("StorageClass"), []string{"miami-local", "miami-nfs3"}; !reflect.DeepEqual(got, want) {
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
