//go:build envtest

package deploy

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/greghaskins/csi-miami/internal/cachecap"
	"github.com/greghaskins/csi-miami/internal/kube"
)

// These tests run the shipped admission policy against a real API server and
// etcd started by envtest (`make envtest`). Only the objects the base renders
// are created, so the YAML under test is the YAML that ships. They fail, and do
// not skip, when the assets are missing.

const (
	capKey      = "miami.csi.greghaskins.com~1cache" // as a JSON-pointer segment
	nodeSAUser  = "system:serviceaccount:miami-system:miami-node"
	nodeNameKey = "authentication.kubernetes.io/node-name"
)

var (
	nodesGVR    = schema.GroupVersionResource{Version: "v1", Resource: "nodes"}
	vapGVR      = schema.GroupVersionResource{Group: "admissionregistration.k8s.io", Version: "v1", Resource: "validatingadmissionpolicies"}
	bindingGVR  = schema.GroupVersionResource{Group: "admissionregistration.k8s.io", Version: "v1", Resource: "validatingadmissionpolicybindings"}
	clusterRole = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}
	clusterBind = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}
)

func dyn(t *testing.T, cfg *rest.Config) dynamic.Interface {
	t.Helper()
	c, err := dynamic.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// createRendered creates the one rendered object of the kind and name.
func createRendered(t *testing.T, admin dynamic.Interface, r rendered, kind, name string, gvr schema.GroupVersionResource) {
	t.Helper()
	for _, o := range r.objects {
		if o.kind != kind || o.name != name {
			continue
		}
		u := &unstructured.Unstructured{}
		if err := u.UnmarshalJSON(o.json); err != nil {
			t.Fatal(err)
		}
		if _, err := admin.Resource(gvr).Create(context.Background(), u, metav1.CreateOptions{}); err != nil {
			t.Fatalf("creating %s %s: %v", kind, name, err)
		}
		return
	}
	t.Fatalf("the base renders no %s %q", kind, name)
}

func asUser(t *testing.T, cfg *rest.Config, user string, groups []string, extra map[string][]string) dynamic.Interface {
	t.Helper()
	c := rest.CopyConfig(cfg)
	c.Impersonate = rest.ImpersonationConfig{UserName: user, Groups: groups, Extra: extra}
	return dyn(t, c)
}

// freshNodes recreates node-a and node-b in a known state, as the admin.
func freshNodes(t *testing.T, admin dynamic.Interface) {
	t.Helper()
	ctx := context.Background()
	for _, name := range []string{"node-a", "node-b"} {
		// A finalizer an earlier row added would keep the node from going.
		_, err := admin.Resource(nodesGVR).Patch(ctx, name, types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`), metav1.PatchOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			t.Fatal(err)
		}
		err = admin.Resource(nodesGVR).Delete(ctx, name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			t.Fatal(err)
		}
		for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(50 * time.Millisecond) {
			if _, err := admin.Resource(nodesGVR).Get(ctx, name, metav1.GetOptions{}); apierrors.IsNotFound(err) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("node %s did not go away", name)
			}
		}
		n := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Node",
			"metadata": map[string]any{
				"name":        name,
				"labels":      map[string]any{"kubernetes.io/hostname": name},
				"annotations": map[string]any{"example.com/a": "b"},
			},
		}}
		if _, err := admin.Resource(nodesGVR).Create(ctx, n, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		// Another manager owns a field by server-side apply, so the node
		// carries an operation: Apply managedFields entry the plugin must not
		// be able to forge, drop or alter.
		apply := `{"apiVersion":"v1","kind":"Node","metadata":{"name":"` + name + `","labels":{"example.com/applied":"1"}},"spec":{"taints":[{"key":"example.com/t","effect":"NoSchedule"}]}}`
		if _, err := admin.Resource(nodesGVR).Patch(ctx, name, types.ApplyPatchType, []byte(apply), metav1.PatchOptions{FieldManager: "other-manager", Force: ptrTo(true)}); err != nil {
			t.Fatal(err)
		}
		status := `{"status":{
			"capacity":{"cpu":"4","miami.csi.greghaskins.com/cache":"10Gi"},
			"allocatable":{"cpu":"4"},
			"conditions":[{"type":"Ready","status":"True","reason":"KubeletReady","lastHeartbeatTime":"2026-01-01T00:00:00Z","lastTransitionTime":"2026-01-01T00:00:00Z"}],
			"images":[{"names":["example.com/a:1"],"sizeBytes":1}]}}`
		if _, err := admin.Resource(nodesGVR).Patch(ctx, name, types.MergePatchType, []byte(status), metav1.PatchOptions{}, "status"); err != nil {
			t.Fatal(err)
		}
	}
}

func ptrTo[T any](v T) *T { return &v }

func jsonPatch(ops ...string) []byte { return []byte("[" + strings.Join(ops, ",") + "]") }

func op(o, path, value string) string {
	s := `{"op":"` + o + `","path":"` + path + `"`
	if value != "" {
		s += `,"value":` + value
	}
	return s + "}"
}

func patchStatus(c dynamic.Interface, node string, body []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := c.Resource(nodesGVR).Patch(ctx, node, types.JSONPatchType, body, metav1.PatchOptions{}, "status")
	return err
}

func TestNodeCapacityPolicy(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Fatal("KUBEBUILDER_ASSETS is not set; run these tests with `make envtest`")
	}
	env := &envtest.Environment{}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("starting the API server: %v", err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Errorf("stopping the API server: %v", err)
		}
	})
	admin := dyn(t, cfg)
	r := base(t)

	// The shipped RBAC and the shipped policy; the test adds only a role for
	// the kubelet, which the cluster's authorizer would grant it.
	createRendered(t, admin, r, "ClusterRole", "miami-node", clusterRole)
	createRendered(t, admin, r, "ClusterRoleBinding", "miami-node", clusterBind)
	createRendered(t, admin, r, "ValidatingAdmissionPolicy", "miami-node-capacity", vapGVR)
	createRendered(t, admin, r, "ValidatingAdmissionPolicyBinding", "miami-node-capacity", bindingGVR)
	for _, o := range []struct {
		gvr  schema.GroupVersionResource
		body string
	}{
		{clusterRole, `{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"ClusterRole","metadata":{"name":"test-kubelet"},"rules":[{"apiGroups":[""],"resources":["nodes","nodes/status"],"verbs":["get","patch","update"]}]}`},
		{clusterBind, `{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"ClusterRoleBinding","metadata":{"name":"test-kubelet"},"roleRef":{"apiGroup":"rbac.authorization.k8s.io","kind":"ClusterRole","name":"test-kubelet"},"subjects":[{"kind":"Group","apiGroup":"rbac.authorization.k8s.io","name":"system:nodes"}]}`},
	} {
		u := &unstructured.Unstructured{}
		if err := u.UnmarshalJSON([]byte(o.body)); err != nil {
			t.Fatal(err)
		}
		if _, err := admin.Resource(o.gvr).Create(context.Background(), u, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	plugin := func(nodeName string) dynamic.Interface {
		var extra map[string][]string
		if nodeName != "" {
			extra = map[string][]string{nodeNameKey: {nodeName}}
		}
		return asUser(t, cfg, nodeSAUser, []string{"system:serviceaccounts", "system:serviceaccounts:miami-system", "system:authenticated"}, extra)
	}

	// envtest runs no controller manager, so the policy's type-checking status
	// is never filled in. An expression that did not compile would, under
	// failurePolicy Fail, deny every request below, including the allowed
	// ones; the matrix is what proves the shipped expressions work. The
	// policy was also admitted, so the API server's cost estimate accepted it.
	deadline := time.Now().Add(60 * time.Second)
	// Wait until the policy enforces, with a request that must be denied.
	freshNodes(t, admin)
	for {
		err := patchStatus(plugin("node-b"), "node-a", jsonPatch(op("add", "/status/capacity/"+capKey, `"30Gi"`)))
		if err != nil && strings.Contains(err.Error(), "miami-node-capacity") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the policy never enforced; last answer: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	type row struct {
		name string
		as   func() dynamic.Interface
		node string
		body []byte
		deny string // "" means allowed; else a substring of the denial
	}
	capAdd := op("add", "/status/capacity/"+capKey, `"30Gi"`)
	kubelet := func() dynamic.Interface {
		return asUser(t, cfg, "system:node:node-a", []string{"system:nodes", "system:authenticated"}, nil)
	}
	administrator := func() dynamic.Interface { return admin }
	own := func() dynamic.Interface { return plugin("node-a") }

	// What each forbidden write does, so that the same set runs as the plugin
	// (denied) and as the admin and the kubelet (allowed).
	forbidden := []struct {
		name, tag string
		body      []byte
	}{
		{"another capacity key", "[capacity]", jsonPatch(op("add", "/status/capacity/example.com~1other", `"1"`))},
		{"the fence probe key", "[capacity]", jsonPatch(op("add", "/status/capacity/miami.csi.greghaskins.com~1fence-probe", `"1"`))},
		{"cpu capacity", "[capacity]", jsonPatch(op("replace", "/status/capacity/cpu", `"8"`))},
		{"removing cpu capacity", "[capacity]", jsonPatch(op("remove", "/status/capacity/cpu", ""))},
		{"our key with another", "[capacity]", jsonPatch(capAdd, op("add", "/status/capacity/example.com~1other", `"1"`))},
		{"our key with cpu", "[capacity]", jsonPatch(capAdd, op("replace", "/status/capacity/cpu", `"8"`))},
		{"allocatable", "[status]", jsonPatch(op("replace", "/status/allocatable/cpu", `"2"`))},
		{"conditions", "[status]", jsonPatch(op("replace", "/status/conditions/0/status", `"False"`))},
		{"images", "[status]", jsonPatch(op("replace", "/status/images/0/sizeBytes", "2"))},
		{"addresses", "[status]", jsonPatch(op("add", "/status/addresses", `[{"type":"InternalIP","address":"192.0.2.1"}]`))},
		{"daemonEndpoints", "[status]", jsonPatch(op("add", "/status/daemonEndpoints", `{"kubeletEndpoint":{"Port":10250}}`))},
		{"nodeInfo", "[status]", jsonPatch(op("add", "/status/nodeInfo", `{"machineID":"","systemUUID":"","bootID":"","kernelVersion":"","osImage":"","containerRuntimeVersion":"","kubeletVersion":"","kubeProxyVersion":"","operatingSystem":"linux","architecture":"amd64"}`))},
		{"volumesInUse", "[status]", jsonPatch(op("add", "/status/volumesInUse", `["kubernetes.io/csi/x^y"]`))},
		{"declaredFeatures", "[status]", jsonPatch(op("add", "/status/declaredFeatures", `["Example"]`))},
		{"labels", "[metadata]", jsonPatch(op("add", "/metadata/labels/example.com~1x", `"y"`))},
		{"removing a label", "[metadata]", jsonPatch(op("remove", "/metadata/labels/kubernetes.io~1hostname", ""))},
		{"annotations", "[metadata]", jsonPatch(op("add", "/metadata/annotations/example.com~1x", `"y"`))},
		{"finalizers", "[metadata]", jsonPatch(op("add", "/metadata/finalizers", `["example.com/f"]`))},
		{"ownerReferences", "[metadata]", jsonPatch(op("add", "/metadata/ownerReferences", `[{"apiVersion":"v1","kind":"Node","name":"node-b","uid":"00000000-0000-0000-0000-000000000001"}]`))},
	}

	rows := []row{
		{"add our key on its own node", own, "node-a", jsonPatch(capAdd), ""},
		{"change our key", own, "node-a", jsonPatch(op("add", "/status/capacity/"+capKey, `"0"`)), ""},
		{"remove our key on its own node", own, "node-a", jsonPatch(op("remove", "/status/capacity/"+capKey, "")), ""},
		{"add our key on another node", own, "node-b", jsonPatch(capAdd), "[node-name]"},
		{"remove our key on another node", own, "node-b", jsonPatch(op("remove", "/status/capacity/"+capKey, "")), "[node-name]"},
		{"no node-name extra", func() dynamic.Interface { return plugin("") }, "node-a", jsonPatch(capAdd), "[node-name]"},
		{"node-name of a node that does not exist", func() dynamic.Interface { return plugin("ghost") }, "node-a", jsonPatch(capAdd), "[node-name]"},
		{"a node that does not exist", func() dynamic.Interface { return plugin("ghost") }, "ghost", jsonPatch(capAdd), "not found"},
		{"the kubelet writes our key", kubelet, "node-a", jsonPatch(capAdd), ""},
	}
	for _, f := range forbidden {
		rows = append(rows,
			row{"plugin: " + f.name, own, "node-a", f.body, f.tag},
			row{"admin: " + f.name, administrator, "node-a", f.body, ""},
			row{"kubelet: " + f.name, kubelet, "node-a", f.body, ""},
		)
	}

	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			freshNodes(t, admin)
			err := patchStatus(tc.as(), tc.node, tc.body)
			switch {
			case tc.deny == "" && err != nil:
				t.Errorf("denied: %v", err)
			case tc.deny != "" && err == nil:
				t.Error("allowed, want denied")
			case tc.deny != "" && !strings.Contains(err.Error(), tc.deny):
				t.Errorf("denied for another reason: %v; want it to name %s", err, tc.deny)
			case tc.deny != "" && tc.deny[0] == '[' && !strings.Contains(err.Error(), "miami-node-capacity"):
				t.Errorf("denied, but not by the policy: %v", err)
			}
		})
	}

	// The probe key: the plugin may remove a leftover one, but never add or
	// change it (the probe relies on its add being denied).
	probeKey := "miami.csi.greghaskins.com~1fence-probe"
	place := func(t *testing.T) {
		freshNodes(t, admin)
		if err := patchStatus(admin, "node-a", jsonPatch(op("add", "/status/capacity/"+probeKey, `"1"`))); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("probe key: removing a leftover is allowed", func(t *testing.T) {
		place(t)
		if err := patchStatus(own(), "node-a", jsonPatch(op("remove", "/status/capacity/"+probeKey, ""))); err != nil {
			t.Errorf("denied: %v", err)
		}
	})
	t.Run("probe key: removing it with our key is allowed", func(t *testing.T) {
		place(t)
		if err := patchStatus(own(), "node-a", jsonPatch(op("remove", "/status/capacity/"+probeKey, ""), capAdd)); err != nil {
			t.Errorf("denied: %v", err)
		}
	})
	t.Run("probe key: changing it is denied", func(t *testing.T) {
		place(t)
		err := patchStatus(own(), "node-a", jsonPatch(op("replace", "/status/capacity/"+probeKey, `"2"`)))
		if err == nil || !strings.Contains(err.Error(), "[capacity]") {
			t.Errorf("err = %v, want a [capacity] denial", err)
		}
	})
	t.Run("probe key: adding it is denied", func(t *testing.T) {
		freshNodes(t, admin)
		err := patchStatus(own(), "node-a", jsonPatch(op("add", "/status/capacity/"+probeKey, `"1"`)))
		if err == nil || !strings.Contains(err.Error(), "[capacity]") {
			t.Errorf("err = %v, want a [capacity] denial", err)
		}
	})
	t.Run("probe key: removing another key with it is still denied", func(t *testing.T) {
		place(t)
		err := patchStatus(own(), "node-a", jsonPatch(op("remove", "/status/capacity/"+probeKey, ""), op("remove", "/status/capacity/cpu", "")))
		if err == nil || !strings.Contains(err.Error(), "[capacity]") {
			t.Errorf("err = %v, want a [capacity] denial", err)
		}
	})
}

// TestFenceProbeAgainstThePolicy runs the node's own probe (kube.ProbeFence)
// as the plugin: denied by the shipped policy while the binding exists, and
// allowed, with its key removed again, once the binding is gone.
func TestFenceProbeAgainstThePolicy(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Fatal("KUBEBUILDER_ASSETS is not set; run these tests with `make envtest`")
	}
	env := &envtest.Environment{}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("starting the API server: %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })
	admin := dyn(t, cfg)
	r := base(t)
	createRendered(t, admin, r, "ClusterRole", "miami-node", clusterRole)
	createRendered(t, admin, r, "ClusterRoleBinding", "miami-node", clusterBind)
	createRendered(t, admin, r, "ValidatingAdmissionPolicy", "miami-node-capacity", vapGVR)
	createRendered(t, admin, r, "ValidatingAdmissionPolicyBinding", "miami-node-capacity", bindingGVR)
	freshNodes(t, admin)

	pc := rest.CopyConfig(cfg)
	pc.Impersonate = rest.ImpersonationConfig{
		UserName: nodeSAUser,
		Groups:   []string{"system:serviceaccounts", "system:authenticated"},
		Extra:    map[string][]string{nodeNameKey: {"node-a"}},
	}
	plugin, err := kube.NewClient(pc, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	probe := func() (cachecap.FenceState, error) { return plugin.ProbeFence(context.Background(), "node-a") }

	deadline := time.Now().Add(60 * time.Second)
	for {
		st, err := probe()
		if st == cachecap.Fenced {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the policy never fenced the probe: %v, %v", st, err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	if err := admin.Resource(bindingGVR).Delete(context.Background(), "miami-node-capacity", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	for {
		st, err := probe()
		if st == cachecap.Unfenced {
			if err != nil {
				t.Fatalf("unfenced, but the probe key was not cleaned: %v", err)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("an unbound policy still looks like a fence: %v, %v", st, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	n, err := admin.Resource(nodesGVR).Get(context.Background(), "node-a", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, found, _ := unstructured.NestedString(n.Object, "status", "capacity", "miami.csi.greghaskins.com/fence-probe"); found {
		t.Error("the probe key was left on the node")
	}
}

// applyEntries returns the operation: Apply entries of the node, as JSON.
func applyEntries(t *testing.T, admin dynamic.Interface, name string) []string {
	t.Helper()
	n, err := admin.Resource(nodesGVR).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	mf, _, _ := unstructured.NestedSlice(n.Object, "metadata", "managedFields")
	var out []string
	for _, e := range mf {
		if m, _ := e.(map[string]any); m["operation"] == "Apply" {
			b, _ := json.Marshal(m)
			out = append(out, string(b))
		}
	}
	return out
}

// TestManagedFieldsApplyEntriesHeld covers the [metadata] rule's hold on the
// server-side-apply entries of managedFields. A forged, altered or dropped
// Apply entry would make another manager's next apply delete fields it does
// not own, taints included.
//
// The rule holds because the plugin's patches touch nothing but two leaf keys
// under status.capacity that no other manager owns (its cache key and the
// probe key; TestPluginPatchesTouchOnlyItsTwoKeys pins the paths), and a JSON
// remove transfers ownership of nothing. Writing a field another manager owns
// would make the server rewrite that manager's entry and trip the rule.
//
// On the status endpoint the API server also ignores managedFields sent in a
// patch (shown below), so tampering there changes nothing; the main-resource
// variant of the policy is what proves the expression itself.
func TestManagedFieldsApplyEntriesHeld(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Fatal("KUBEBUILDER_ASSETS is not set; run these tests with `make envtest`")
	}
	env := &envtest.Environment{}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("starting the API server: %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })
	admin := dyn(t, cfg)
	r := base(t)
	createRendered(t, admin, r, "ClusterRole", "miami-node", clusterRole)
	createRendered(t, admin, r, "ClusterRoleBinding", "miami-node", clusterBind)
	createRendered(t, admin, r, "ValidatingAdmissionPolicy", "miami-node-capacity", vapGVR)
	createRendered(t, admin, r, "ValidatingAdmissionPolicyBinding", "miami-node-capacity", bindingGVR)

	// The same policy, matching the main resource, where managedFields sent in
	// a patch are honoured; and a role that lets the plugin patch it.
	for _, o := range r.objects {
		if o.name != "miami-node-capacity" || (o.kind != "ValidatingAdmissionPolicy" && o.kind != "ValidatingAdmissionPolicyBinding") {
			continue
		}
		u := &unstructured.Unstructured{}
		if err := u.UnmarshalJSON(o.json); err != nil {
			t.Fatal(err)
		}
		u.SetName("main-copy")
		gvr := vapGVR
		if o.kind == "ValidatingAdmissionPolicy" {
			rules := []any{map[string]any{"apiGroups": []any{""}, "apiVersions": []any{"v1"}, "operations": []any{"UPDATE"}, "resources": []any{"nodes"}}}
			if err := unstructured.SetNestedSlice(u.Object, rules, "spec", "matchConstraints", "resourceRules"); err != nil {
				t.Fatal(err)
			}
		} else {
			gvr = bindingGVR
			if err := unstructured.SetNestedField(u.Object, "main-copy", "spec", "policyName"); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := admin.Resource(gvr).Create(context.Background(), u, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	for _, o := range []struct {
		gvr  schema.GroupVersionResource
		body string
	}{
		{clusterRole, `{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"ClusterRole","metadata":{"name":"test-main"},"rules":[{"apiGroups":[""],"resources":["nodes"],"verbs":["get","patch"]}]}`},
		{clusterBind, `{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"ClusterRoleBinding","metadata":{"name":"test-main"},"roleRef":{"apiGroup":"rbac.authorization.k8s.io","kind":"ClusterRole","name":"test-main"},"subjects":[{"kind":"User","apiGroup":"rbac.authorization.k8s.io","name":"` + nodeSAUser + `"}]}`},
	} {
		u := &unstructured.Unstructured{}
		if err := u.UnmarshalJSON([]byte(o.body)); err != nil {
			t.Fatal(err)
		}
		if _, err := admin.Resource(o.gvr).Create(context.Background(), u, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	plugin := asUser(t, cfg, nodeSAUser, []string{"system:authenticated"}, map[string][]string{nodeNameKey: {"node-a"}})
	patchMain := func(body []byte) error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := plugin.Resource(nodesGVR).Patch(ctx, "node-a", types.JSONPatchType, body, metav1.PatchOptions{})
		return err
	}

	deadline := time.Now().Add(60 * time.Second)
	for {
		freshNodes(t, admin)
		e1 := patchStatus(plugin, "node-a", jsonPatch(op("add", "/status/capacity/example.com~1other", `"1"`)))
		e2 := patchMain(jsonPatch(op("add", "/spec/unschedulable", "true"), op("add", "/metadata/labels/x", `"y"`)))
		if e1 != nil && e2 != nil && strings.Contains(e1.Error(), "miami-node-capacity") && strings.Contains(e2.Error(), "main-copy") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the policies never enforced: %v / %v", e1, e2)
		}
		time.Sleep(500 * time.Millisecond)
	}

	freshNodes(t, admin)
	held := applyEntries(t, admin, "node-a")
	if len(held) != 1 {
		t.Fatalf("want one Apply entry on the node, have %v", held)
	}
	at := "/metadata/managedFields/0"
	{
		n, _ := admin.Resource(nodesGVR).Get(context.Background(), "node-a", metav1.GetOptions{})
		mf, _, _ := unstructured.NestedSlice(n.Object, "metadata", "managedFields")
		for i, e := range mf {
			if m, _ := e.(map[string]any); m["operation"] == "Apply" {
				at = "/metadata/managedFields/" + strconv.Itoa(i)
			}
		}
	}
	forged := `{"manager":"forged","operation":"Apply","apiVersion":"v1","fieldsType":"FieldsV1","fieldsV1":{"f:metadata":{"f:labels":{"f:kubernetes.io/hostname":{}}}}}`
	tamper := map[string][]byte{
		"forging an Apply entry":                  jsonPatch(op("add", "/metadata/managedFields/-", forged)),
		"clearing managedFields":                  jsonPatch(op("add", "/metadata/managedFields", "[]")),
		"resetting managedFields":                 jsonPatch(op("replace", "/metadata/managedFields", "[{}]")),
		"removing managedFields":                  jsonPatch(op("remove", "/metadata/managedFields", "")),
		"altering the manager of an Apply entry":  jsonPatch(op("replace", at+"/manager", `"someone-else"`)),
		"altering the fields of an Apply entry":   jsonPatch(op("replace", at+"/fieldsV1", `{"f:metadata":{"f:labels":{"f:kubernetes.io/hostname":{}}}}`)),
		"removing the Apply entry":                jsonPatch(op("remove", at, "")),
		"turning the Apply entry into an Update":  jsonPatch(op("replace", at+"/operation", `"Update"`)),
		"replacing all entries with a forged one": jsonPatch(op("replace", "/metadata/managedFields", "["+forged+"]")),
	}

	t.Run("status endpoint: the server ignores managedFields in a patch", func(t *testing.T) {
		for name, body := range tamper {
			freshNodes(t, admin)
			err := patchStatus(plugin, "node-a", body)
			if err != nil && !strings.Contains(err.Error(), "[metadata]") {
				t.Errorf("%s: denied for another reason: %v", name, err)
			}
			if got := applyEntries(t, admin, "node-a"); len(got) != 1 || got[0] != held[0] && !stripTime(got[0], held[0]) {
				t.Errorf("%s: the Apply entries changed: %v", name, got)
			}
		}
	})

	t.Run("main resource: the rule denies every tamper", func(t *testing.T) {
		for name, body := range tamper {
			if name == "removing managedFields" {
				continue // a request with no managedFields leaves them as they were
			}
			freshNodes(t, admin)
			err := patchMain(body)
			if err == nil || !strings.Contains(err.Error(), "main-copy") || !strings.Contains(err.Error(), "[metadata]") {
				t.Errorf("%s: %v, want a [metadata] denial", name, err)
			}
		}
		freshNodes(t, admin)
		if err := patchMain(jsonPatch(op("add", "/spec/unschedulable", "true"))); err != nil {
			t.Errorf("an ordinary patch of the main resource was denied: %v", err)
		}
	})

	t.Run("the plugin's own patches leave another manager's apply byte-identical", func(t *testing.T) {
		freshNodes(t, admin)
		before := applyEntries(t, admin, "node-a")
		for _, step := range [][]byte{
			jsonPatch(op("add", "/status/capacity/"+capKey, `"1Gi"`)),
			jsonPatch(op("add", "/status/capacity/"+capKey, `"2Gi"`)),
			jsonPatch(op("remove", "/status/capacity/"+capKey, "")),
		} {
			if err := patchStatus(plugin, "node-a", step); err != nil {
				t.Errorf("%s: denied: %v", step, err)
			}
		}
		// The probe key: its add is denied by design; a leftover one, placed by
		// the admin, must be removable, and the removal must succeed.
		if err := patchStatus(plugin, "node-a", jsonPatch(op("add", "/status/capacity/miami.csi.greghaskins.com~1fence-probe", `"1"`))); err == nil {
			t.Error("the probe key add was allowed")
		}
		if err := patchStatus(admin, "node-a", jsonPatch(op("add", "/status/capacity/miami.csi.greghaskins.com~1fence-probe", `"1"`))); err != nil {
			t.Fatal(err)
		}
		if err := patchStatus(plugin, "node-a", jsonPatch(op("remove", "/status/capacity/miami.csi.greghaskins.com~1fence-probe", ""))); err != nil {
			t.Errorf("removing a leftover probe key was denied: %v", err)
		}
		if after := applyEntries(t, admin, "node-a"); !reflect.DeepEqual(before, after) {
			t.Errorf("the Apply entry changed:\n before %v\n after  %v", before, after)
		}
		n, _ := admin.Resource(nodesGVR).Get(context.Background(), "node-a", metav1.GetOptions{})
		if taints, _, _ := unstructured.NestedSlice(n.Object, "spec", "taints"); len(taints) != 1 {
			t.Errorf("the taint another manager applied is gone: %v", taints)
		}
		if v, _, _ := unstructured.NestedString(n.Object, "metadata", "labels", "example.com/applied"); v != "1" {
			t.Errorf("the label another manager applied is gone")
		}
	})
}

// stripTime reports whether two Apply entries are equal but for their time.
func stripTime(a, b string) bool {
	var x, y map[string]any
	if json.Unmarshal([]byte(a), &x) != nil || json.Unmarshal([]byte(b), &y) != nil {
		return false
	}
	delete(x, "time")
	delete(y, "time")
	return reflect.DeepEqual(x, y)
}
