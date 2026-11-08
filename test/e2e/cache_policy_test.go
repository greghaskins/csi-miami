//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// The node plugin's one write on a Node is its own node's cache key, and the
// admission policy is what keeps it to that. This test is the security test of
// the feature: it takes the plugin's own identity, as the kubelet binds it to
// a pod, and tries what a compromised plugin would. Each refusal is logged by
// name with the API server's words.
func TestCachePolicyNegative(t *testing.T) {
	e := loadEnv(t)
	e.needCache(t)
	ws := e.workers(t)
	own, other := ws[0], ws[1]

	ctx, cancel := e.withDeadline()
	ownPod, err := e.nodePluginPod(ctx, own)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	otherPod, err := e.nodePluginPod(ctx, other)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	asOwn := e.pluginClient(t, &ownPod.ObjectMeta, true)
	asOther := e.pluginClient(t, &otherPod.ObjectMeta, true)
	unbound := e.pluginClient(t, nil, false)

	t.Run("allowed-own-node-own-key", func(t *testing.T) {
		if err := patchCapacity(asOwn, own, "add", cacheKeyPointer, "3Gi"); err != nil {
			t.Fatalf("the plugin's own change of its own key was refused: %v", err)
		}
		t.Logf("allowed: the plugin on %s changed its %s from %s to 3Gi", own, cacheKey, cacheBudget)
		// The plugin puts its figure back within two passes of the overlay's 5s.
		e.waitFor(t, own+"'s plugin to restore "+cacheBudget, func(ctx context.Context) (string, bool, error) {
			q, ok, err := e.nodeCapacity(ctx, own)
			if err != nil {
				return "", false, err
			}
			return q.String(), ok && q.Cmp(resource.MustParse(cacheBudget)) == 0, nil
		})
		t.Logf("restored: %s is back at %s", own, cacheBudget)
	})

	t.Run("refused-other-nodes-key", func(t *testing.T) {
		err := patchCapacity(asOwn, other, "add", cacheKeyPointer, "1Gi")
		wantDenied(t, err, "node-name")
		e.wantCapacity(t, other, cacheBudget)
	})

	t.Run("refused-another-capacity-key-on-own-node", func(t *testing.T) {
		err := patchCapacity(asOwn, own, "add", "/status/capacity/example.com~1other", "1")
		wantDenied(t, err, "capacity")
		ctx, cancel := e.withDeadline()
		defer cancel()
		n, gerr := e.Client.CoreV1().Nodes().Get(ctx, own, metav1.GetOptions{})
		if gerr != nil {
			t.Fatal(gerr)
		}
		if _, ok := n.Status.Capacity["example.com/other"]; ok {
			t.Error("example.com/other is on the node although the patch was refused")
		}
	})

	t.Run("refused-label-through-nodes-status", func(t *testing.T) {
		const label = "miami-e2e/policy-negative"
		t.Cleanup(func() {
			ctx, cancel := e.withDeadline()
			defer cancel()
			patch := fmt.Sprintf(`{"metadata":{"labels":{%q:null}}}`, label)
			_, _ = e.Client.CoreV1().Nodes().Patch(ctx, own, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
		})
		ctx, cancel := e.withDeadline()
		defer cancel()
		patch := fmt.Sprintf(`{"metadata":{"labels":{%q:"1"}}}`, label)
		_, err := asOwn.CoreV1().Nodes().Patch(ctx, own, types.MergePatchType, []byte(patch), metav1.PatchOptions{FieldManager: "miami-node"}, "status")
		wantDenied(t, err, "metadata")
		n, gerr := e.Client.CoreV1().Nodes().Get(ctx, own, metav1.GetOptions{})
		if gerr != nil {
			t.Fatal(gerr)
		}
		if _, ok := n.Labels[label]; ok {
			t.Error("the label is on the node although the patch was refused")
		}
	})

	t.Run("refused-token-bound-to-the-other-workers-pod", func(t *testing.T) {
		err := patchCapacity(asOther, own, "add", cacheKeyPointer, "1Gi")
		wantDenied(t, err, "node-name")
		e.wantCapacity(t, own, cacheBudget)
	})

	t.Run("refused-unbound-token", func(t *testing.T) {
		err := patchCapacity(unbound, own, "add", cacheKeyPointer, "1Gi")
		wantDenied(t, err, "node-name")
		e.wantCapacity(t, own, cacheBudget)
	})
}

// The plugin puts its key back: when it is removed by hand, and when a stale
// lower figure is written over it, within a few of the overlay's 5s passes.
func TestCacheRepatchAfterRemoval(t *testing.T) {
	e := loadEnv(t)
	e.needCache(t)
	node := e.workers(t)[0]

	admin := func(op, value string) {
		t.Helper()
		if err := patchCapacity(e.Client, node, op, cacheKeyPointer, value); err != nil {
			t.Fatalf("%s of %s on %s by an admin: %v", op, cacheKey, node, err)
		}
	}
	back := func(what string) {
		t.Helper()
		start := time.Now()
		e.waitFor(t, node+"'s plugin to put its figure back after "+what, func(ctx context.Context) (string, bool, error) {
			q, ok, err := e.nodeCapacity(ctx, node)
			if err != nil {
				return "", false, err
			}
			return fmt.Sprintf("capacity %s present %v", q.String(), ok), ok && q.Cmp(resource.MustParse(cacheBudget)) == 0, nil
		})
		t.Logf("%s: %s was back at %s after %s", what, cacheKey, cacheBudget, time.Since(start).Round(time.Second))
	}

	admin("remove", "")
	back("a removal")
	admin("add", "1Gi")
	back("a stale lower value")
}

// cacheKeyPointer is the JSON pointer of the cache key in status.capacity.
var cacheKeyPointer = "/status/capacity/" + strings.ReplaceAll(cacheKey, "/", "~1")

// patchCapacity sends one JSON patch operation to nodes/status as a client.
func patchCapacity(c kubernetes.Interface, node, op, path, value string) error {
	o := map[string]string{"op": op, "path": path}
	if op != "remove" {
		o["value"] = value
	}
	body, err := json.Marshal([]map[string]string{o})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = c.CoreV1().Nodes().Patch(ctx, node, types.JSONPatchType, body, metav1.PatchOptions{FieldManager: "miami-node"}, "status")
	return err
}

// wantDenied fails unless err is the API server saying the policy's named
// rule denied the request, and logs the refusal.
func wantDenied(t *testing.T, err error, rule string) {
	t.Helper()
	if err == nil {
		t.Fatalf("the request was allowed, want it denied by rule [%s]", rule)
	}
	msg := err.Error()
	if !apierrors.IsForbidden(err) || !strings.Contains(msg, "ValidatingAdmissionPolicy 'miami-node-capacity'") || !strings.Contains(msg, "["+rule+"]") {
		t.Fatalf("the request was refused, but not by the policy's [%s] rule: %v", rule, err)
	}
	t.Logf("refused by [%s]: %s", rule, msg)
}

// wantCapacity fails unless the node still advertises the budget.
func (e *env) wantCapacity(t *testing.T, node, want string) {
	t.Helper()
	ctx, cancel := e.withDeadline()
	defer cancel()
	q, ok, err := e.nodeCapacity(ctx, node)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || q.Cmp(resource.MustParse(want)) != 0 {
		t.Errorf("node %s advertises %s %s (present %v) after the refused request, want %s", node, cacheKey, q.String(), ok, want)
	}
}

// pluginClient is a client with a token of the node plugin's ServiceAccount:
// bound to the pod when bind is true (the token carries the pod's node, as the
// kubelet's projected token does), and unbound otherwise.
func (e *env) pluginClient(t *testing.T, pod *metav1.ObjectMeta, bind bool) kubernetes.Interface {
	t.Helper()
	exp := int64(900)
	req := &authenticationv1.TokenRequest{Spec: authenticationv1.TokenRequestSpec{ExpirationSeconds: &exp}}
	if bind {
		req.Spec.BoundObjectRef = &authenticationv1.BoundObjectReference{Kind: "Pod", APIVersion: "v1", Name: pod.Name, UID: pod.UID}
	}
	ctx, cancel := e.withDeadline()
	defer cancel()
	tok, err := e.Client.CoreV1().ServiceAccounts(e.DriverNS).CreateToken(ctx, "miami-node", req, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating a token for miami-node: %v", err)
	}
	cfg := rest.AnonymousClientConfig(e.Config)
	cfg.BearerToken = tok.Status.Token
	c, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
