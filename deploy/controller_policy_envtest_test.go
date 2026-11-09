//go:build envtest

package deploy

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

const controllerSAUser = "system:serviceaccount:miami-system:miami-controller"

var (
	mwcGVR     = schema.GroupVersionResource{Group: "admissionregistration.k8s.io", Version: "v1", Resource: "mutatingwebhookconfigurations"}
	secretsGVR = schema.GroupVersionResource{Version: "v1", Resource: "secrets"}
	roleGVR    = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}
	roleBindGV = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}
	namespaces = schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}
)

// startEnvtest starts an API server and returns its configuration.
func startEnvtest(t *testing.T) *rest.Config {
	t.Helper()
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
	return cfg
}

// controllerAs is the dynamic client of the controller's ServiceAccount.
func controllerAs(t *testing.T, cfg *rest.Config) dynamic.Interface {
	t.Helper()
	return asUser(t, cfg, controllerSAUser, []string{"system:serviceaccounts", "system:serviceaccounts:miami-system", "system:authenticated"}, nil)
}

func createJSON(t *testing.T, c dynamic.Interface, gvr schema.GroupVersionResource, ns, body string) {
	t.Helper()
	u := &unstructured.Unstructured{}
	if err := u.UnmarshalJSON([]byte(body)); err != nil {
		t.Fatal(err)
	}
	var err error
	if ns == "" {
		_, err = c.Resource(gvr).Create(context.Background(), u, metav1.CreateOptions{})
	} else {
		_, err = c.Resource(gvr).Namespace(ns).Create(context.Background(), u, metav1.CreateOptions{})
	}
	if err != nil {
		t.Fatalf("creating %s: %v", gvr.Resource, err)
	}
}

// testCA is a PEM certificate, which the API server requires of a caBundle.
func testCA(t *testing.T, cn string) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: cn},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

// TestControllerWebhookConfigPolicy runs the shipped policy that fences the
// controller's patch of its MutatingWebhookConfiguration against a real API
// server. It also proves the premise: a ValidatingAdmissionPolicy is evaluated
// for mutatingwebhookconfigurations (a configuration is exempt from webhooks,
// not from policies), because every denial below comes from the policy.
func TestControllerWebhookConfigPolicy(t *testing.T) {
	cfg := startEnvtest(t)
	admin := dyn(t, cfg)
	r := base(t)

	createRendered(t, admin, r, "ClusterRole", "miami-controller", clusterRole)
	createRendered(t, admin, r, "ClusterRoleBinding", "miami-controller", clusterBind)
	createRendered(t, admin, r, "MutatingWebhookConfiguration", "miami-cache", mwcGVR)
	createRendered(t, admin, r, "ValidatingAdmissionPolicy", "miami-controller-webhook-config", vapGVR)
	createRendered(t, admin, r, "ValidatingAdmissionPolicyBinding", "miami-controller-webhook-config", bindingGVR)
	ctrl := controllerAs(t, cfg)
	other := asUser(t, cfg, "system:serviceaccount:miami-system:someone-else", []string{"system:masters"}, nil)

	patch := func(c dynamic.Interface, body []byte) error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := c.Resource(mwcGVR).Patch(ctx, "miami-cache", types.JSONPatchType, body, metav1.PatchOptions{})
		return err
	}
	// caBundle is a []byte, so its JSON form is the base64 of the PEM.
	bundle := func(cn string) string {
		b, _ := jsonString(base64.StdEncoding.EncodeToString([]byte(testCA(t, cn))))
		return b
	}
	w := "/webhooks/0/"
	setBundle := func(cn string) []byte {
		return jsonPatch(op("add", w+"clientConfig/caBundle", bundle(cn)))
	}

	// Wait until the policy enforces, with a request it must deny.
	// A different value each time: the API server skips admission for a patch
	// that changes nothing.
	deadline := time.Now().Add(60 * time.Second)
	for i := 0; ; i++ {
		err := patch(ctrl, jsonPatch(op("add", w+"timeoutSeconds", strconv.Itoa(11+i%10))))
		if err != nil && strings.Contains(err.Error(), "miami-controller-webhook-config") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the policy never enforced; last answer: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	if err := patch(ctrl, setBundle("one")); err != nil {
		t.Errorf("setting the caBundle was denied: %v", err)
	}
	if err := patch(ctrl, setBundle("two")); err != nil {
		t.Errorf("replacing the caBundle was denied: %v", err)
	}

	forbidden := []struct {
		name, tag string
		body      []byte
	}{
		{"widening the rules to Secrets", "[webhooks]", jsonPatch(op("replace", w+"rules/0/resources", `["secrets","pods"]`))},
		{"another operation", "[webhooks]", jsonPatch(op("replace", w+"rules/0/operations", `["CREATE","UPDATE"]`))},
		{"another apiGroup", "[webhooks]", jsonPatch(op("replace", w+"rules/0/apiGroups", `["*"]`))},
		{"another rule", "[webhooks]", jsonPatch(op("add", w+"rules/-", `{"apiGroups":[""],"apiVersions":["v1"],"operations":["CREATE"],"resources":["secrets"]}`))},
		{"the name", "[webhooks]", jsonPatch(op("replace", w+"name", `"other.miami.csi.greghaskins.com"`))},
		{"failurePolicy", "[webhooks]", jsonPatch(op("replace", w+"failurePolicy", `"Fail"`))},
		{"matchPolicy", "[webhooks]", jsonPatch(op("replace", w+"matchPolicy", `"Exact"`))},
		{"sideEffects", "[webhooks]", jsonPatch(op("replace", w+"sideEffects", `"NoneOnDryRun"`))},
		{"timeoutSeconds", "[webhooks]", jsonPatch(op("replace", w+"timeoutSeconds", `5`))},
		{"timeoutSeconds back to the default", "[webhooks]", jsonPatch(op("replace", w+"timeoutSeconds", `10`))},
		{"admissionReviewVersions", "[webhooks]", jsonPatch(op("replace", w+"admissionReviewVersions", `["v1","v1beta1"]`))},
		{"reinvocationPolicy", "[webhooks]", jsonPatch(op("replace", w+"reinvocationPolicy", `"Never"`))},
		{"namespaceSelector widened", "[webhooks]", jsonPatch(op("replace", w+"namespaceSelector", `{}`))},
		{"namespaceSelector removed", "[webhooks]", jsonPatch(op("remove", w+"namespaceSelector", ""))},
		{"objectSelector", "[webhooks]", jsonPatch(op("replace", w+"objectSelector", `{"matchLabels":{"a":"b"}}`))},
		{"matchConditions", "[webhooks]", jsonPatch(op("add", w+"matchConditions", `[{"name":"x","expression":"true"}]`))},
		{"the service name", "[webhooks]", jsonPatch(op("replace", w+"clientConfig/service/name", `"elsewhere"`))},
		{"the service namespace", "[webhooks]", jsonPatch(op("replace", w+"clientConfig/service/namespace", `"kube-system"`))},
		{"the service path", "[webhooks]", jsonPatch(op("replace", w+"clientConfig/service/path", `"/other"`))},
		{"the service port", "[webhooks]", jsonPatch(op("replace", w+"clientConfig/service/port", `8443`))},
		{"a url instead of the service", "[webhooks]", jsonPatch(op("replace", w+"clientConfig", `{"url":"https://example.invalid/x"}`))},
		{"a second webhook", "[webhooks]", jsonPatch(op("add", "/webhooks/-", `{"name":"b.miami.csi.greghaskins.com","admissionReviewVersions":["v1"],"sideEffects":"None","clientConfig":{"url":"https://example.invalid/x"},"rules":[{"apiGroups":[""],"apiVersions":["v1"],"operations":["CREATE"],"resources":["secrets"]}]}`))},
		{"the caBundle with a rule", "[webhooks]", jsonPatch(op("add", w+"clientConfig/caBundle", bundle("three")), op("replace", w+"rules/0/resources", `["secrets"]`))},
		{"a label", "[metadata]", jsonPatch(op("add", "/metadata/labels", `{"example.com/x":"y"}`))},
		{"an annotation", "[metadata]", jsonPatch(op("add", "/metadata/annotations", `{"example.com/x":"y"}`))},
		{"forging an Apply managedFields entry", "[metadata]", jsonPatch(op("add", "/metadata/managedFields/-", `{"manager":"someone","operation":"Apply","apiVersion":"admissionregistration.k8s.io/v1","fieldsType":"FieldsV1","fieldsV1":{"f:webhooks":{}}}`))},
		{"a finalizer", "[metadata]", jsonPatch(op("add", "/metadata/finalizers", `["example.com/f"]`))},
	}
	for _, f := range forbidden {
		t.Run("controller: "+f.name, func(t *testing.T) {
			err := patch(ctrl, f.body)
			switch {
			case err == nil:
				t.Error("allowed, want denied")
			case !strings.Contains(err.Error(), "miami-controller-webhook-config") || !strings.Contains(err.Error(), f.tag):
				t.Errorf("denied, but not by the policy's %s rule: %v", f.tag, err)
			}
		})
	}
	// The policy fences the controller's ServiceAccount and nobody else. Run
	// last: it changes the configuration.
	for _, f := range forbidden[:3] {
		t.Run("admin: "+f.name, func(t *testing.T) {
			if err := patch(admin, f.body); err != nil {
				t.Errorf("denied: %v", err)
			}
		})
		t.Run("another account: "+f.name, func(t *testing.T) {
			if err := patch(other, f.body); err != nil {
				t.Errorf("denied: %v", err)
			}
		})
	}
}

// jsonString is s as a JSON string literal.
func jsonString(s string) (string, error) {
	b, err := json.Marshal(s)
	return string(b), err
}
