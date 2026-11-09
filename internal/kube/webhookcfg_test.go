package kube

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func secretJSON(rv string, data map[string]string) map[string]any {
	d := map[string]any{}
	for k, v := range data {
		d[k] = base64.StdEncoding.EncodeToString([]byte(v))
	}
	return map[string]any{"apiVersion": "v1", "kind": "Secret", "type": "kubernetes.io/tls",
		"metadata": map[string]any{"name": "s", "namespace": "ns", "resourceVersion": rv}, "data": d}
}

func TestGetSecretDecodesAndMapsNotFound(t *testing.T) {
	t.Parallel()
	c, _ := recordingServer(t, 200, secretJSON("7", map[string]string{"tls.crt": "abc"}))
	s, err := c.GetSecret(context.Background(), "ns", "s")
	if err != nil || string(s.Data["tls.crt"]) != "abc" || s.ResourceVersion != "7" || s.Type != SecretTypeTLS {
		t.Fatalf("GetSecret = %+v, %v", s, err)
	}
	c, _ = recordingServer(t, 404, notFound("secrets", "s"))
	if _, err := c.GetSecret(context.Background(), "ns", "s"); !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
	// A 404 that is not the API server's own answer is not "absent".
	cfg := apiServer(t, func(w http.ResponseWriter, _ *http.Request) { http.NotFound(w, nil) })
	c, _ = NewClient(cfg, time.Second)
	if _, err := c.GetSecret(context.Background(), "ns", "s"); err == nil || errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want an ordinary error", err)
	}
}

func TestCreateAndUpdateSecret(t *testing.T) {
	t.Parallel()
	c, call := recordingServer(t, 201, secretJSON("1", nil))
	s := &Secret{Namespace: "ns", Name: "s", Type: SecretTypeTLS, Data: map[string][]byte{"k": []byte("v")}}
	if err := c.CreateSecret(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	got := call()
	if got.method != http.MethodPost || got.path != "/api/v1/namespaces/ns/secrets" ||
		!strings.Contains(got.body, `"type":"kubernetes.io/tls"`) || !strings.Contains(got.body, base64.StdEncoding.EncodeToString([]byte("v"))) {
		t.Errorf("create = %+v", got)
	}

	c, _ = recordingServer(t, 409, metav1.Status{TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}, Status: "Failure", Code: 409, Reason: metav1.StatusReasonAlreadyExists})
	if err := c.CreateSecret(context.Background(), s); !errors.Is(err, ErrAlreadyExists) {
		t.Errorf("create err = %v, want ErrAlreadyExists", err)
	}

	if err := c.UpdateSecret(context.Background(), s); err == nil {
		t.Error("an update with no resourceVersion was sent")
	}
	s.ResourceVersion = "5"
	c, _ = recordingServer(t, 409, metav1.Status{TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}, Status: "Failure", Code: 409, Reason: metav1.StatusReasonConflict})
	if err := c.UpdateSecret(context.Background(), s); !errors.Is(err, ErrConflict) {
		t.Errorf("update err = %v, want ErrConflict", err)
	}
	c, call = recordingServer(t, 200, secretJSON("6", nil))
	if err := c.UpdateSecret(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if got := call(); got.method != http.MethodPut || !strings.Contains(got.body, `"resourceVersion":"5"`) {
		t.Errorf("update = %+v", got)
	}
}

// TestUpdateSecretKeepsTheRestOfTheObject: the update replaces the data and
// nothing else, so labels, annotations and owner references someone put on the
// Secret (a GitOps tool's tracking label, say) survive a renewal.
func TestUpdateSecretKeepsTheRestOfTheObject(t *testing.T) {
	t.Parallel()
	obj := secretJSON("7", map[string]string{"tls.crt": "old"})
	obj["metadata"] = map[string]any{
		"name": "s", "namespace": "ns", "resourceVersion": "7",
		"labels":          map[string]any{"example.com/l": "1", SecretOwnerLabel: SecretOwnerValue},
		"annotations":     map[string]any{"example.com/a": "2"},
		"finalizers":      []any{"example.com/f"},
		"ownerReferences": []any{map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "name": "o", "uid": "u-1"}},
	}
	c, call := recordingServer(t, 200, obj)
	s, err := c.GetSecret(context.Background(), "ns", "s")
	if err != nil {
		t.Fatal(err)
	}
	if s.Labels["example.com/l"] != "1" || s.Labels[SecretOwnerLabel] != SecretOwnerValue {
		t.Errorf("labels = %v", s.Labels)
	}
	next := *s
	next.Data = map[string][]byte{"tls.crt": []byte("new")}
	if err := c.UpdateSecret(context.Background(), &next); err != nil {
		t.Fatal(err)
	}
	got := call()
	if got.method != http.MethodPut {
		t.Fatalf("update = %+v", got)
	}
	for _, want := range []string{`"example.com/l":"1"`, `"example.com/a":"2"`, `"example.com/f"`, `"u-1"`, `"resourceVersion":"7"`, base64.StdEncoding.EncodeToString([]byte("new"))} {
		if !strings.Contains(got.body, want) {
			t.Errorf("the update body lacks %s: %s", want, got.body)
		}
	}
	if strings.Contains(got.body, base64.StdEncoding.EncodeToString([]byte("old"))) {
		t.Errorf("the old data was sent: %s", got.body)
	}
}

func TestCreateSecretCarriesItsLabels(t *testing.T) {
	t.Parallel()
	c, call := recordingServer(t, 201, secretJSON("1", nil))
	s := &Secret{Namespace: "ns", Name: "s", Type: SecretTypeTLS, Labels: map[string]string{SecretOwnerLabel: SecretOwnerValue}}
	if err := c.CreateSecret(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if got := call(); !strings.Contains(got.body, `"`+SecretOwnerLabel+`":"`+SecretOwnerValue+`"`) {
		t.Errorf("create body lacks the label: %s", got.body)
	}
}

func TestWebhookConfigReadAndPatch(t *testing.T) {
	t.Parallel()
	cfgObj := map[string]any{"apiVersion": "admissionregistration.k8s.io/v1", "kind": "MutatingWebhookConfiguration",
		"metadata": map[string]any{"name": "c"},
		"webhooks": []any{
			map[string]any{"name": "a", "clientConfig": map[string]any{"caBundle": base64.StdEncoding.EncodeToString([]byte("CA"))}},
			map[string]any{"name": "b", "clientConfig": map[string]any{}},
		}}
	c, _ := recordingServer(t, 200, cfgObj)
	got, err := c.GetMutatingWebhookConfiguration(context.Background(), "c")
	if err != nil || len(got.CABundles) != 2 || string(got.CABundles[0]) != "CA" || len(got.CABundles[1]) != 0 {
		t.Fatalf("get = %+v, %v", got, err)
	}
	c, _ = recordingServer(t, 404, notFound("mutatingwebhookconfigurations", "c"))
	if _, err := c.GetMutatingWebhookConfiguration(context.Background(), "c"); !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v", err)
	}

	c, call := recordingServer(t, 200, cfgObj)
	if err := c.PatchCABundle(context.Background(), "c", []int{0, 1}, []byte("NEW")); err != nil {
		t.Fatal(err)
	}
	v := base64.StdEncoding.EncodeToString([]byte("NEW"))
	want := `[{"op":"add","path":"/webhooks/0/clientConfig/caBundle","value":"` + v + `"},{"op":"add","path":"/webhooks/1/clientConfig/caBundle","value":"` + v + `"}]`
	g := call()
	if g.method != http.MethodPatch || g.contentType != "application/json-patch+json" || g.body != want {
		t.Errorf("patch = %+v, want body %s", g, want)
	}
}
