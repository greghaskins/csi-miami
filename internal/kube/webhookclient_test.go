package kube

import (
	"testing"

	"k8s.io/client-go/rest"
)

func TestWebhookClientHasItsOwnRateLimit(t *testing.T) {
	t.Parallel()
	in := &rest.Config{Host: "https://example.invalid", QPS: 1, Burst: 1, UserAgent: "x"}
	c := webhookRestConfig(in)
	if c.QPS != webhookClientQPS || c.Burst != webhookClientBurst || c.UserAgent != "csi-miami-webhook" {
		t.Fatalf("QPS %v, burst %d, agent %q", c.QPS, c.Burst, c.UserAgent)
	}
	if in.QPS != 1 || in.Burst != 1 || in.UserAgent != "x" {
		t.Fatal("the input config was modified")
	}
	if c.Host != in.Host {
		t.Fatal("the connection settings were not carried over")
	}
}

func TestNewWebhookClientNeedsAConfig(t *testing.T) {
	t.Parallel()
	if _, err := NewWebhookClient(nil, 0); err == nil {
		t.Fatal("nil config accepted")
	}
	if _, err := NewWebhookClient(&rest.Config{Host: "https://example.invalid"}, 0); err != nil {
		t.Fatal(err)
	}
}
