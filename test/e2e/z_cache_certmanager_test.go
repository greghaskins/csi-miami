//go:build e2e

package e2e

// This file's name sorts after every other test file's, so that
// TestCacheCertManagerComponent, which changes the whole driver deployment and
// puts it back, runs last.

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// certManagerManifest is the release manifest of a pinned cert-manager. It is
// applied from a URL without a checksum: acceptable only for a throwaway kind
// cluster in CI, and not to be copied into deploy/.
const certManagerManifest = "https://github.com/cert-manager/cert-manager/releases/download/v1.19.1/cert-manager.yaml"

// managedSecret is the Secret the cert-manager component's Certificate fills.
const managedSecret = "miami-webhook-tls-managed" //nolint:gosec // a Secret's name

// managedArg is the controller argument the component adds.
const managedArg = "--webhook-tls-secret=" + managedSecret

// With cert-manager owning the webhook's certificate, the CA is injected into
// the webhook configuration, the webhook serves, and a certificate that is
// reissued is picked up without a restart. cert-manager is installed for the
// test and removed at the end, and the plain e2e overlay is put back.
func TestCacheCertManagerComponent(t *testing.T) {
	e := loadEnv(t)
	e.needCache(t)
	n := e.newVolumeNamespace(t)
	class := e.volumeClass(t, volumeClassEncrypted)
	n.boundClaims(t, class, "data")

	// Whatever happens, leave the cluster as it was found.
	restored := false
	restore := func() {
		if restored {
			return
		}
		restored = true
		e.revertCertManager(t)
	}
	t.Cleanup(restore)

	// Install cert-manager and wait for its deployments.
	e.kubectl(t, 5*time.Minute, "apply", "-f", certManagerManifest)
	e.kubectl(t, 6*time.Minute, "-n", "cert-manager", "wait", "--for=condition=Available", "--timeout=300s", "deployment", "--all")

	// The base with the component.
	before := e.controllerUID(t)
	e.applyOverlay(t, true)
	e.waitControllerRolled(t, before, true)

	var managedCA []byte
	e.waitFor(t, "Secret "+managedSecret+" to be issued by cert-manager", func(ctx context.Context) (string, bool, error) {
		s, err := e.Client.CoreV1().Secrets(e.DriverNS).Get(ctx, managedSecret, metav1.GetOptions{})
		if err != nil {
			return "", false, err
		}
		managedCA = s.Data["ca.crt"]
		return fmt.Sprintf("keys %d", len(s.Data)), len(managedCA) > 0 && len(s.Data["tls.crt"]) > 0, nil
	})
	e.waitBundle(t, managedCA, "the managed Secret's ca.crt, injected by cert-manager")

	probe := 0
	mutated := func(why string) {
		t.Helper()
		e.waitFor(t, "a pod to be mutated "+why, func(ctx context.Context) (string, bool, error) {
			probe++
			name := fmt.Sprintf("probe%d", probe)
			p, err := e.Client.CoreV1().Pods(n.Name).Create(ctx, n.cachePodObject(cachePod{Name: name, Claims: []string{"data"}, Never: true}), metav1.CreateOptions{})
			if err != nil {
				return "", false, err
			}
			req, lim := cacheRequest(p.Spec.Containers[0])
			_ = e.Client.CoreV1().Pods(n.Name).Delete(ctx, name, metav1.DeleteOptions{})
			return fmt.Sprintf("request %q limit %q", req, lim), req == cacheFootprint && lim == cacheFootprint, nil
		})
	}
	mutated("through the webhook served with cert-manager's certificate")
	t.Logf("a pod with a bound claim gets %s through the webhook under the component", cacheFootprint)

	// Rotation: delete the issued Secret, so cert-manager issues another
	// certificate; the controller, never restarted, serves it.
	ctrl := e.controllerUID(t)
	oldSerial := e.servedSerial(t)
	ctx, cancel := e.withDeadline()
	err := e.Client.CoreV1().Secrets(e.DriverNS).Delete(ctx, managedSecret, metav1.DeleteOptions{})
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	var newSerial string
	e.waitFor(t, "cert-manager to reissue "+managedSecret, func(ctx context.Context) (string, bool, error) {
		s, err := e.Client.CoreV1().Secrets(e.DriverNS).Get(ctx, managedSecret, metav1.GetOptions{})
		if err != nil {
			return "", false, err
		}
		cert, err := parseLeaf(s.Data["tls.crt"])
		if err != nil {
			return err.Error(), false, nil
		}
		newSerial = cert.SerialNumber.String()
		return "serial " + newSerial, newSerial != oldSerial && bytes.Equal(s.Data["ca.crt"], managedCA), nil
	})
	e.waitFor(t, "the webhook to serve the reissued certificate without a restart", func(_ context.Context) (string, bool, error) {
		got, err := e.servedSerialErr()
		return fmt.Sprintf("serving %s, want %s (was %s)", got, newSerial, oldSerial), err == nil && got == newSerial, nil
	})
	if now := e.controllerUID(t); now != ctrl {
		t.Errorf("the controller pod changed from %s to %s: the certificate was not reloaded in place", ctrl, now)
	}
	e.waitBundle(t, managedCA, "the same CA after the rotation")
	mutated("after the rotation")
	t.Logf("the webhook serves serial %s (was %s) in the same controller pod and still mutates", newSerial, oldSerial)

	restore()
}

// kubectl runs kubectl against the cluster under test.
func (e *env) kubectl(t *testing.T, d time.Duration, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	out, err := run(ctx, "kubectl", args...)
	if err != nil {
		t.Fatalf("kubectl %s: %v", strings.Join(args, " "), err)
	}
	return out
}

// kubectlQuiet is kubectl for a cleanup: it logs a failure and goes on.
func (e *env) kubectlQuiet(t *testing.T, d time.Duration, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	if _, err := run(ctx, "kubectl", args...); err != nil {
		t.Errorf("kubectl %s: %v", strings.Join(args, " "), err)
	}
}

// controllerUID is the UID of the controller pod.
func (e *env) controllerUID(t *testing.T) string {
	t.Helper()
	ctx, cancel := e.withDeadline()
	defer cancel()
	p, err := e.controllerPod(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return string(p.UID)
}

// applyOverlay applies the e2e overlay for this run, with the cert-manager
// component or without it. The image is the one the controller runs now.
func (e *env) applyOverlay(t *testing.T, component bool) {
	t.Helper()
	ctx, cancel := e.withDeadline()
	d, err := e.Client.AppsV1().Deployments(e.DriverNS).Get(ctx, "miami-controller", metav1.GetOptions{})
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	image := ""
	for _, c := range d.Spec.Template.Spec.Containers {
		if c.Name == "miami" {
			image = c.Image
		}
	}
	if image == "" {
		t.Fatal("the controller has no miami container")
	}
	dir := t.TempDir()
	root := repoRoot(t)
	script := filepath.Join(root, "hack", "e2e-overlay.sh")
	sctx, scancel := context.WithTimeout(context.Background(), time.Minute)
	defer scancel()
	if out, err := run(sctx, script, "--image", image, "--nfs-server", e.NFSServer, "--out", dir); err != nil {
		t.Fatalf("hack/e2e-overlay.sh: %v\n%s", err, out)
	}
	if component {
		rel, err := filepath.Rel(dir, filepath.Join(root, "deploy", "kustomize", "components", "cert-manager"))
		if err != nil {
			t.Fatal(err)
		}
		f, err := os.OpenFile(filepath.Join(dir, "kustomization.yaml"), os.O_APPEND|os.O_WRONLY, 0o600) //nolint:gosec // a file in the test's own temp dir
		if err != nil {
			t.Fatal(err)
		}
		_, err = fmt.Fprintf(f, "components:\n  - %s\n", rel)
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	// The cert-manager webhook may not be serving the first time its kinds are
	// created, and the API server's view of its CRDs may lag.
	var last error
	for i := 0; i < 6; i++ {
		actx, acancel := context.WithTimeout(context.Background(), 2*time.Minute)
		out, err := run(actx, "kubectl", "apply", "-k", dir)
		acancel()
		if err == nil {
			t.Logf("applied the overlay (component %v):\n%s", component, tail(out, 1500))
			return
		}
		last = err
		time.Sleep(10 * time.Second)
	}
	t.Fatalf("applying the overlay (component %v): %v", component, last)
}

// waitControllerRolled waits for a new controller pod, Ready, whose arguments
// do or do not name the managed Secret.
func (e *env) waitControllerRolled(t *testing.T, old string, managed bool) {
	t.Helper()
	e.waitFor(t, fmt.Sprintf("the controller to roll to a new pod (managed secret %v)", managed), func(ctx context.Context) (string, bool, error) {
		p, err := e.controllerPod(ctx)
		if err != nil {
			return err.Error(), false, nil
		}
		has := false
		for _, c := range p.Spec.Containers {
			if c.Name == "miami" && slices.Contains(c.Args, managedArg) {
				has = true
			}
		}
		return fmt.Sprintf("pod %s (old %s) ready %v, managed arg %v", p.Name, old, podReady(p), has),
			string(p.UID) != old && podReady(p) && has == managed, nil
	})
}

// waitBundle waits for every webhook's caBundle to be ca.
func (e *env) waitBundle(t *testing.T, ca []byte, what string) {
	t.Helper()
	e.waitFor(t, "the caBundle of miami-cache to be "+what, func(ctx context.Context) (string, bool, error) {
		bundles, err := e.webhookBundles(ctx)
		if err != nil {
			return "", false, err
		}
		for _, b := range bundles {
			if len(b) == 0 || !bytes.Equal(b, ca) {
				return fmt.Sprintf("a bundle of %d bytes, want %d bytes", len(b), len(ca)), false, nil
			}
		}
		return "equal", len(bundles) > 0, nil
	})
}

// parseLeaf reads the first certificate of a PEM bundle.
func parseLeaf(pemBytes []byte) (*x509.Certificate, error) {
	rest := pemBytes
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			return nil, fmt.Errorf("no certificate in %d bytes", len(pemBytes))
		}
		if block.Type == "CERTIFICATE" {
			return x509.ParseCertificate(block.Bytes)
		}
	}
}

// servedSerial is the serial of the certificate the controller's webhook
// serves now, read over a connection to the controller's host address.
func (e *env) servedSerial(t *testing.T) string {
	t.Helper()
	s, err := e.servedSerialErr()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func (e *env) servedSerialErr() (string, error) {
	ctx, cancel := e.withDeadline()
	defer cancel()
	p, err := e.controllerPod(ctx)
	if err != nil {
		return "", err
	}
	if p.Status.HostIP == "" {
		return "", fmt.Errorf("controller pod %s has no host IP yet", p.Name)
	}
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 10 * time.Second}, "tcp", net.JoinHostPort(p.Status.HostIP, "9814"),
		&tls.Config{InsecureSkipVerify: true}) //nolint:gosec // only the served certificate's serial is read
	if err != nil {
		return "", err
	}
	defer func() { _ = conn.Close() }()
	certs := conn.ConnectionState().PeerCertificates
	if len(certs) == 0 {
		return "", fmt.Errorf("the webhook served no certificate")
	}
	return certs[0].SerialNumber.String(), nil
}

// revertCertManager puts the cluster back: the plain overlay, a controller
// that serves its own certificate, no cert-manager objects and no cert-manager.
func (e *env) revertCertManager(t *testing.T) {
	t.Helper()
	managed := false
	ctx, cancel := e.withDeadline()
	if p, err := e.controllerPod(ctx); err == nil {
		for _, c := range p.Spec.Containers {
			managed = managed || slices.Contains(c.Args, managedArg)
		}
	}
	cancel()
	old := e.controllerUID(t)
	e.applyOverlay(t, false)
	if managed {
		e.waitControllerRolled(t, old, false)
	}
	// The objects the component made, then cert-manager itself.
	e.kubectlQuiet(t, 3*time.Minute, "-n", e.DriverNS, "delete", "--ignore-not-found", "certificates.cert-manager.io,issuers.cert-manager.io", "--all")
	e.kubectlQuiet(t, 2*time.Minute, "-n", e.DriverNS, "delete", "--ignore-not-found", "secret", managedSecret, "miami-webhook-ca")
	e.kubectlQuiet(t, 6*time.Minute, "delete", "--ignore-not-found", "-f", certManagerManifest)

	// The controller's own CA is back in the configuration.
	var ca []byte
	e.waitFor(t, "the controller's own Secret miami-webhook-tls", func(ctx context.Context) (string, bool, error) {
		s, err := e.Client.CoreV1().Secrets(e.DriverNS).Get(ctx, "miami-webhook-tls", metav1.GetOptions{})
		if err != nil {
			return "", false, err
		}
		ca = s.Data["ca.crt"]
		return fmt.Sprintf("ca.crt %d bytes", len(ca)), len(ca) > 0, nil
	})
	e.waitBundle(t, ca, "the controller's own CA again")
}
