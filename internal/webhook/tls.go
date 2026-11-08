package webhook

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/big"
	"slices"
	"sync/atomic"
	"time"

	"github.com/greghaskins/csi-miami/internal/kube"
)

// Secrets is the part of the API the webhook's TLS needs: the Secret in the
// driver's namespace. *kube.Client implements it.
type Secrets interface {
	GetSecret(ctx context.Context, namespace, name string) (*kube.Secret, error)
	// CreateSecret wraps kube.ErrAlreadyExists when the Secret exists.
	CreateSecret(ctx context.Context, s *kube.Secret) error
	// UpdateSecret sends s.ResourceVersion and wraps kube.ErrConflict when the
	// Secret has changed since.
	UpdateSecret(ctx context.Context, s *kube.Secret) error
}

var _ Secrets = (*kube.Client)(nil)

// The data keys of the Secret. The first three are the keys cert-manager
// writes; ca.key is ours alone, so a renewal can keep the CA.
const (
	keyCert  = "tls.crt"
	keyKey   = "tls.key"
	keyCA    = "ca.crt"
	keyCAKey = "ca.key"
)

const (
	caLifetime   = 10 * 365 * 24 * time.Hour
	leafLifetime = 365 * 24 * time.Hour
	// renewBefore is how long before expiry a certificate is replaced.
	renewBefore = 30 * 24 * time.Hour
	// backdate covers clock skew between the controller and the API server.
	backdate = 5 * time.Minute
	// maxAttempts bounds the read-modify-write loop against a concurrent
	// replica: each lost race re-reads, so a few rounds always converge.
	maxAttempts = 5
)

// errNotReady is a managed Secret that is absent or incomplete: someone else
// has yet to write it, and the next pass tries again.
var errNotReady = errors.New("webhook: the TLS Secret is not ready")

// Material is what serving needs: the certificate with its key, and the CA
// (PEM) that signed it, which is the caBundle.
type Material struct {
	Cert tls.Certificate
	CA   []byte
}

// TLSSource produces the serving material from the Secret.
//
// Self-signed (Managed false): it creates the Secret when absent and repairs
// it when unusable, keeping a usable CA across serving-certificate renewals.
// Writes are conditional on the resourceVersion read, so a replica that loses
// a race re-reads and adopts the winner's material when it is valid; it never
// overwrites a different CA it has not looked at.
//
// Managed: someone else owns the Secret. The source only reads it and never
// writes, however broken it is.
type TLSSource struct {
	Secrets   Secrets
	Namespace string
	Name      string
	// Service is the Service whose names the serving certificate carries.
	Service string
	Managed bool
	// Now is the clock; nil means time.Now.
	Now func() time.Time
	Log *slog.Logger
}

func (s *TLSSource) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *TLSSource) log() *slog.Logger {
	if s.Log != nil {
		return s.Log
	}
	return slog.Default()
}

// wantNames are the names the serving certificate must carry, exactly.
func (s *TLSSource) wantNames() []string {
	base := s.Service + "." + s.Namespace + ".svc"
	return []string{base, base + ".cluster.local"}
}

// Load returns the material in the Secret, writing it first when self-signed
// and it needs it.
func (s *TLSSource) Load(ctx context.Context) (*Material, error) {
	if s.Managed {
		return s.loadManaged(ctx)
	}
	for range maxAttempts {
		sec, err := s.Secrets.GetSecret(ctx, s.Namespace, s.Name)
		if errors.Is(err, kube.ErrNotFound) {
			sec, err = nil, nil
		}
		if err != nil {
			return nil, err
		}
		var data map[string][]byte
		if sec != nil {
			data = sec.Data
		}
		m, ca, err := s.assess(data)
		if err != nil {
			return nil, err
		}
		if m != nil {
			return m, nil
		}
		fresh, m, err := s.generate(ca)
		if err != nil {
			return nil, err
		}
		if sec == nil {
			err = s.Secrets.CreateSecret(ctx, &kube.Secret{Namespace: s.Namespace, Name: s.Name, Type: kube.SecretTypeTLS, Data: fresh})
			if err == nil {
				s.log().Info("created the webhook TLS Secret", "secret", s.Name)
			}
		} else {
			next := *sec
			next.Data = maps.Clone(sec.Data)
			if next.Data == nil {
				next.Data = map[string][]byte{}
			}
			maps.Copy(next.Data, fresh)
			err = s.Secrets.UpdateSecret(ctx, &next)
			if err == nil {
				s.log().Info("renewed the webhook TLS material", "secret", s.Name)
			}
		}
		switch {
		case err == nil:
			return m, nil
		case errors.Is(err, kube.ErrAlreadyExists), errors.Is(err, kube.ErrConflict):
			// Another replica wrote first: look at what it wrote.
			continue
		default:
			return nil, err
		}
	}
	return nil, errors.New("webhook: the TLS Secret kept changing under us; will retry on the next pass")
}

// loadManaged reads a Secret someone else manages. It never writes.
func (s *TLSSource) loadManaged(ctx context.Context) (*Material, error) {
	sec, err := s.Secrets.GetSecret(ctx, s.Namespace, s.Name)
	if errors.Is(err, kube.ErrNotFound) {
		return nil, fmt.Errorf("%w: Secret %q does not exist yet", errNotReady, s.Name)
	}
	if err != nil {
		return nil, err
	}
	for _, k := range []string{keyCert, keyKey, keyCA} {
		if len(sec.Data[k]) == 0 {
			return nil, fmt.Errorf("%w: Secret %q has no %s", errNotReady, s.Name, k)
		}
	}
	pair, err := tls.X509KeyPair(sec.Data[keyCert], sec.Data[keyKey])
	if err != nil {
		return nil, fmt.Errorf("%w: Secret %q holds no usable key pair", errNotReady, s.Name)
	}
	if b, _ := pem.Decode(sec.Data[keyCA]); b == nil {
		return nil, fmt.Errorf("%w: Secret %q holds no usable CA", errNotReady, s.Name)
	}
	return &Material{Cert: pair, CA: slices.Clone(sec.Data[keyCA])}, nil
}

// assess says what the Secret's data is worth. It returns the material when
// all of it is valid; otherwise nil material and, when the CA in the data is
// usable, that CA to keep. Unparseable or inconsistent data is never an error:
// it is simply replaced.
func (s *TLSSource) assess(data map[string][]byte) (*Material, *caPair, error) {
	now := s.now()
	ca := parseCA(data, now)
	if ca == nil {
		return nil, nil, nil
	}
	pair, err := tls.X509KeyPair(data[keyCert], data[keyKey])
	if err != nil {
		return nil, ca, nil
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return nil, ca, nil
	}
	if leaf.CheckSignatureFrom(ca.cert) != nil ||
		!slices.Equal(leaf.DNSNames, s.wantNames()) || len(leaf.IPAddresses) != 0 || len(leaf.URIs) != 0 || len(leaf.EmailAddresses) != 0 ||
		now.Before(leaf.NotBefore) || !now.Add(renewBefore).Before(leaf.NotAfter) {
		return nil, ca, nil
	}
	pair.Leaf = leaf
	return &Material{Cert: pair, CA: slices.Clone(data[keyCA])}, ca, nil
}

type caPair struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

// parseCA returns the CA in data when it parses, its key matches, and it is
// not within renewBefore of its end.
func parseCA(data map[string][]byte, now time.Time) *caPair {
	b, _ := pem.Decode(data[keyCA])
	if b == nil {
		return nil
	}
	cert, err := x509.ParseCertificate(b.Bytes)
	if err != nil || !cert.IsCA || now.Before(cert.NotBefore) || !now.Add(renewBefore).Before(cert.NotAfter) {
		return nil
	}
	kb, _ := pem.Decode(data[keyCAKey])
	if kb == nil {
		return nil
	}
	k, err := x509.ParsePKCS8PrivateKey(kb.Bytes)
	if err != nil {
		return nil
	}
	key, ok := k.(*ecdsa.PrivateKey)
	if !ok {
		return nil
	}
	pub, ok := cert.PublicKey.(*ecdsa.PublicKey)
	if !ok || !pub.Equal(&key.PublicKey) {
		return nil
	}
	return &caPair{cert: cert, key: key, pem: data[keyCA]}
}

// generate makes a serving certificate under keep (a new CA when nil) and
// returns the Secret data and the material that data holds.
func (s *TLSSource) generate(keep *caPair) (map[string][]byte, *Material, error) {
	now := s.now()
	data := map[string][]byte{}
	ca := keep
	if ca == nil {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return nil, nil, err
		}
		serial, err := serialNumber()
		if err != nil {
			return nil, nil, err
		}
		tmpl := &x509.Certificate{
			SerialNumber: serial, Subject: pkix.Name{CommonName: "miami-webhook-ca"},
			NotBefore: now.Add(-backdate), NotAfter: now.Add(caLifetime),
			IsCA: true, BasicConstraintsValid: true, MaxPathLenZero: true,
			KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
		if err != nil {
			return nil, nil, err
		}
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			return nil, nil, err
		}
		ca = &caPair{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
	}
	caKeyDER, err := x509.MarshalPKCS8PrivateKey(ca.key)
	if err != nil {
		return nil, nil, err
	}
	data[keyCA] = slices.Clone(ca.pem)
	data[keyCAKey] = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: caKeyDER})

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	serial, err := serialNumber()
	if err != nil {
		return nil, nil, err
	}
	names := s.wantNames()
	tmpl := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: names[0]},
		NotBefore: now.Add(-backdate), NotAfter: now.Add(leafLifetime),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames: names, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		return nil, nil, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	data[keyCert] = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	data[keyKey] = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	pair, err := tls.X509KeyPair(data[keyCert], data[keyKey])
	if err != nil {
		return nil, nil, err
	}
	return data, &Material{Cert: pair, CA: slices.Clone(ca.pem)}, nil
}

func serialNumber() (*big.Int, error) {
	return rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
}

// CertStore holds the certificate being served and hands it to
// tls.Config.GetCertificate, so a renewal needs no restart.
type CertStore struct {
	cur atomic.Pointer[tls.Certificate]
}

// Set replaces the certificate served from now on.
func (c *CertStore) Set(cert *tls.Certificate) { c.cur.Store(cert) }

// GetCertificate implements tls.Config.GetCertificate. Before the first
// certificate is loaded it fails the handshake.
func (c *CertStore) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	if cert := c.cur.Load(); cert != nil {
		return cert, nil
	}
	return nil, errors.New("webhook: no serving certificate yet")
}

// sameBundle reports whether two bundles are the same bytes.
func sameBundle(a, b []byte) bool { return bytes.Equal(a, b) }
