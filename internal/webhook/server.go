package webhook

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// MutatePath is the only path served; the webhook configuration names it.
	MutatePath = "/mutate"

	// maxBodyBytes bounds a request body. A pod is far below this (the API
	// server limits an object to about 1.5 MiB, and the review carries one
	// pod); the bound keeps a stray client from making the controller buffer
	// more.
	maxBodyBytes = 1 << 20

	admissionAPIVersion = "admission.k8s.io/v1"
	admissionKind       = "AdmissionReview"

	shutdownTimeout = 5 * time.Second
)

// Admitter answers one admission request. *Mutator is one.
type Admitter interface {
	Mutate(ctx context.Context, req *admissionv1.AdmissionRequest) *admissionv1.AdmissionResponse
}

var _ Admitter = (*Mutator)(nil)

// Server serves the admission webhook over TLS at MutatePath.
type Server struct {
	// Addr is the listen address.
	Addr    string
	Mutator Admitter
	// GetCertificate supplies the serving certificate (a CertStore's), so a
	// renewal needs no restart.
	GetCertificate func(*tls.ClientHelloInfo) (*tls.Certificate, error)
	Log            *slog.Logger

	lis net.Listener
}

func (s *Server) log() *slog.Logger {
	if s.Log != nil {
		return s.Log
	}
	return slog.Default()
}

// Listen binds Addr. It is separate from Run so a port in use fails the
// controller at start instead of later.
func (s *Server) Listen() error {
	if s.lis != nil {
		return nil
	}
	lis, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	s.lis = lis
	return nil
}

// ListenAddr returns the address being served on, or nil before Listen.
func (s *Server) ListenAddr() net.Addr {
	if s.lis == nil {
		return nil
	}
	return s.lis.Addr()
}

func (s *Server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(MutatePath, s.serveMutate)
	return mux
}

func (s *Server) httpServer() *http.Server {
	return &http.Server{
		Handler: s.handler(),
		TLSConfig: &tls.Config{
			MinVersion:     tls.VersionTLS12,
			GetCertificate: s.GetCertificate,
		},
		// The API server sends one small request per connection use; these
		// bound a slow or stalled client. WriteTimeout is above the 7s
		// deadline of Mutate.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    64 << 10,
		// Handshake noise (a probe, a client that does not trust the CA) is
		// not an event worth a line each.
		ErrorLog: slog.NewLogLogger(s.log().Handler(), slog.LevelDebug),
	}
}

// Run serves until ctx ends, then shuts down, waiting up to five seconds for
// requests in flight. It returns nil after a requested stop and the error if
// serving failed first.
func (s *Server) Run(ctx context.Context) error {
	if err := s.Listen(); err != nil {
		return err
	}
	srv := s.httpServer()
	failed := make(chan error, 1)
	go func() { failed <- srv.ServeTLS(s.lis, "", "") }()
	select {
	case err := <-failed:
		return err
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		_ = srv.Close()
	}
	if err := <-failed; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *Server) serveMutate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if r.ContentLength > maxBodyBytes {
		http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "could not read the request", http.StatusBadRequest)
		return
	}
	var in admissionv1.AdmissionReview
	if err := json.Unmarshal(body, &in); err != nil {
		http.Error(w, "the body is not an AdmissionReview", http.StatusBadRequest)
		return
	}
	if in.APIVersion != admissionAPIVersion || in.Kind != admissionKind {
		http.Error(w, "only admission.k8s.io/v1 AdmissionReview is served", http.StatusBadRequest)
		return
	}
	if in.Request == nil {
		http.Error(w, "the review has no request", http.StatusBadRequest)
		return
	}
	resp := s.Mutator.Mutate(r.Context(), in.Request)
	if resp == nil {
		resp = &admissionv1.AdmissionResponse{Allowed: true}
	}
	resp.UID = in.Request.UID
	out, err := json.Marshal(admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{APIVersion: admissionAPIVersion, Kind: admissionKind},
		Response: resp,
	})
	if err != nil {
		s.log().Error("encoding the admission response failed", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(out)
}
