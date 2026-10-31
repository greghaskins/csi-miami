package csi

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
)

// DefaultStopTimeout bounds a graceful stop when ServerOptions.StopTimeout is
// not positive.
const DefaultStopTimeout = 10 * time.Second

// ParseEndpoint returns the socket path of a unix endpoint, "unix:///path" or
// "unix:/path". Any other endpoint, and any path that is not absolute and
// cleaned, is an error: the services listen on a unix socket only.
func ParseEndpoint(endpoint string) (string, error) {
	var p string
	switch {
	case strings.HasPrefix(endpoint, "unix://"):
		p = strings.TrimPrefix(endpoint, "unix://")
	case strings.HasPrefix(endpoint, "unix:"):
		p = strings.TrimPrefix(endpoint, "unix:")
	default:
		return "", fmt.Errorf("csi: endpoint %q must be unix:///<absolute path>", endpoint)
	}
	if !filepath.IsAbs(p) || filepath.Clean(p) != p {
		return "", fmt.Errorf("csi: endpoint %q must name an absolute, cleaned socket path", endpoint)
	}
	return p, nil
}

// ServerOptions configure a Server.
type ServerOptions struct {
	// Endpoint is where to listen: unix:///<absolute path>.
	Endpoint string

	// Identity is required. Controller and Node are registered when not nil.
	Identity   csi.IdentityServer
	Controller csi.ControllerServer
	Node       csi.NodeServer

	// Interceptors run around every call, after the logging interceptor and
	// outside the recover interceptor, in order: the first is outermost. A
	// metrics interceptor goes here. Because the recover interceptor is
	// innermost, they see a panic as the Internal error it becomes.
	Interceptors []grpc.UnaryServerInterceptor

	// Log receives the server's messages. Nil means slog.Default().
	Log *slog.Logger

	// StopTimeout bounds a graceful stop: calls still running after it are cut
	// off. Zero or negative means DefaultStopTimeout.
	StopTimeout time.Duration
}

// Server serves the CSI services on a unix socket.
type Server struct {
	path        string
	log         *slog.Logger
	stopTimeout time.Duration
	grpc        *grpc.Server
	lis         net.Listener
}

// NewServer builds a server and registers the services. It does not listen.
func NewServer(o ServerOptions) (*Server, error) {
	path, err := ParseEndpoint(o.Endpoint)
	if err != nil {
		return nil, err
	}
	if o.Identity == nil {
		return nil, errors.New("csi: an identity service is required")
	}
	log := o.Log
	if log == nil {
		log = slog.Default()
	}
	stop := o.StopTimeout
	if stop <= 0 {
		stop = DefaultStopTimeout
	}

	chain := []grpc.UnaryServerInterceptor{LoggingInterceptor(log)}
	chain = append(chain, o.Interceptors...)
	chain = append(chain, RecoverInterceptor(log))
	g := grpc.NewServer(grpc.ChainUnaryInterceptor(chain...))

	csi.RegisterIdentityServer(g, o.Identity)
	if o.Controller != nil {
		csi.RegisterControllerServer(g, o.Controller)
	}
	if o.Node != nil {
		csi.RegisterNodeServer(g, o.Node)
	}
	return &Server{path: path, log: log, stopTimeout: stop, grpc: g}, nil
}

// Listen creates the socket. A stale socket file left by an earlier process
// is removed first; anything else at the path (a regular file, a directory) is
// an error and is left alone, because the endpoint is configuration and a
// mistake in it must not delete data.
func (s *Server) Listen() error {
	if s.lis != nil {
		return errors.New("csi: already listening")
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o750); err != nil {
		return fmt.Errorf("csi: %w", err)
	}
	if fi, err := os.Lstat(s.path); err == nil {
		if fi.Mode()&fs.ModeSocket == 0 {
			return fmt.Errorf("csi: %s exists and is not a socket", s.path)
		}
		if err := os.Remove(s.path); err != nil {
			return fmt.Errorf("csi: removing the stale socket: %w", err)
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("csi: %w", err)
	}
	lis, err := net.Listen("unix", s.path)
	if err != nil {
		return fmt.Errorf("csi: %w", err)
	}
	s.lis = lis
	return nil
}

// Serve serves on the socket made by Listen until ctx ends, then stops
// gracefully: it stops accepting, waits for running calls up to the stop
// timeout, and cuts off any that remain. It returns nil after a requested
// stop, and the error if serving failed first.
func (s *Server) Serve(ctx context.Context) error {
	if s.lis == nil {
		return errors.New("csi: Serve before Listen")
	}
	failed := make(chan error, 1)
	go func() { failed <- s.grpc.Serve(s.lis) }()

	select {
	case err := <-failed:
		return err
	case <-ctx.Done():
	}

	s.log.Info("stopping")
	stopped := make(chan struct{})
	go func() {
		s.grpc.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(s.stopTimeout):
		s.log.Warn("graceful stop timed out; cutting off running calls", "timeout", s.stopTimeout)
		s.grpc.Stop()
	}
	if err := <-failed; err != nil { // Serve has returned
		s.log.Debug("serve ended", "error", err)
	}
	return nil
}

// Run is Listen followed by Serve.
func (s *Server) Run(ctx context.Context) error {
	if err := s.Listen(); err != nil {
		return err
	}
	return s.Serve(ctx)
}
