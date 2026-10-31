package csi

import (
	"context"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestIdentity(t *testing.T) {
	t.Parallel()
	id := NewIdentity("v1.2.3")
	ctx := context.Background()

	info, err := id.GetPluginInfo(ctx, &csi.GetPluginInfoRequest{})
	if err != nil || info.GetName() != "miami.csi.greghaskins.com" || info.GetVendorVersion() != "v1.2.3" {
		t.Errorf("GetPluginInfo = %v, %v", info, err)
	}

	caps, err := id.GetPluginCapabilities(ctx, &csi.GetPluginCapabilitiesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var got []csi.PluginCapability_Service_Type
	for _, c := range caps.GetCapabilities() {
		got = append(got, c.GetService().GetType())
	}
	want := []csi.PluginCapability_Service_Type{
		csi.PluginCapability_Service_CONTROLLER_SERVICE,
		csi.PluginCapability_Service_VOLUME_ACCESSIBILITY_CONSTRAINTS,
	}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("capabilities = %v, want %v", got, want)
	}

	probe, err := id.Probe(ctx, &csi.ProbeRequest{})
	if err != nil || !probe.GetReady().GetValue() {
		t.Errorf("Probe = %v, %v; want ready", probe, err)
	}
}

func TestParseEndpoint(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in   string
		want string
		bad  bool
	}{
		{in: "unix:///csi/csi.sock", want: "/csi/csi.sock"},
		{in: "unix:/csi/csi.sock", want: "/csi/csi.sock"},
		{in: "tcp://192.0.2.1:9000", bad: true},
		{in: "/csi/csi.sock", bad: true},
		{in: "", bad: true},
		{in: "unix://csi.sock", bad: true},
		{in: "unix:///csi/../csi.sock", bad: true},
		{in: "unix:///csi//csi.sock", bad: true},
	}
	for _, tt := range tests {
		got, err := ParseEndpoint(tt.in)
		if (err != nil) != tt.bad || got != tt.want {
			t.Errorf("ParseEndpoint(%q) = %q, %v; want %q, bad=%v", tt.in, got, err, tt.want, tt.bad)
		}
	}
}

func TestNewServerErrors(t *testing.T) {
	t.Parallel()
	if _, err := NewServer(ServerOptions{Endpoint: "tcp://192.0.2.1:1", Identity: NewIdentity("x")}); err == nil {
		t.Error("a tcp endpoint was accepted")
	}
	if _, err := NewServer(ServerOptions{Endpoint: "unix:///tmp/x.sock"}); err == nil {
		t.Error("no identity service was accepted")
	}
	s, err := NewServer(ServerOptions{Endpoint: "unix:///tmp/x.sock", Identity: NewIdentity("x")})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Serve(context.Background()); err == nil {
		t.Error("Serve before Listen succeeded")
	}
}

// panicky is a controller whose only method panics.
type panicky struct {
	csi.UnimplementedControllerServer
}

func (panicky) ControllerGetCapabilities(context.Context, *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	panic("kaboom with a secret detail")
}

// runServer starts a server on a socket in a temporary directory and returns
// a client connection and a function that stops it and returns Run's result.
func runServer(t *testing.T, o ServerOptions) (*grpc.ClientConn, func() error) {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "csi.sock")
	o.Endpoint = "unix://" + sock
	s, err := NewServer(o)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Listen(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx) }()

	conn, err := grpc.NewClient("unix:"+sock, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	var stopped bool
	stop := func() error {
		if stopped {
			return nil
		}
		stopped = true
		_ = conn.Close()
		cancel()
		return <-done
	}
	t.Cleanup(func() { _ = stop() })
	return conn, stop
}

func TestServerServesAndStops(t *testing.T) {
	t.Parallel()
	logs := &syncBuffer{}
	var mu sync.Mutex
	var seen []string
	metrics := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		mu.Lock()
		seen = append(seen, info.FullMethod)
		mu.Unlock()
		return h(ctx, req)
	}
	e := newEnv(t)
	conn, stop := runServer(t, ServerOptions{
		Identity:     NewIdentity("v9"),
		Controller:   e.ctrl,
		Interceptors: []grpc.UnaryServerInterceptor{metrics},
		Log:          slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	info, err := csi.NewIdentityClient(conn).GetPluginInfo(ctx, &csi.GetPluginInfoRequest{})
	if err != nil || info.GetVendorVersion() != "v9" {
		t.Fatalf("GetPluginInfo = %v, %v", info, err)
	}
	caps, err := csi.NewControllerClient(conn).ControllerGetCapabilities(ctx, &csi.ControllerGetCapabilitiesRequest{})
	if err != nil || len(caps.GetCapabilities()) != 3 {
		t.Fatalf("ControllerGetCapabilities = %v, %v", caps, err)
	}
	// The node service was not registered.
	_, err = csi.NewNodeClient(conn).NodeGetCapabilities(ctx, &csi.NodeGetCapabilitiesRequest{})
	if status.Code(err) != codes.Unimplemented {
		t.Errorf("node call: code = %v, want Unimplemented", status.Code(err))
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) < 2 || seen[0] != "/csi.v1.Identity/GetPluginInfo" {
		t.Errorf("the extra interceptor saw %v", seen)
	}
	if err := stop(); err != nil {
		t.Errorf("Serve returned %v after a requested stop", err)
	}
}

func TestServerRemovesItsSocketOnStop(t *testing.T) {
	t.Parallel()
	sock := filepath.Join(t.TempDir(), "csi.sock")
	s, err := NewServer(ServerOptions{Endpoint: "unix://" + sock, Identity: NewIdentity("x")})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Listen(); err != nil {
		t.Fatal(err)
	}
	mustExist(t, sock)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx) }()
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	mustNotExist(t, sock)
}

func TestListenReplacesAStaleSocketAndRefusesAnythingElse(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// A stale socket: bound by a process that is gone and left its file.
	stale := filepath.Join(dir, "stale.sock")
	l, err := net.Listen("unix", stale)
	if err != nil {
		t.Fatal(err)
	}
	l.(*net.UnixListener).SetUnlinkOnClose(false)
	_ = l.Close()
	mustExist(t, stale)
	s, err := NewServer(ServerOptions{Endpoint: "unix://" + stale, Identity: NewIdentity("x")})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Listen(); err != nil {
		t.Fatalf("Listen over a stale socket: %v", err)
	}
	if err := s.Listen(); err == nil {
		t.Error("Listen twice succeeded")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Serve(ctx); err != nil {
		t.Error(err)
	}

	// A regular file is not a socket and is left alone.
	file := filepath.Join(dir, "file")
	writeFile(t, file, "precious")
	s, err = NewServer(ServerOptions{Endpoint: "unix://" + file, Identity: NewIdentity("x")})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Listen(); err == nil || !strings.Contains(err.Error(), "not a socket") {
		t.Errorf("Listen over a file: %v", err)
	}
	if readFile(t, file) != "precious" {
		t.Error("the file was touched")
	}

	// The directory of the socket is made when missing.
	nested := filepath.Join(dir, "a", "b", "csi.sock")
	s, err = NewServer(ServerOptions{Endpoint: "unix://" + nested, Identity: NewIdentity("x")})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Listen(); err != nil {
		t.Errorf("Listen in a new directory: %v", err)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	_ = s.Serve(ctx2)
}

func TestRecoverInterceptorTurnsAPanicIntoInternal(t *testing.T) {
	t.Parallel()
	logs := &syncBuffer{}
	var mu sync.Mutex
	var codesSeen []codes.Code
	outer := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		resp, err := h(ctx, req)
		mu.Lock()
		codesSeen = append(codesSeen, status.Code(err))
		mu.Unlock()
		return resp, err
	}
	conn, _ := runServer(t, ServerOptions{
		Identity:     NewIdentity("x"),
		Controller:   panicky{},
		Interceptors: []grpc.UnaryServerInterceptor{outer},
		Log:          slog.New(slog.NewTextHandler(logs, nil)),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := csi.NewControllerClient(conn).ControllerGetCapabilities(ctx, &csi.ControllerGetCapabilitiesRequest{})
	if status.Code(err) != codes.Internal {
		t.Fatalf("code = %v, want Internal", status.Code(err))
	}
	if strings.Contains(err.Error(), "secret detail") {
		t.Errorf("the panic value reached the caller: %v", err)
	}
	if l := logs.String(); !strings.Contains(l, "panic in handler") || !strings.Contains(l, "kaboom") || !strings.Contains(l, "stack=") {
		t.Errorf("the panic was not logged with its stack: %s", l)
	}
	mu.Lock()
	if len(codesSeen) != 1 || codesSeen[0] != codes.Internal {
		t.Errorf("an outer interceptor saw %v, want [Internal]", codesSeen)
	}
	mu.Unlock()
	// The server survives.
	if _, err := csi.NewIdentityClient(conn).Probe(ctx, &csi.ProbeRequest{}); err != nil {
		t.Errorf("Probe after a panic: %v", err)
	}
}

func TestLoggingInterceptorLogsTheVolumeIDAndNothingElse(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	logs := &syncBuffer{}
	conn, _ := runServer(t, ServerOptions{
		Identity:   NewIdentity("x"),
		Controller: e.ctrl,
		Log:        slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo})),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cc := csi.NewControllerClient(conn)

	req := e.createReq(nameA)
	req.Secrets = map[string]string{"token": "s3cret-value"}
	req.Parameters["subdir"] = "very-private-subdir"
	if _, err := cc.CreateVolume(ctx, req); err != nil {
		t.Fatal(err)
	}
	if _, err := cc.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "directory:" + nameB}); err != nil {
		t.Fatal(err)
	}
	if _, err := cc.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{VolumeId: "bogus", NodeId: "n", VolumeCapability: rwx()[0]}); status.Code(err) != codes.NotFound {
		t.Fatalf("publish: %v", err)
	}
	// Quiet calls are not logged at info.
	if _, err := csi.NewIdentityClient(conn).Probe(ctx, &csi.ProbeRequest{}); err != nil {
		t.Fatal(err)
	}

	out := logs.String()
	for _, secret := range []string{"s3cret-value", "very-private-subdir", e.host} {
		if strings.Contains(out, secret) {
			t.Errorf("the log contains %q:\n%s", secret, out)
		}
	}
	for _, want := range []string{
		`method=/csi.v1.Controller/CreateVolume code=OK`, "volume_id=" + nameA,
		`method=/csi.v1.Controller/DeleteVolume`, "volume_id=directory:" + nameB,
		`level=WARN msg="rpc failed" method=/csi.v1.Controller/ControllerPublishVolume code=NotFound`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the log lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "Probe") {
		t.Errorf("a successful Probe was logged at info:\n%s", out)
	}
}

// A socket that something is listening on belongs to another process: it is
// not replaced, and it keeps answering.
func TestListenLeavesALiveSocketAlone(t *testing.T) {
	t.Parallel()
	sock := filepath.Join(t.TempDir(), "csi.sock")
	live, err := NewServer(ServerOptions{Endpoint: "unix://" + sock, Identity: NewIdentity("first")})
	if err != nil {
		t.Fatal(err)
	}
	if err := live.Listen(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- live.Serve(ctx) }()
	defer func() {
		cancel()
		<-done
	}()

	second, err := NewServer(ServerOptions{Endpoint: "unix://" + sock, Identity: NewIdentity("second")})
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Listen(); err == nil || !strings.Contains(err.Error(), "another process is listening") {
		t.Fatalf("Listen over a live socket: %v, want an error saying another process is listening", err)
	}

	conn, err := grpc.NewClient("unix:"+sock, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	callCtx, callCancel := context.WithTimeout(ctx, 10*time.Second)
	defer callCancel()
	info, err := csi.NewIdentityClient(conn).GetPluginInfo(callCtx, &csi.GetPluginInfoRequest{}, grpc.WaitForReady(true))
	if err != nil || info.GetVendorVersion() != "first" {
		t.Errorf("the first server no longer answers: %v, %v", info, err)
	}
}

// A socket that cannot be told to be stale (here: a connection is refused for
// a reason other than nobody listening) is left alone too.
func TestRemoveIfStaleOnlyAfterConnectionRefused(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Not a socket at all: connect(2) to it is refused like a stale socket's,
	// so the check of what is there is what keeps it.
	file := filepath.Join(dir, "file")
	writeFile(t, file, "precious")
	if err := removeIfStale(file); err == nil || !strings.Contains(err.Error(), "left alone") {
		t.Errorf("removeIfStale(file) = %v, want an error saying it is left alone", err)
	}
	mustExist(t, file)

	// A socket with no listener: connection refused, removed.
	stale := filepath.Join(dir, "stale.sock")
	l, err := net.Listen("unix", stale)
	if err != nil {
		t.Fatal(err)
	}
	l.(*net.UnixListener).SetUnlinkOnClose(false)
	_ = l.Close()
	if err := removeIfStale(stale); err != nil {
		t.Errorf("removeIfStale(stale) = %v", err)
	}
	mustNotExist(t, stale)
}
