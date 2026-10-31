package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	kubefake "github.com/greghaskins/csi-miami/internal/kube/fake"
	"github.com/greghaskins/csi-miami/internal/mount/fake"
)

func TestRunControllerFlags(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		args       []string
		wantCode   int
		wantStdout []string // substrings
		wantStderr []string
	}{
		{
			name:     "help lists every flag",
			args:     []string{"-h"},
			wantCode: 0,
			wantStdout: []string{
				"Usage of miami controller:", "-endpoint", "unix:///csi/csi.sock",
				"-transports-root", "/var/lib/miami", "-kubeconfig", "-deadline", "10s", "-metrics-addr", ":9811",
			},
		},
		{name: "unknown flag", args: []string{"-nope"}, wantCode: 2, wantStderr: []string{"flag provided but not defined: -nope"}},
		{name: "extra argument", args: []string{"x"}, wantCode: 2, wantStderr: []string{`unexpected argument "x"`}},
		{name: "zero deadline", args: []string{"-deadline", "0s"}, wantCode: 2, wantStderr: []string{"-deadline must be positive"}},
		{name: "negative deadline", args: []string{"-deadline=-1s"}, wantCode: 2, wantStderr: []string{"-deadline must be positive"}},
		{name: "bad deadline", args: []string{"-deadline", "soon"}, wantCode: 2, wantStderr: []string{"invalid value"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var stdout, stderr bytes.Buffer
			code := runController(tt.args, &stdout, &stderr)
			if code != tt.wantCode {
				t.Errorf("exit code = %d, want %d", code, tt.wantCode)
			}
			for _, want := range tt.wantStdout {
				if !strings.Contains(stdout.String(), want) {
					t.Errorf("stdout lacks %q:\n%s", want, stdout.String())
				}
			}
			for _, want := range tt.wantStderr {
				if !strings.Contains(stderr.String(), want) {
					t.Errorf("stderr lacks %q:\n%s", want, stderr.String())
				}
			}
		})
	}
}

// Outside a cluster and with no kubeconfig the controller says so and exits 1,
// without having made a socket.
func TestRunControllerWithoutAClusterFails(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	sock := filepath.Join(t.TempDir(), "csi.sock")
	var stderr bytes.Buffer
	code := runController([]string{"-endpoint", "unix://" + sock, "-transports-root", t.TempDir()}, io.Discard, &stderr)
	if code != 1 || !strings.Contains(stderr.String(), "no kubeconfig") {
		t.Errorf("exit code = %d, stderr = %q; want 1 and a message about the kubeconfig", code, stderr.String())
	}
	if _, err := os.Lstat(sock); err == nil {
		t.Error("a socket was made")
	}
}

func TestServeControllerWithServesUntilTheContextEnds(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	sock := filepath.Join(dir, "csi.sock")
	cfg := controllerConfig{endpoint: "unix://" + sock, root: filepath.Join(dir, "transports-root", "nested"), deadline: time.Second, metricsAddr: "127.0.0.1:0"}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	go func() { done <- serveControllerWith(ctx, cfg, &kubefake.Reader{}, &fake.Mounter{}, log) }()

	conn, err := grpc.NewClient("unix:"+sock, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	callCtx, callCancel := context.WithTimeout(ctx, 10*time.Second)
	defer callCancel()
	// WaitForReady makes the call wait for the server to start listening.
	info, err := csi.NewIdentityClient(conn).GetPluginInfo(callCtx, &csi.GetPluginInfoRequest{}, grpc.WaitForReady(true))
	if err != nil {
		t.Fatal(err)
	}
	if info.GetName() != "miami.csi.greghaskins.com" || info.GetVendorVersion() != version {
		t.Errorf("GetPluginInfo = %v", info)
	}
	caps, err := csi.NewControllerClient(conn).ControllerGetCapabilities(callCtx, &csi.ControllerGetCapabilitiesRequest{})
	if err != nil || len(caps.GetCapabilities()) != 3 {
		t.Errorf("ControllerGetCapabilities = %v, %v", caps, err)
	}

	cancel()
	if err := <-done; err != nil {
		t.Errorf("serveControllerWith returned %v after a requested stop", err)
	}
	if _, err := os.Lstat(filepath.Join(dir, "transports-root", "nested")); err != nil {
		t.Errorf("the transports root was not made: %v", err)
	}
}

func TestServeControllerWithErrors(t *testing.T) {
	t.Parallel()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	dir := t.TempDir()
	file := filepath.Join(dir, "file")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		cfg  controllerConfig
	}{
		{"root under a file", controllerConfig{endpoint: "unix://" + filepath.Join(dir, "a.sock"), root: filepath.Join(file, "root")}},
		{"endpoint that is not a unix socket", controllerConfig{endpoint: "tcp://192.0.2.1:1", root: dir}},
		{"a bad metrics address", controllerConfig{endpoint: "unix://" + filepath.Join(dir, "b.sock"), root: dir, metricsAddr: "256.0.0.1:1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := serveControllerWith(context.Background(), tt.cfg, &kubefake.Reader{}, &fake.Mounter{}, log)
			if err == nil {
				t.Error("no error")
			}
		})
	}
}

func TestResolveRootResolvesSymlinks(t *testing.T) {
	t.Parallel()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir, "target")
	if err := os.Mkdir(target, 0o750); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	got, err := resolveRoot(link)
	if err != nil || got != target {
		t.Errorf("resolveRoot(link) = %q, %v; want %q", got, err, target)
	}
}

func TestRunDispatchesController(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	if code := run([]string{"controller", "-h"}, &stdout, &stderr); code != 0 || !strings.HasPrefix(stdout.String(), "Usage of miami controller:") {
		t.Errorf("exit code = %d, stdout = %q", code, stdout.String())
	}
}
