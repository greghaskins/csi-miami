package blockd

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

func TestControlSocketMode(t *testing.T) {
	cfg, _ := testConfig(t)
	lis, err := listenControl(cfg.ControlSocket)
	if err != nil {
		t.Fatalf("listenControl: %v", err)
	}
	defer func() { _ = lis.Close() }()
	fi, err := os.Stat(cfg.ControlSocket)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("control socket mode = %v, want 0600", fi.Mode().Perm())
	}
}

func TestControlClientRoundTrip(t *testing.T) {
	cfg, _ := testConfig(t)
	s := newTestServer(t, cfg)
	dev := &fakeDevice{idx: 11}
	s.setDevice(dev)

	lis, err := listenControl(cfg.ControlSocket)
	if err != nil {
		t.Fatalf("listenControl: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	served := make(chan error, 1)
	go func() { served <- s.serveControl(ctx, lis) }()

	cli, err := Dial(ctx, cfg.ControlSocket)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = cli.Close() }()

	st, err := cli.Status(ctx)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Index != 11 || st.BinaryHash == "" || st.Size != testSize {
		t.Fatalf("status = %+v", st)
	}

	// Write a full chunk, snapshot it and publish it.
	writeDevice(t, s, 0, pattern(1, testChunk), false)
	if err := cli.Boundary(ctx); err != nil {
		t.Fatalf("Boundary: %v", err)
	}
	reply, err := cli.Publish(ctx, "request")
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if reply.Name == "" {
		t.Fatalf("publish reply: %+v", reply)
	}

	// No boundary now: Publish maps to ErrNoBoundary.
	if _, err := cli.Publish(ctx, "request"); !errors.Is(err, ErrNoBoundary) {
		t.Fatalf("second Publish = %v, want ErrNoBoundary", err)
	}

	// Grow reaches the device.
	const newSize = 4 << 20
	if err := cli.Grow(ctx, newSize); err != nil {
		t.Fatalf("Grow: %v", err)
	}
	if dev.size != newSize || s.back.Size() != newSize {
		t.Fatalf("grow not applied: device %d, backend %d", dev.size, s.back.Size())
	}

	// Stop asks the server to shut down.
	if err := cli.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	select {
	case <-s.stop:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not request shutdown")
	}

	cancel()
	if err := <-served; err != nil {
		t.Fatalf("serveControl: %v", err)
	}
}

func TestControlRefusedDeliversReply(t *testing.T) {
	cfg, chk := testConfig(t)
	s := newTestServer(t, cfg)
	chk.set(false, nil)

	lis, err := listenControl(cfg.ControlSocket)
	if err != nil {
		t.Fatalf("listenControl: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.serveControl(ctx, lis) }()

	cli, err := Dial(ctx, cfg.ControlSocket)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = cli.Close() }()

	writeDevice(t, s, 0, pattern(1, testChunk), false)
	if err := cli.Boundary(ctx); err != nil {
		t.Fatalf("Boundary: %v", err)
	}
	reply, err := cli.Publish(ctx, "request")
	if !errors.Is(err, ErrRefused) {
		t.Fatalf("Publish = %v, want ErrRefused", err)
	}
	if !reply.Refused {
		t.Fatal("reply.Refused is false over the wire")
	}
}
