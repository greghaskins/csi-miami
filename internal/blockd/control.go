package blockd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"time"
)

// controlServiceName is the net/rpc service name the Client dials.
const controlServiceName = "Control"

// Status is what the control service reports about the running server. It never
// carries a key or any chunk content.
type Status struct {
	Index            int   // the nbd device index, -1 before the device connects
	Size             int64 // the device size in bytes
	Dirty            int64 // unpublished dirty bytes
	Cached           int64 // bytes resident in the cache
	Base             string
	LastPublish      time.Time
	LastPublishError string
	Refused          bool
	BinaryHash       string
}

// PublishArgs is the argument of Publish; Trigger is interval|watermark|request|unstage.
type PublishArgs struct{ Trigger string }

// PublishReply is the result of a publish.
type PublishReply struct {
	Name     string // the manifest made current, empty when nothing was published
	Uploaded int64  // bytes uploaded
	Refused  bool   // the fencing check refused the generation
}

// Control is the net/rpc service the node plugin drives. Boundary is step 2 of
// generations (called between FIFREEZE and FITHAW); Publish is steps 3 and 4.
type Control interface {
	Status(args struct{}, reply *Status) error
	Boundary(args struct{}, reply *struct{}) error
	Publish(args PublishArgs, reply *PublishReply) error
	Grow(args struct{ Size int64 }, reply *struct{}) error
	Stop(args struct{}, reply *struct{}) error
}

// controlService is the server side of Control over one server.
type controlService struct{ s *server }

var _ Control = (*controlService)(nil)

// Status reports the current state.
func (c *controlService) Status(_ struct{}, reply *Status) error {
	*reply = c.s.status()
	return nil
}

// Boundary snapshots the dirty set; the caller holds the filesystem frozen.
func (c *controlService) Boundary(_ struct{}, _ *struct{}) error {
	return c.s.boundary()
}

// Publish uploads the boundary generation. A refusal is delivered as a reply
// with Refused set (net/rpc drops the reply on a returned error), so the Client
// can see Refused and surface ErrRefused itself; ErrNoBoundary and transport
// errors are returned.
func (c *controlService) Publish(args PublishArgs, reply *PublishReply) error {
	r, err := c.s.publish(c.s.runCtx, args.Trigger)
	if errors.Is(err, ErrRefused) {
		*reply = r
		return nil
	}
	if err != nil {
		return err
	}
	*reply = r
	return nil
}

// Grow raises the device size.
func (c *controlService) Grow(args struct{ Size int64 }, _ *struct{}) error {
	return c.s.growTo(c.s.runCtx, args.Size)
}

// Stop asks the server to shut down; Run then disconnects and exits.
func (c *controlService) Stop(_ struct{}, _ *struct{}) error {
	c.s.requestStop()
	return nil
}

// growTo raises the device to size (a whole number of chunks, never smaller
// than the current size) in the cache and then in the kernel.
func (s *server) growTo(ctx context.Context, size int64) error {
	cur := s.back.Size()
	switch {
	case size == cur:
		return nil
	case size < cur:
		return fmt.Errorf("blockd: cannot shrink from %d to %d", cur, size)
	case size%s.cfg.Chunk != 0:
		return fmt.Errorf("blockd: size %d is not a whole number of %d byte chunks", size, s.cfg.Chunk)
	}
	if err := s.back.grow(size); err != nil {
		return err
	}
	return s.device().reconfigure(ctx, size)
}

// listenControl opens the control socket at mode 0600, replacing a stale one.
func listenControl(path string) (net.Listener, error) {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("blockd: control socket: %w", err)
	}
	lis, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("blockd: control socket: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = lis.Close()
		return nil, fmt.Errorf("blockd: control socket: %w", err)
	}
	return lis, nil
}

// serveControl answers control RPCs until ctx ends, then returns nil.
func (s *server) serveControl(ctx context.Context, lis net.Listener) error {
	srv := rpc.NewServer()
	if err := srv.RegisterName(controlServiceName, &controlService{s}); err != nil {
		return fmt.Errorf("blockd: register control service: %w", err)
	}
	go func() {
		<-ctx.Done()
		_ = lis.Close()
	}()
	for {
		conn, err := lis.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("blockd: control accept: %w", err)
		}
		go srv.ServeConn(conn)
	}
}
