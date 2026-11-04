package blockd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/rpc"
)

// Client drives a block server's control service over its unix socket. The
// node plugin uses it. Every call is bounded by its context.
type Client struct{ rpc *rpc.Client }

// Dial connects to the control socket.
func Dial(ctx context.Context, socket string) (*Client, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", socket)
	if err != nil {
		return nil, fmt.Errorf("blockd: dial control socket: %w", err)
	}
	return &Client{rpc: rpc.NewClient(conn)}, nil
}

// Close closes the connection.
func (c *Client) Close() error { return c.rpc.Close() }

// call runs one RPC bounded by ctx. A context that ends before the reply
// returns ctx's error; the outstanding call is abandoned, not joined.
func (c *Client) call(ctx context.Context, method string, args, reply any) error {
	done := c.rpc.Go(controlServiceName+"."+method, args, reply, make(chan *rpc.Call, 1))
	select {
	case <-ctx.Done():
		return ctx.Err()
	case call := <-done.Done:
		return call.Error
	}
}

// Status reports the server's state.
func (c *Client) Status(ctx context.Context) (Status, error) {
	var r Status
	err := c.call(ctx, "Status", struct{}{}, &r)
	return r, err
}

// Boundary snapshots the dirty set; the caller must hold the filesystem frozen.
func (c *Client) Boundary(ctx context.Context) error {
	return c.call(ctx, "Boundary", struct{}{}, &struct{}{})
}

// Publish uploads the boundary generation. It returns ErrNoBoundary when none
// was set and ErrRefused when the fencing check refused it; the reply's Refused
// field is set in the latter case.
func (c *Client) Publish(ctx context.Context, trigger string) (PublishReply, error) {
	var r PublishReply
	err := c.call(ctx, "Publish", PublishArgs{Trigger: trigger}, &r)
	if err != nil {
		if isServerError(err, ErrNoBoundary) {
			return r, ErrNoBoundary
		}
		return r, err
	}
	if r.Refused {
		return r, ErrRefused
	}
	return r, nil
}

// Grow raises the device size.
func (c *Client) Grow(ctx context.Context, size int64) error {
	return c.call(ctx, "Grow", struct{ Size int64 }{Size: size}, &struct{}{})
}

// Stop asks the server to shut down.
func (c *Client) Stop(ctx context.Context) error {
	return c.call(ctx, "Stop", struct{}{}, &struct{}{})
}

// isServerError reports whether err is a net/rpc server error whose message is
// target's; the error identity is lost across the wire, so it is matched by
// message.
func isServerError(err, target error) bool {
	var se rpc.ServerError
	if errors.As(err, &se) {
		return string(se) == target.Error()
	}
	return err.Error() == target.Error()
}
