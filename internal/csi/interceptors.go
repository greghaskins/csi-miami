package csi

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// quietMethods are the calls that are made over and over by the sidecars'
// health checks and registration; they are logged at debug level when they
// succeed.
var quietMethods = map[string]bool{
	"/csi.v1.Identity/Probe":                       true,
	"/csi.v1.Identity/GetPluginInfo":               true,
	"/csi.v1.Identity/GetPluginCapabilities":       true,
	"/csi.v1.Controller/ControllerGetCapabilities": true,
	"/csi.v1.Node/NodeGetCapabilities":             true,
}

// volumeIDOf returns what a request says about which volume it is for, and
// nothing else of it: the volume ID, or for CreateVolume the name, which
// becomes the volume's ID. No parameter, secret, path or capability is ever
// logged.
func volumeIDOf(req any) string {
	switch r := req.(type) {
	case *csi.CreateVolumeRequest:
		return r.GetName()
	case interface{ GetVolumeId() string }:
		return r.GetVolumeId()
	default:
		return ""
	}
}

// LoggingInterceptor logs every call once, when it ends: the method, the
// gRPC code, the duration and the volume ID if the request has one. It logs a
// failure at warning level with the status message, and a success at info (at
// debug for the calls in quietMethods). Request payloads are never logged.
func LoggingInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)

		code := status.Code(err)
		attrs := []any{"method", info.FullMethod, "code", code.String(), "duration", time.Since(start)}
		if id := volumeIDOf(req); id != "" {
			attrs = append(attrs, "volume_id", id)
		}
		switch {
		case err != nil:
			attrs = append(attrs, "error", status.Convert(err).Message())
			log.Warn("rpc failed", attrs...)
		case quietMethods[info.FullMethod]:
			log.Debug("rpc", attrs...)
		default:
			log.Info("rpc", attrs...)
		}
		return resp, err
	}
}

// RecoverInterceptor turns a panic in a handler into an Internal error and
// logs it with its stack, so that one bad request cannot take the process
// down. The panic value is not sent to the caller.
func RecoverInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				log.Error("panic in handler", "method", info.FullMethod, "panic", fmt.Sprint(r),
					"stack", strings.TrimSpace(string(debug.Stack())))
				resp, err = nil, status.Error(codes.Internal, "internal error")
			}
		}()
		return handler(ctx, req)
	}
}
