package grpc

import (
	"context"

	"google.golang.org/grpc"
)

// UnaryAuthInterceptor is a no-op unary interceptor placeholder for phase 4 auth.
func UnaryAuthInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	return handler(ctx, req)
}

// StreamAuthInterceptor is a no-op streaming interceptor placeholder for phase 4 auth.
func StreamAuthInterceptor(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return handler(srv, ss)
}
