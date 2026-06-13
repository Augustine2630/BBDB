package grpc

import (
	"context"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type contextKey struct{}

var requestIDKey = contextKey{}

// UnaryAuthInterceptor logs each unary RPC with request_id, method, duration, error.
func UnaryAuthInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	reqID := newRequestID()
	ctx = context.WithValue(ctx, requestIDKey, reqID)
	start := time.Now()

	resp, err := handler(ctx, req)

	fields := []zap.Field{
		zap.String("request_id", reqID),
		zap.String("method", info.FullMethod),
		zap.Duration("duration", time.Since(start)),
	}
	if err != nil {
		zap.L().Error("unary RPC failed", append(fields, zap.Error(err))...)
	} else {
		zap.L().Debug("unary RPC", fields...)
	}
	return resp, err
}

// StreamAuthInterceptor logs each streaming RPC with request_id, method, duration, error.
func StreamAuthInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	reqID := newRequestID()
	ctx := context.WithValue(ss.Context(), requestIDKey, reqID)
	wrapped := &wrappedStream{ServerStream: ss, ctx: ctx}
	start := time.Now()

	err := handler(srv, wrapped)

	fields := []zap.Field{
		zap.String("request_id", reqID),
		zap.String("method", info.FullMethod),
		zap.Duration("duration", time.Since(start)),
	}
	if err != nil {
		zap.L().Error("stream RPC failed", append(fields, zap.Error(err))...)
	} else {
		zap.L().Debug("stream RPC", fields...)
	}
	return err
}

type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }

// RequestIDFromContext extracts the request_id from ctx; returns "" if not set.
func RequestIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(requestIDKey).(string)
	return v
}
