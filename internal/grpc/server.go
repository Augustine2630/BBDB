package grpc

import (
	"context"
	"net"

	bbdbv1 "BBDB/api/gen/bbdb/v1"
	"BBDB/internal/ingestion"
	"BBDB/internal/meta"

	"google.golang.org/grpc"
)

// Server wraps a gRPC server with an IngestionServer.
type Server struct {
	listenAddr string
	grpc       *grpc.Server
	ingestion  *IngestionServer
}

// NewServer creates a Server that listens on listenAddr and serves the ingestion RPC.
func NewServer(listenAddr string, db *meta.DB, writerCfg ingestion.WriterConfig) *Server {
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(UnaryAuthInterceptor),
		grpc.ChainStreamInterceptor(StreamAuthInterceptor),
	)
	ing := NewIngestionServer(db, writerCfg)
	bbdbv1.RegisterEventIngestionServer(srv, ing)
	return &Server{
		listenAddr: listenAddr,
		grpc:       srv,
		ingestion:  ing,
	}
}

// Run starts the gRPC listener and blocks until ctx is cancelled, then performs a graceful stop.
func (s *Server) Run(ctx context.Context) error {
	lis, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return err
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.grpc.Serve(lis)
	}()

	select {
	case <-ctx.Done():
		s.grpc.GracefulStop()
		s.ingestion.Stop()
		return nil
	case err := <-errCh:
		return err
	}
}
