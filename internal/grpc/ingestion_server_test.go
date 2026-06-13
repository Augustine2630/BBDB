package grpc_test

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	bbdbv1 "BBDB/api/gen/bbdb/v1"
	internalgrpc "BBDB/internal/grpc"
	"BBDB/internal/ingestion"
	"BBDB/internal/meta"

	gogrpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// startTestGRPCServer spins up a real in-process gRPC server on a random port.
// Returns the EventIngestionClient and a cleanup function.
func startTestGRPCServer(t *testing.T, db *meta.DB) (bbdbv1.EventIngestionClient, func()) {
	t.Helper()

	cfg := ingestion.DefaultWriterConfig
	cfg.RingBufSize = 64

	srv := internalgrpc.NewIngestionServer(db, cfg)

	grpcSrv := gogrpc.NewServer()
	bbdbv1.RegisterEventIngestionServer(grpcSrv, srv)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}

	go func() { _ = grpcSrv.Serve(lis) }()

	conn, err := gogrpc.NewClient(
		lis.Addr().String(),
		gogrpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}

	client := bbdbv1.NewEventIngestionClient(conn)

	cleanup := func() {
		conn.Close()
		grpcSrv.GracefulStop() // wait for all in-flight RPCs to finish
		srv.Stop()             // signal ShardWriters to stop; they do a final flush
		// Give ShardWriter goroutines time to complete their final pebble flush.
		time.Sleep(50 * time.Millisecond)
		db.Close()
	}
	return client, cleanup
}

func TestWrite_singleBatch(t *testing.T) {
	dir := t.TempDir()
	db, err := meta.Open(dir)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}

	client, cleanup := startTestGRPCServer(t, db)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.Write(ctx)
	if err != nil {
		t.Fatalf("client.Write: %v", err)
	}

	req := &bbdbv1.WriteRequest{
		BatchId: "batch-1",
		Events: []*bbdbv1.Event{
			{
				PartitionKey: []byte("key-a"),
				EventType:    1,
				TimestampNs:  1_000_000_000,
				Payload:      []byte("hello"),
			},
			{
				PartitionKey: nil, // auto-generated key
				EventType:    2,
				TimestampNs:  0, // auto-generated timestamp
				Payload:      []byte("world"),
			},
		},
	}

	if err := stream.Send(req); err != nil {
		t.Fatalf("stream.Send: %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("stream.CloseSend: %v", err)
	}

	resp, err := stream.Recv()
	if err != nil && err != io.EOF {
		t.Fatalf("stream.Recv: %v", err)
	}

	if resp.GetBatchId() != "batch-1" {
		t.Errorf("BatchId: expected batch-1, got %s", resp.GetBatchId())
	}
	if resp.GetAccepted() != 2 {
		t.Errorf("Accepted: expected 2, got %d", resp.GetAccepted())
	}
	if resp.GetError() != nil {
		t.Errorf("unexpected error: %v", resp.GetError())
	}
	if len(resp.GetPartitionKeys()) != 2 {
		t.Errorf("PartitionKeys: expected 2, got %d", len(resp.GetPartitionKeys()))
	}
	// First key preserved as-is.
	if string(resp.GetPartitionKeys()[0]) != "key-a" {
		t.Errorf("PartitionKeys[0]: expected key-a, got %s", resp.GetPartitionKeys()[0])
	}
	// Second key auto-generated (16 bytes UUID).
	if len(resp.GetPartitionKeys()[1]) != 16 {
		t.Errorf("PartitionKeys[1]: expected 16 bytes UUID, got %d bytes", len(resp.GetPartitionKeys()[1]))
	}
}

func TestWrite_invalidEventType(t *testing.T) {
	dir := t.TempDir()
	db, err := meta.Open(dir)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}

	client, cleanup := startTestGRPCServer(t, db)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.Write(ctx)
	if err != nil {
		t.Fatalf("client.Write: %v", err)
	}

	req := &bbdbv1.WriteRequest{
		BatchId: "batch-invalid",
		Events: []*bbdbv1.Event{
			{
				PartitionKey: []byte("key-x"),
				EventType:    999, // invalid: > 255
				TimestampNs:  1_000_000,
				Payload:      []byte("bad"),
			},
		},
	}

	if err := stream.Send(req); err != nil {
		t.Fatalf("stream.Send: %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("stream.CloseSend: %v", err)
	}

	resp, err := stream.Recv()
	if err != nil && err != io.EOF {
		t.Fatalf("stream.Recv: %v", err)
	}

	if resp.GetError() == nil {
		t.Fatal("expected error response for invalid event_type, got nil")
	}
	if resp.GetError().GetCode() != 1 {
		t.Errorf("Error.Code: expected 1, got %d", resp.GetError().GetCode())
	}
	if resp.GetAccepted() != 0 {
		t.Errorf("Accepted: expected 0, got %d", resp.GetAccepted())
	}
}
