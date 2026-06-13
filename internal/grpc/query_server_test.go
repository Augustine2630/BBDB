package grpc_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	bbdbv1 "BBDB/api/gen/bbdb/v1"
	"BBDB/internal/block"
	internalgrpc "BBDB/internal/grpc"
	"BBDB/internal/query"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// mockReader is a test double for query.Reader.
type mockReader struct {
	events []block.Event
	err    error
}

func (m *mockReader) Query(_ context.Context, _ query.QueryRequest) ([]block.Event, error) {
	return m.events, m.err
}

// startQueryServer spins up an in-process gRPC server and returns a client + cleanup func.
func startQueryServer(t *testing.T, reader query.Reader) (bbdbv1.EventQueryClient, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	bbdbv1.RegisterEventQueryServer(srv, internalgrpc.NewQueryServer(reader))
	go srv.Serve(lis) //nolint:errcheck

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	client := bbdbv1.NewEventQueryClient(conn)
	return client, func() {
		conn.Close()
		srv.GracefulStop()
	}
}

func makeQueryReq(key []byte, fromNs, toNs int64) *bbdbv1.QueryRequest {
	return &bbdbv1.QueryRequest{
		PartitionKey: key,
		FromNs:       fromNs,
		ToNs:         toNs,
	}
}

func collectResponses(t *testing.T, stream bbdbv1.EventQuery_QueryClient) []*bbdbv1.QueryResponse {
	t.Helper()
	var out []*bbdbv1.QueryResponse
	for {
		resp, err := stream.Recv()
		if err != nil {
			break
		}
		out = append(out, resp)
		if resp.GetIsLast() {
			break
		}
	}
	return out
}

func TestQueryServer_EmptyResult(t *testing.T) {
	client, cleanup := startQueryServer(t, &mockReader{events: nil})
	defer cleanup()

	now := time.Now()
	stream, err := client.Query(context.Background(), makeQueryReq(
		[]byte("pk"),
		now.Add(-time.Hour).UnixNano(),
		now.UnixNano(),
	))
	if err != nil {
		t.Fatal(err)
	}
	resps := collectResponses(t, stream)
	if len(resps) != 1 {
		t.Fatalf("expected 1 terminal response, got %d", len(resps))
	}
	r := resps[0]
	if !r.GetIsLast() {
		t.Error("expected is_last=true")
	}
	if r.GetTotal() != 0 {
		t.Errorf("expected total=0, got %d", r.GetTotal())
	}
	if r.GetError() != nil {
		t.Errorf("unexpected error: %v", r.GetError().GetMessage())
	}
}

func TestQueryServer_WithEvents(t *testing.T) {
	pk := []byte("user-123")
	evs := []block.Event{
		{KeyHash: block.KeyHashFor(pk), Timestamp: 1_000_000_000, Payload: []byte("a")},
		{KeyHash: block.KeyHashFor(pk), Timestamp: 2_000_000_000, Payload: []byte("b")},
	}
	client, cleanup := startQueryServer(t, &mockReader{events: evs})
	defer cleanup()

	now := time.Now()
	stream, err := client.Query(context.Background(), makeQueryReq(
		pk,
		now.Add(-time.Hour).UnixNano(),
		now.UnixNano(),
	))
	if err != nil {
		t.Fatal(err)
	}
	resps := collectResponses(t, stream)

	var total uint64
	var allEvents []*bbdbv1.Event
	for _, r := range resps {
		allEvents = append(allEvents, r.GetEvents()...)
		if r.GetIsLast() {
			total = r.GetTotal()
		}
	}
	if total != 2 {
		t.Errorf("expected total=2, got %d", total)
	}
	if len(allEvents) != 2 {
		t.Fatalf("expected 2 events, got %d", len(allEvents))
	}
	// partition_key echoed back on every event.
	for i, e := range allEvents {
		if string(e.GetPartitionKey()) != string(pk) {
			t.Errorf("event[%d] partition_key: expected %q, got %q", i, pk, e.GetPartitionKey())
		}
	}
}

func TestQueryServer_ReaderError(t *testing.T) {
	client, cleanup := startQueryServer(t, &mockReader{err: errors.New("storage failure")})
	defer cleanup()

	now := time.Now()
	stream, err := client.Query(context.Background(), makeQueryReq(
		[]byte("pk"),
		now.Add(-time.Hour).UnixNano(),
		now.UnixNano(),
	))
	if err != nil {
		t.Fatal(err)
	}
	resps := collectResponses(t, stream)
	if len(resps) == 0 {
		t.Fatal("expected at least one response")
	}
	last := resps[len(resps)-1]
	if last.GetError() == nil {
		t.Error("expected error in terminal response")
	}
	if last.GetError().GetMessage() != "storage failure" {
		t.Errorf("unexpected error message: %s", last.GetError().GetMessage())
	}
}

func TestQueryServer_EmptyPartitionKey(t *testing.T) {
	client, cleanup := startQueryServer(t, &mockReader{})
	defer cleanup()

	now := time.Now()
	stream, err := client.Query(context.Background(), makeQueryReq(
		nil, // empty partition_key
		now.Add(-time.Hour).UnixNano(),
		now.UnixNano(),
	))
	if err != nil {
		t.Fatal(err)
	}
	resps := collectResponses(t, stream)
	if len(resps) == 0 {
		t.Fatal("expected terminal error response")
	}
	last := resps[len(resps)-1]
	if last.GetError() == nil {
		t.Error("expected error for empty partition_key")
	}
}

func TestQueryServer_InvalidTimeRange(t *testing.T) {
	client, cleanup := startQueryServer(t, &mockReader{})
	defer cleanup()

	now := time.Now()
	stream, err := client.Query(context.Background(), makeQueryReq(
		[]byte("pk"),
		now.UnixNano(),            // from
		now.Add(-time.Hour).UnixNano(), // to < from → invalid
	))
	if err != nil {
		t.Fatal(err)
	}
	resps := collectResponses(t, stream)
	if len(resps) == 0 {
		t.Fatal("expected terminal error response")
	}
	last := resps[len(resps)-1]
	if last.GetError() == nil {
		t.Error("expected error for invalid time range")
	}
}
