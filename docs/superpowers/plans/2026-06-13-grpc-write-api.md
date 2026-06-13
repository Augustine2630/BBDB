# gRPC Write API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a gRPC Write API to BBDB so Java, Go, and other language clients can stream events over the network.

**Architecture:** A thin `internal/grpc` adapter translates proto `WriteRequest` batches into `block.Event` values and routes them through the existing `ingestion.ShardWriter`. The gRPC server runs as a goroutine alongside the existing reaper/janitor in `internal/server/server.go`. No business logic lives in the gRPC layer.

**Tech Stack:** Go 1.24, `google.golang.org/grpc` v1.71+, `google.golang.org/protobuf` (already in go.mod as indirect), `github.com/google/uuid` for partition_key generation, `buf` CLI for codegen (installed via `go install`).

---

## File Map

| File | Action | Purpose |
|------|--------|---------|
| `api/proto/bbdb/v1/common.proto` | Create | Event, Error messages |
| `api/proto/bbdb/v1/ingestion.proto` | Create | EventIngestion service |
| `buf.yaml` | Create | buf module config |
| `buf.gen.yaml` | Create | go + grpc-go codegen plugins |
| `api/gen/bbdb/v1/*.pb.go` | Generated+committed | Proto Go bindings |
| `api/gen/bbdb/v1/*_grpc.pb.go` | Generated+committed | gRPC Go bindings |
| `internal/grpc/convert.go` | Create | proto↔block.Event + partition_key gen |
| `internal/grpc/interceptor.go` | Create | no-op auth interceptor stub |
| `internal/grpc/ingestion_server.go` | Create | EventIngestion service impl |
| `internal/grpc/server.go` | Create | gRPC server bootstrap + graceful stop |
| `internal/grpc/convert_test.go` | Create | Unit tests for convert.go |
| `internal/grpc/ingestion_server_test.go` | Create | Integration test for Write stream |
| `internal/config/config.go` | Modify | Add GRPCConfig |
| `configs/bbdb.example.yaml` | Modify | Add grpc section |
| `configs/bbdb-local.yaml` | Modify | Add grpc section |
| `internal/server/server.go` | Modify | Wire gRPC server into Run() |
| `Makefile` | Modify | Add proto/gen target |

---

## Task 1: Install buf and add gRPC dependencies

**Files:**
- Modify: `go.mod` / `go.sum` (via `go get`)

- [ ] **Step 1: Install buf CLI**

```bash
go install github.com/bufbuild/buf/cmd/buf@v1.50.0
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.5
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
```

Verify:
```bash
buf --version
# Expected: 1.50.0
protoc-gen-go --version
# Expected: protoc-gen-go v1.36.5
protoc-gen-go-grpc --version
# Expected: protoc-gen-go-grpc 1.5.1
```

- [ ] **Step 2: Add gRPC and UUID to go.mod**

```bash
cd /Users/cherepovskiy.m3/GolandProjects/BBDB
go get google.golang.org/grpc@v1.71.1
go get github.com/google/uuid@v1.6.0
go mod tidy
```

Expected: `go.mod` now lists `google.golang.org/grpc` and `github.com/google/uuid` as direct deps.

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum
git commit -m "chore: add grpc and uuid dependencies"
```

---

## Task 2: Proto files and buf config

**Files:**
- Create: `api/proto/bbdb/v1/common.proto`
- Create: `api/proto/bbdb/v1/ingestion.proto`
- Create: `buf.yaml`
- Create: `buf.gen.yaml`

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p api/proto/bbdb/v1 api/gen/bbdb/v1
```

- [ ] **Step 2: Create `api/proto/bbdb/v1/common.proto`**

```protobuf
syntax = "proto3";
package bbdb.v1;

option go_package = "github.com/Augustine2630/BBDB/api/gen/bbdb/v1;bbdbv1";
option java_package = "io.bbdb.v1";
option java_multiple_files = true;

message Event {
  bytes  partition_key = 1;
  uint32 event_type    = 2;
  int64  timestamp_ns  = 3;
  bytes  payload       = 4;
}

message Error {
  uint32 code    = 1;
  string message = 2;
}
```

- [ ] **Step 3: Create `api/proto/bbdb/v1/ingestion.proto`**

```protobuf
syntax = "proto3";
package bbdb.v1;

option go_package = "github.com/Augustine2630/BBDB/api/gen/bbdb/v1;bbdbv1";
option java_package = "io.bbdb.v1";
option java_multiple_files = true;

import "bbdb/v1/common.proto";

service EventIngestion {
  rpc Write(stream WriteRequest) returns (stream WriteResponse);
}

message WriteRequest {
  repeated Event events   = 1;
  string         batch_id = 2;
}

message WriteResponse {
  string         batch_id       = 1;
  uint32         accepted       = 2;
  repeated bytes partition_keys = 3;
  Error          error          = 4;
}
```

- [ ] **Step 4: Create `buf.yaml`**

```yaml
version: v2
modules:
  - path: api/proto
lint:
  use:
    - STANDARD
breaking:
  use:
    - FILE
```

- [ ] **Step 5: Create `buf.gen.yaml`**

```yaml
version: v2
plugins:
  - remote: buf.build/protocolbuffers/go
    out: api/gen
    opt:
      - paths=source_relative
  - remote: buf.build/grpc/go
    out: api/gen
    opt:
      - paths=source_relative
      - require_unimplemented_servers=false
```

- [ ] **Step 6: Commit proto files**

```bash
git add api/proto/ buf.yaml buf.gen.yaml
git commit -m "feat(grpc): add proto files and buf config"
```

---

## Task 3: Generate Go code from proto

**Files:**
- Create: `api/gen/bbdb/v1/common.pb.go`
- Create: `api/gen/bbdb/v1/ingestion.pb.go`
- Create: `api/gen/bbdb/v1/ingestion_grpc.pb.go`

- [ ] **Step 1: Add proto/gen target to Makefile**

Add after the `tidy` target in `Makefile`:

```makefile
# ── Proto ─────────────────────────────────────────────────────────────────────

.PHONY: proto/gen
proto/gen: ## Generate Go code from proto files (requires buf)
	buf generate
```

- [ ] **Step 2: Run codegen**

```bash
make proto/gen
```

Expected: files created in `api/gen/bbdb/v1/`:
- `common.pb.go`
- `ingestion.pb.go`
- `ingestion_grpc.pb.go`

- [ ] **Step 3: Verify generated code compiles**

```bash
go build ./api/gen/...
```

Expected: no errors.

- [ ] **Step 4: Commit generated code**

```bash
git add api/gen/ Makefile
git commit -m "feat(grpc): generate Go bindings from proto"
```

---

## Task 4: `convert.go` — proto↔block.Event conversion

**Files:**
- Create: `internal/grpc/convert.go`
- Create: `internal/grpc/convert_test.go`

- [ ] **Step 1: Write failing tests**

Create `internal/grpc/convert_test.go`:

```go
package grpc_test

import (
	"testing"
	"time"

	bbdbv1 "github.com/Augustine2630/BBDB/api/gen/bbdb/v1"
	bbdbgrpc "github.com/Augustine2630/BBDB/internal/grpc"
)

func TestResolvePartitionKey_provided(t *testing.T) {
	key := []byte("msisdn:+79001234567")
	got := bbdbgrpc.ResolvePartitionKey(key)
	if string(got) != string(key) {
		t.Fatalf("want %q, got %q", key, got)
	}
}

func TestResolvePartitionKey_generated(t *testing.T) {
	got := bbdbgrpc.ResolvePartitionKey(nil)
	if len(got) != 16 {
		t.Fatalf("want 16-byte UUID, got len=%d", len(got))
	}
	got2 := bbdbgrpc.ResolvePartitionKey([]byte{})
	if string(got) == string(got2) {
		t.Fatal("two generated keys must differ")
	}
}

func TestResolveTimestamp_zero(t *testing.T) {
	before := time.Now().UnixNano()
	got := bbdbgrpc.ResolveTimestamp(0)
	after := time.Now().UnixNano()
	if got < before || got > after {
		t.Fatalf("expected now, got %d", got)
	}
}

func TestResolveTimestamp_nonzero(t *testing.T) {
	ts := int64(1_000_000_000)
	if got := bbdbgrpc.ResolveTimestamp(ts); got != ts {
		t.Fatalf("want %d, got %d", ts, got)
	}
}

func TestProtoEventsToBlock(t *testing.T) {
	events := []*bbdbv1.Event{
		{PartitionKey: []byte("key1"), EventType: 1, TimestampNs: 1000, Payload: []byte("p1")},
		{PartitionKey: nil, EventType: 2, TimestampNs: 0, Payload: []byte("p2")},
	}
	blockEvents, keys := bbdbgrpc.ProtoEventsToBlock(events)
	if len(blockEvents) != 2 {
		t.Fatalf("want 2 block events, got %d", len(blockEvents))
	}
	if len(keys) != 2 {
		t.Fatalf("want 2 keys, got %d", len(keys))
	}
	if string(keys[0]) != "key1" {
		t.Fatalf("want key1, got %q", keys[0])
	}
	if len(keys[1]) != 16 {
		t.Fatalf("want generated 16-byte key, got len=%d", len(keys[1]))
	}
	if blockEvents[0].Timestamp != 1000 {
		t.Fatalf("want timestamp 1000, got %d", blockEvents[0].Timestamp)
	}
	if blockEvents[1].Timestamp == 0 {
		t.Fatal("zero timestamp must be replaced with now()")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test ./internal/grpc/... -run TestResolve -v
```

Expected: `FAIL` — package does not exist yet.

- [ ] **Step 3: Implement `internal/grpc/convert.go`**

```go
package grpc

import (
	"time"

	"github.com/google/uuid"

	bbdbv1 "github.com/Augustine2630/BBDB/api/gen/bbdb/v1"
	"github.com/Augustine2630/BBDB/internal/block"
)

// ResolvePartitionKey returns key as-is if non-empty, otherwise generates a UUID v4 (16 raw bytes).
func ResolvePartitionKey(key []byte) []byte {
	if len(key) > 0 {
		return key
	}
	id := uuid.New()
	return id[:]
}

// ResolveTimestamp returns ts if non-zero, otherwise time.Now().UnixNano().
func ResolveTimestamp(ts int64) int64 {
	if ts != 0 {
		return ts
	}
	return time.Now().UnixNano()
}

// ProtoEventsToBlock converts a slice of proto Events into block.Events and the resolved partition keys.
// Returns (blockEvents, resolvedKeys) where len(resolvedKeys) == len(events).
func ProtoEventsToBlock(events []*bbdbv1.Event) ([]block.Event, [][]byte) {
	blockEvents := make([]block.Event, len(events))
	keys := make([][]byte, len(events))
	for i, e := range events {
		key := ResolvePartitionKey(e.PartitionKey)
		keys[i] = key
		blockEvents[i] = block.Event{
			KeyHash:   block.KeyHashFor(key),
			Timestamp: ResolveTimestamp(e.TimestampNs),
			Payload:   e.Payload,
		}
	}
	return blockEvents, keys
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
go test ./internal/grpc/... -run "TestResolve|TestProtoEvents" -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/grpc/convert.go internal/grpc/convert_test.go
git commit -m "feat(grpc): proto↔block.Event conversion with partition_key generation"
```

---

## Task 5: `interceptor.go` — no-op auth stub

**Files:**
- Create: `internal/grpc/interceptor.go`

- [ ] **Step 1: Create `internal/grpc/interceptor.go`**

```go
package grpc

import (
	"context"

	"google.golang.org/grpc"
)

// UnaryAuthInterceptor is a no-op placeholder for phase 4 auth.
// TODO: phase 4 — pluggable auth (none/password/mTLS)
func UnaryAuthInterceptor(
	ctx context.Context,
	req any,
	_ *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	return handler(ctx, req)
}

// StreamAuthInterceptor is a no-op placeholder for phase 4 auth.
// TODO: phase 4 — pluggable auth (none/password/mTLS)
func StreamAuthInterceptor(
	srv any,
	ss grpc.ServerStream,
	_ *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	return handler(srv, ss)
}
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./internal/grpc/...
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add internal/grpc/interceptor.go
git commit -m "feat(grpc): no-op auth interceptor stub (phase 4 placeholder)"
```

---

## Task 6: `ingestion_server.go` — Write stream handler

**Files:**
- Create: `internal/grpc/ingestion_server.go`
- Create: `internal/grpc/ingestion_server_test.go`

- [ ] **Step 1: Write failing test**

Create `internal/grpc/ingestion_server_test.go`:

```go
package grpc_test

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	bbdbv1 "github.com/Augustine2630/BBDB/api/gen/bbdb/v1"
	bbdbgrpc "github.com/Augustine2630/BBDB/internal/grpc"
	"github.com/Augustine2630/BBDB/internal/ingestion"
	"github.com/Augustine2630/BBDB/internal/meta"
)

func openTestDB(t *testing.T) (*meta.DB, func()) {
	t.Helper()
	dir, _ := os.MkdirTemp("", "bbdb-grpc-*")
	db, err := meta.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	return db, func() { db.Close(); os.RemoveAll(dir) }
}

func startTestGRPCServer(t *testing.T, db *meta.DB) (bbdbv1.EventIngestionClient, func()) {
	t.Helper()

	writerCfg := ingestion.WriterConfig{
		BatchInterval: 5 * time.Millisecond,
		RingBufSize:   64,
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	srv := grpc.NewServer(
		grpc.UnaryInterceptor(bbdbgrpc.UnaryAuthInterceptor),
		grpc.StreamInterceptor(bbdbgrpc.StreamAuthInterceptor),
	)
	bbdbv1.RegisterEventIngestionServer(srv, bbdbgrpc.NewIngestionServer(db, writerCfg))
	go srv.Serve(lis)

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	client := bbdbv1.NewEventIngestionClient(conn)

	return client, func() {
		conn.Close()
		srv.GracefulStop()
	}
}

func TestWrite_singleBatch(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	client, stop := startTestGRPCServer(t, db)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.Write(ctx)
	if err != nil {
		t.Fatal(err)
	}

	req := &bbdbv1.WriteRequest{
		BatchId: "batch-1",
		Events: []*bbdbv1.Event{
			{PartitionKey: []byte("user-1"), EventType: 1, TimestampNs: 1000, Payload: []byte("hello")},
			{PartitionKey: nil, EventType: 2, TimestampNs: 0, Payload: []byte("world")},
		},
	}
	if err := stream.Send(req); err != nil {
		t.Fatal(err)
	}

	resp, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}

	if resp.BatchId != "batch-1" {
		t.Fatalf("want batch_id=batch-1, got %q", resp.BatchId)
	}
	if resp.Accepted != 2 {
		t.Fatalf("want accepted=2, got %d", resp.Accepted)
	}
	if len(resp.PartitionKeys) != 2 {
		t.Fatalf("want 2 partition_keys, got %d", len(resp.PartitionKeys))
	}
	if string(resp.PartitionKeys[0]) != "user-1" {
		t.Fatalf("want key user-1, got %q", resp.PartitionKeys[0])
	}
	if len(resp.PartitionKeys[1]) != 16 {
		t.Fatalf("want 16-byte generated key, got len=%d", len(resp.PartitionKeys[1]))
	}
	if resp.Error != nil && resp.Error.Code != 0 {
		t.Fatalf("unexpected error: %v", resp.Error.Message)
	}

	stream.CloseSend()
}

func TestWrite_invalidEventType(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	client, stop := startTestGRPCServer(t, db)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.Write(ctx)
	if err != nil {
		t.Fatal(err)
	}

	req := &bbdbv1.WriteRequest{
		BatchId: "batch-err",
		Events: []*bbdbv1.Event{
			{PartitionKey: []byte("k"), EventType: 999, Payload: []byte("x")},
		},
	}
	if err := stream.Send(req); err != nil {
		t.Fatal(err)
	}

	resp, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}

	if resp.Error == nil || resp.Error.Code == 0 {
		t.Fatal("want error for event_type > 255")
	}
	if resp.Accepted != 0 {
		t.Fatalf("want accepted=0, got %d", resp.Accepted)
	}

	stream.CloseSend()
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test ./internal/grpc/... -run "TestWrite" -v
```

Expected: FAIL — `bbdbgrpc.NewIngestionServer` not defined.

- [ ] **Step 3: Implement `internal/grpc/ingestion_server.go`**

```go
package grpc

import (
	"context"
	"fmt"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	bbdbv1 "github.com/Augustine2630/BBDB/api/gen/bbdb/v1"
	"github.com/Augustine2630/BBDB/internal/block"
	"github.com/Augustine2630/BBDB/internal/ingestion"
	"github.com/Augustine2630/BBDB/internal/meta"
)

// IngestionServer implements bbdbv1.EventIngestionServer.
type IngestionServer struct {
	bbdbv1.UnimplementedEventIngestionServer
	db        *meta.DB
	writerCfg ingestion.WriterConfig
	writers   map[meta.ShardID]*ingestion.ShardWriter
}

// NewIngestionServer creates an IngestionServer. writerCfg is used as the base config for ShardWriters.
func NewIngestionServer(db *meta.DB, writerCfg ingestion.WriterConfig) *IngestionServer {
	return &IngestionServer{
		db:        db,
		writerCfg: writerCfg,
		writers:   make(map[meta.ShardID]*ingestion.ShardWriter),
	}
}

// Write implements the bidirectional streaming Write RPC.
func (s *IngestionServer) Write(stream bbdbv1.EventIngestion_WriteServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return status.Errorf(codes.Internal, "recv: %v", err)
		}

		resp := s.handleBatch(stream.Context(), req)
		if err := stream.Send(resp); err != nil {
			return status.Errorf(codes.Internal, "send: %v", err)
		}
	}
}

func (s *IngestionServer) handleBatch(ctx context.Context, req *bbdbv1.WriteRequest) *bbdbv1.WriteResponse {
	// Validate all event_types before processing
	for _, e := range req.Events {
		if e.EventType > 255 {
			return &bbdbv1.WriteResponse{
				BatchId:  req.BatchId,
				Accepted: 0,
				Error: &bbdbv1.Error{
					Code:    1,
					Message: fmt.Sprintf("event_type %d exceeds maximum value 255", e.EventType),
				},
			}
		}
	}

	blockEvents, keys := ProtoEventsToBlock(req.Events)

	// Route each event to its shard writer
	// Group by shard for efficiency
	type shardBatch struct {
		shard  meta.ShardID
		events []block.Event
	}
	shardMap := make(map[meta.ShardID][]block.Event)
	for i, e := range req.Events {
		shard := block.ShardFor(keys[i], uint8(e.EventType))
		shardMap[shard] = append(shardMap[shard], blockEvents[i])
	}

	for shard, events := range shardMap {
		w := s.getOrCreateWriter(shard)
		if err := w.Write(ctx, events); err != nil {
			return &bbdbv1.WriteResponse{
				BatchId:  req.BatchId,
				Accepted: 0,
				Error:    &bbdbv1.Error{Code: 2, Message: err.Error()},
			}
		}
	}

	return &bbdbv1.WriteResponse{
		BatchId:       req.BatchId,
		Accepted:      uint32(len(req.Events)),
		PartitionKeys: keys,
	}
}

func (s *IngestionServer) getOrCreateWriter(shard meta.ShardID) *ingestion.ShardWriter {
	if w, ok := s.writers[shard]; ok {
		return w
	}
	w := ingestion.NewShardWriter(s.db, shard, s.writerCfg)
	s.writers[shard] = w
	return w
}

// Stop gracefully stops all shard writers.
func (s *IngestionServer) Stop() {
	for _, w := range s.writers {
		w.Stop()
	}
}
```

- [ ] **Step 4: Run tests**

```bash
go test ./internal/grpc/... -run "TestWrite" -v
```

Expected: both PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/grpc/ingestion_server.go internal/grpc/ingestion_server_test.go
git commit -m "feat(grpc): EventIngestion Write stream handler"
```

---

## Task 7: `server.go` — gRPC server bootstrap

**Files:**
- Create: `internal/grpc/server.go`

- [ ] **Step 1: Create `internal/grpc/server.go`**

```go
package grpc

import (
	"context"
	"net"

	"google.golang.org/grpc"

	bbdbv1 "github.com/Augustine2630/BBDB/api/gen/bbdb/v1"
	"github.com/Augustine2630/BBDB/internal/ingestion"
	"github.com/Augustine2630/BBDB/internal/meta"
)

// Server wraps a gRPC server with graceful shutdown.
type Server struct {
	listenAddr string
	grpc       *grpc.Server
	ingestion  *IngestionServer
}

// NewServer creates a gRPC Server. Call Run to start listening.
func NewServer(listenAddr string, db *meta.DB, writerCfg ingestion.WriterConfig) *Server {
	ing := NewIngestionServer(db, writerCfg)

	srv := grpc.NewServer(
		grpc.UnaryInterceptor(UnaryAuthInterceptor),
		grpc.StreamInterceptor(StreamAuthInterceptor),
	)
	bbdbv1.RegisterEventIngestionServer(srv, ing)

	return &Server{
		listenAddr: listenAddr,
		grpc:       srv,
		ingestion:  ing,
	}
}

// Run starts the gRPC server and blocks until ctx is cancelled.
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
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./internal/grpc/...
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add internal/grpc/server.go
git commit -m "feat(grpc): gRPC server bootstrap with graceful shutdown"
```

---

## Task 8: Config — add GRPCConfig

**Files:**
- Modify: `internal/config/config.go`
- Modify: `configs/bbdb.example.yaml`
- Modify: `configs/bbdb-local.yaml`

- [ ] **Step 1: Add GRPCConfig to `internal/config/config.go`**

Add after `BlockConfig`:

```go
// GRPCConfig controls the gRPC server.
type GRPCConfig struct {
	ListenAddr string `mapstructure:"listen_addr"`
}
```

Add `GRPC GRPCConfig` field to `Config` struct:

```go
type Config struct {
	Data      DataConfig      `mapstructure:"data"`
	Tiers     TiersConfig     `mapstructure:"tiers"`
	Ingestion IngestionConfig `mapstructure:"ingestion"`
	Block     BlockConfig     `mapstructure:"block"`
	Query     QueryConfig     `mapstructure:"query"`
	TTL       TTLConfig       `mapstructure:"ttl"`
	GRPC      GRPCConfig      `mapstructure:"grpc"`
}
```

Add default in `Load()`:

```go
v.SetDefault("grpc.listen_addr", ":7070")
```

- [ ] **Step 2: Add grpc section to `configs/bbdb.example.yaml`**

Append at the end:

```yaml
grpc:
  listen_addr: ":7070"
```

- [ ] **Step 3: Add grpc section to `configs/bbdb-local.yaml`**

Append at the end:

```yaml
grpc:
  listen_addr: ":7070"
```

- [ ] **Step 4: Verify config test still passes**

```bash
go test ./internal/config/... -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/config/config.go configs/bbdb.example.yaml configs/bbdb-local.yaml
git commit -m "feat(grpc): add GRPCConfig with listen_addr default :7070"
```

---

## Task 9: Wire gRPC into server.Run()

**Files:**
- Modify: `internal/server/server.go`

- [ ] **Step 1: Update `internal/server/server.go`**

Add import `bbdbgrpc "github.com/Augustine2630/BBDB/internal/grpc"` and `grpcServer *bbdbgrpc.Server` field to `Server` struct:

```go
type Server struct {
	db              *meta.DB
	engine          *query.Engine
	reaper          *ttl.Reaper
	janitor         *ttl.Janitor
	tiers           map[meta.Tier]tier.TierStore
	writerCfg       ingestion.WriterConfig
	grpcServer      *bbdbgrpc.Server
	shutdownTimeout time.Duration
	onShutdown      func()
}
```

In `New()`, after building `writerCfg`, add:

```go
grpcServer := bbdbgrpc.NewServer(cfg.GRPC.ListenAddr, db, writerCfg)
```

And set in the returned struct:

```go
return &Server{
	db:              db,
	engine:          engine,
	reaper:          reaper,
	janitor:         janitor,
	tiers:           tiers,
	writerCfg:       writerCfg,
	grpcServer:      grpcServer,
	shutdownTimeout: timeout,
}, nil
```

In `Run()`, add gRPC server to the WaitGroup alongside reaper/janitor:

```go
func (s *Server) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	wg.Add(3)
	go func() {
		defer wg.Done()
		s.janitor.Run(ctx)
	}()
	go func() {
		defer wg.Done()
		s.reaper.Run(ctx)
	}()
	go func() {
		defer wg.Done()
		if err := s.grpcServer.Run(ctx); err != nil {
			// non-fatal: log only
			_ = err
		}
	}()

	<-ctx.Done()

	waitDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
	case <-time.After(s.shutdownTimeout):
	}

	s.Shutdown()
	return nil
}
```

- [ ] **Step 2: Verify full build**

```bash
go build ./...
```

Expected: no errors.

- [ ] **Step 3: Run all tests**

```bash
go test ./... -count=1
```

Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/server/server.go
git commit -m "feat(grpc): wire gRPC server into Server.Run() alongside reaper/janitor"
```

---

## Task 10: Smoke test and final check

**Files:**
- Modify: `Makefile` (add `test/grpc` target)

- [ ] **Step 1: Add test/grpc target to Makefile**

```makefile
.PHONY: test/grpc
test/grpc: ## gRPC layer tests
	go test ./internal/grpc/... -v -count=1 -coverprofile=$(COVERAGE_OUT)
	go tool cover -func=$(COVERAGE_OUT) | grep -E "^github|^total"
```

- [ ] **Step 2: Run gRPC tests with coverage**

```bash
make test/grpc
```

Expected: all PASS, coverage ≥ 65%.

- [ ] **Step 3: Build binary and do a smoke test**

```bash
make build/bbdb
CONFIG=configs/bbdb-local.yaml make run &
sleep 1
# verify gRPC port is open
nc -z 127.0.0.1 7070 && echo "gRPC port open" || echo "FAIL"
kill %1
```

Expected: `gRPC port open`.

- [ ] **Step 4: Run full test suite**

```bash
go test ./... -count=1
```

Expected: all PASS.

- [ ] **Step 5: Final commit**

```bash
git add Makefile
git commit -m "feat(grpc): add test/grpc Makefile target"
```
