# gRPC Write API Design

## Goal

Add a gRPC network layer to BBDB so Java, Go, and other language clients can write events over the network. The first phase covers write-only. Read (Query), driver-level batching/reconnect, and authentication are separate phases.

## Architecture

A thin `internal/grpc` adapter sits between the gRPC transport and the existing `ingestion.ShardWriter`. It translates proto messages into `block.Event` values and routes them through the existing write path. No business logic lives in the gRPC layer.

```
gRPC client
  └─> internal/grpc/ingestion_server.go
        └─> ingestion.ShardWriter (existing)
              └─> WAL → memtable → block seal (existing)
```

The gRPC server is bootstrapped in `internal/grpc/server.go` and wired into `internal/server/server.go` alongside the existing reaper/janitor goroutines.

## File Structure

```
api/proto/bbdb/v1/
  common.proto          — Event, Error shared types
  ingestion.proto       — EventIngestion service

api/gen/bbdb/v1/        — protoc-generated Go code (committed)
  *.pb.go
  *_grpc.pb.go

internal/grpc/
  server.go             — gRPC server bootstrap (listen, graceful stop)
  ingestion_server.go   — EventIngestion service implementation
  interceptor.go        — auth interceptor stub (TODO: phase 4)
  convert.go            — proto ↔ block.Event conversion + partition_key generation
```

## Proto Definition

### `common.proto`

```protobuf
syntax = "proto3";
package bbdb.v1;
option go_package = "BBDB/api/gen/bbdb/v1;bbdbv1";
option java_package = "io.bbdb.v1";

message Event {
  bytes  partition_key = 1; // empty → server generates UUID v4 (16 bytes)
  uint32 event_type    = 2; // 0–255; required
  int64  timestamp_ns  = 3; // unix nanoseconds; 0 → server substitutes time.Now()
  bytes  payload       = 4; // opaque blob; required
}

message Error {
  uint32 code    = 1;
  string message = 2;
}
```

### `ingestion.proto`

```protobuf
syntax = "proto3";
package bbdb.v1;
option go_package = "BBDB/api/gen/bbdb/v1;bbdbv1";
option java_package = "io.bbdb.v1";

import "bbdb/v1/common.proto";

service EventIngestion {
  // Bidirectional stream: client sends batches, server acknowledges each.
  rpc Write(stream WriteRequest) returns (stream WriteResponse);
}

message WriteRequest {
  repeated Event events   = 1; // batch of events
  string         batch_id = 2; // client-side idempotency key; optional
}

message WriteResponse {
  string         batch_id       = 1; // echoed from request
  uint32         accepted       = 2; // number of events accepted
  repeated bytes partition_keys = 3; // final key per event, len == len(events)
  Error          error          = 4; // absent = success
}
```

## Server Behaviour

### partition_key resolution

- `partition_key` empty (`len == 0`) → server generates UUID v4 as 16 raw bytes, uses it for sharding, returns it in `partition_keys[i]`
- `partition_key` non-empty → used as-is for sharding, echoed back in `partition_keys[i]`
- `partition_keys` in the response always has the same length as `events` in the request

### timestamp_ns

- `timestamp_ns == 0` → server substitutes `time.Now().UnixNano()`
- Non-zero value is stored as-is (client clock)

### event_type validation

- `event_type > 255` → `WriteResponse.error` is set, `accepted = 0`, stream continues (not terminated)

### Batching

- Each `WriteRequest` produces exactly one `WriteResponse` (1:1)
- Server routes events to `ShardWriter` per `(partition_key, event_type)` shard
- ShardWriter batching and autoseal are unchanged

### Error handling

- Per-batch errors are returned in `WriteResponse.error`; the stream stays open
- Unrecoverable server errors close the stream with a gRPC status code

## Configuration

New section in `config.go` and `bbdb.example.yaml`:

```yaml
grpc:
  listen_addr: ":7070"   # gRPC listen address
```

`GRPCConfig.ListenAddr string` added to `Config` root struct.

## Auth

`internal/grpc/interceptor.go` contains a no-op unary and stream interceptor with a `// TODO: phase 4 — pluggable auth (none/password/cert)` comment. Wired into the server at startup so phase 4 only needs to fill in the implementation.

## Graceful Shutdown

`internal/grpc/server.go` exposes `Run(ctx)` and `Stop()`. `Server.Run()` in `internal/server/server.go` adds the gRPC server to its `sync.WaitGroup` alongside reaper and janitor.

## Code Generation

`buf` is used for protoc code generation:

```
buf.yaml          — buf module config
buf.gen.yaml      — go + grpc plugins
```

Generated code is committed to `api/gen/` so clients can `go get` without needing `buf` installed.

Makefile target:

```makefile
proto/gen:
    buf generate
```

## Phased Roadmap

| Phase | Scope |
|-------|-------|
| 1 (this spec) | Write API: proto, gRPC server, ingestion adapter |
| 2 | Query API: EventQuery service, server-side streaming |
| 3 | Driver: batching, reconnect, flow control |
| 4 | Auth: pluggable interceptor (none / password / mTLS) |
