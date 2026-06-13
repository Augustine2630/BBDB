# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test

```bash
go build ./...                          # build all packages
go test ./...                           # run all tests
go test ./internal/meta/... -v -cover   # test a specific package with coverage
go test ./internal/meta/... -run TestWAL -v  # run a single test by name
go tool cover -func=coverage.out | grep -E "^total|meta|tier"  # check coverage %
```

**Coverage requirement: minimum 65% per package.** Always verify with `-cover` before committing.

## Architecture

BBDB is a write-heavy append-only storage engine for telecom events (calls, SMS, metadata). Events flow through a ring buffer → WAL (pebble) → per-shard memtable → sealed immutable block files on disk.

### Package dependency graph (no cycles)

```
api → query → index, tier, block
ingestion → block → meta
block → tier, index, meta
ttl → index, tier, meta
```

`internal/meta` is the **only** package that imports pebble directly. All other packages interact with pebble through meta's typed functions.

### Key design decisions

**Sharding:** `shard_id = uint16(event_type) << 8 | uint8(xxHash64(partition_key) % 256)`. Events with the same `partition_key` + `event_type` always land in the same shard. `partition_key` is opaque `[]byte`.

**Sealed blocks are immutable.** After `rename()` into the tier directory a block file is never modified. TTL deletion = `unlink` of the whole block — no compaction.

**Pebble stores:** WAL entries (`wal:`), block metadata (`blk:`), sparse index (`idx:`), TTL expiry keys (`exp:`), migration state (`mgr:`), per-shard WAL seq counters (`seq:`). All numeric keys use `binary.BigEndian` for correct lexicographic ordering.

**WAL durability:** ingestion batches use `Batch.Commit(pebble.Sync)` with pebble group commit (≤2ms interval). The seal operation commits one final `pebble.Sync` batch that atomically: deletes WAL entries, writes block meta, writes expiry key. idx keys are written in 100K-entry NoSync chunks before this final batch.

**Bloom filters:** stored as `xxHash64(partition_key)` values (not raw bytes) in per-block `.bloom` files (`github.com/bits-and-blooms/bloom/v3`). At read time: `filter.Test(xxHash64(partition_key))`. Bloom files are fsync-ed before the block rename and deleted together with the block.

**Crash recovery on startup:** scan for WAL keys without a corresponding `blk:` key → these shards need replay and re-seal. Scan `mgr:` keys for in-progress tier migrations → resume from copy+verify step.

### Disk layout

```
/data/
├── pebble/
├── tiers/
│   ├── hot/blocks/{shard_id:04x}/{YYYY-MM-DD}T{HH}.block
│   ├── hot/blocks/{shard_id:04x}/{YYYY-MM-DD}T{HH}.bloom
│   ├── warm/blocks/{shard_id:04x}/
│   └── cold/blocks/{shard_id:04x}/
└── tmp/{block_id}.block.tmp
```

Block filename = UTC hour the block was opened (lower time boundary). Blocks sealed early by size (256MB compressed) have `SealedAt` in pebble `BlockMeta`; the TTL reaper uses `SealedAt` as the TTL start for these blocks.

### Sealed block file format

```
HEADER (64 bytes): magic "BBDB", version uint8, shard_id uint16, event_type uint8,
                   opened_at int64, sealed_at int64, row_count uint64,
                   reserved [25]byte, header_checksum uint32
COLUMNS:           key_hash[] (zstd), timestamp[] (zstd+delta), payload[] (zstd)
FOOTER (48 bytes): col_offsets [3]uint64, col_sizes [3]uint64,
                   body_checksum uint32, footer_magic "TBBD"
```

Read order: footer first (seek EOF−48) → header → `key_hash[]`+`timestamp[]` for filtering → `payload[]` only for matching rows.

### Read path

`QueryRequest{partition_key []byte, event_type?, from, to}` → compute `keyHash = xxHash64(partition_key)` → pebble prefix scan on `idx:{event_type}:{keyHash}:` filtered by block hour → bloom check (`filter.Test(keyHash)`) → `pebble.Get(blk:{block_id})` for tier (skip if not found = deleted block) → parallel block reads via bounded goroutine pool → k-way heap merge (blocks are pre-sorted by `(key_hash, timestamp)` at seal time).

### TTL reaper

Runs every 10 minutes. Scans `exp:[0x00..00, expiryKey(now)]` → for each expired block: `TierStore.Delete()` then `pebble.Batch{Delete(blk:), Delete(exp:)}.Commit(Sync)`. Janitor at startup + hourly: scans `exp:` keys ≤ now where file is absent → cleans up orphaned pebble metadata.

## Design documents

- Spec: `docs/superpowers/specs/2026-06-11-bbdb-design.md`
- Implementation Plan 1 (meta + tier): `docs/superpowers/plans/2026-06-11-foundation.md`

## vexp — Context-Aware AI Coding <!-- vexp v1.0.0 -->

### MANDATORY: use vexp pipeline — do NOT grep, glob, or Read files
For every task — bug fixes, features, refactors, debugging:
**call `run_pipeline` FIRST**. It executes context search + impact analysis +
memory recall in a single call, returning compressed results.

Do NOT use grep, glob, Bash, Read, or cat to search/explore the codebase.
vexp returns pre-indexed, graph-ranked context that is more relevant and
uses fewer tokens than manual file reading.

### Primary Tool
- `run_pipeline` — **USE THIS FOR EVERYTHING**. Single call that runs
  capsule + impact + memory server-side. Returns compressed results.
  Auto-detects intent (debug/modify/refactor/explore) from your task.
  Includes full file content for pivots (no need to Read files).
  Examples:
  - `run_pipeline({ "task": "fix JWT validation bug" })` — auto-detect
  - `run_pipeline({ "task": "refactor db layer", "preset": "refactor" })` — explicit
  - `run_pipeline({ "task": "add auth", "observation": "using JWT" })` — save insight in same call

### Other MCP tools (use only when run_pipeline is insufficient)
- `get_context_capsule` — lightweight alternative for simple questions only
- `get_impact_graph` — standalone deep impact analysis of a specific symbol
- `search_logic_flow` — trace execution paths between two specific symbols
- `get_skeleton` — token-efficient file structure for a specific file
- `index_status` — indexing status and health check
- `get_session_context` — recall observations from current/previous sessions
- `search_memory` — cross-session search for past decisions
- `save_observation` — persist insights (prefer using run_pipeline's observation param instead)

### Workflow
1. `run_pipeline("your task")` — ALWAYS FIRST. Returns pivots + impact + memories in 1 call
2. Make targeted changes based on the context returned
3. `run_pipeline` again ONLY if you need more context during implementation
4. Do NOT chain multiple vexp calls — one `run_pipeline` replaces capsule + impact + memory + observation

### Smart Features (automatic — no action needed)
- **Intent Detection**: auto-detects from your task keywords. "fix bug" → Debug, "refactor" → blast-radius, "add" → Modify
- **Hybrid Search**: keyword + semantic + graph centrality ranking
- **Session Memory**: auto-captures observations; memories auto-surfaced in results
- **LSP Bridge**: VS Code captures type-resolved call edges
- **Change Coupling**: co-changed files included as related context

### Advanced Parameters
- `preset: "debug"` — forces debug mode (capsule+tests+impact+memory)
- `preset: "refactor"` — deep impact analysis (depth 5)
- `max_tokens: 12000` — increase total budget for complex tasks
- `include_tests: true` — include test files in results
- `include_file_content: false` — omit full file content (lighter response)

### Multi-Repo Workspaces
`run_pipeline` auto-queries all indexed repos. Use `repos: ["alias"]` to scope.
Use `index_status` to discover available repo aliases.
<!-- /vexp -->
