package grpc

import (
	"context"
	"fmt"
	"io"
	"sync"

	"go.uber.org/zap"

	bbdbv1 "BBDB/api/gen/bbdb/v1"
	"BBDB/internal/block"
	"BBDB/internal/ingestion"
	"BBDB/internal/meta"
)

// IngestionServer implements bbdbv1.EventIngestionServer.
type IngestionServer struct {
	bbdbv1.UnimplementedEventIngestionServer
	db        *meta.DB
	writerCfg ingestion.WriterConfig
	mu        sync.RWMutex
	writers   map[meta.ShardID]*ingestion.ShardWriter
}

// NewIngestionServer creates an IngestionServer with the given meta.DB and writer config.
func NewIngestionServer(db *meta.DB, writerCfg ingestion.WriterConfig) *IngestionServer {
	return &IngestionServer{
		db:        db,
		writerCfg: writerCfg,
		writers:   make(map[meta.ShardID]*ingestion.ShardWriter),
	}
}

// Write handles the bidirectional streaming RPC.
func (s *IngestionServer) Write(stream bbdbv1.EventIngestion_WriteServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp := s.handleBatch(stream.Context(), req)
		if sendErr := stream.Send(resp); sendErr != nil {
			return sendErr
		}
	}
}

// handleBatch processes a single WriteRequest and returns a WriteResponse.
func (s *IngestionServer) handleBatch(ctx context.Context, req *bbdbv1.WriteRequest) *bbdbv1.WriteResponse {
	zap.L().Debug("write batch received",
		zap.String("batch_id", req.GetBatchId()),
		zap.Int("events", len(req.GetEvents())),
		zap.String("request_id", RequestIDFromContext(ctx)),
	)
	blockEvents, resolvedKeys := ProtoEventsToBlock(req.GetEvents())

	// Validate event types — after key resolution so partition_keys is always populated.
	for _, e := range req.GetEvents() {
		if e.GetEventType() > 255 {
			return &bbdbv1.WriteResponse{
				BatchId:       req.GetBatchId(),
				Accepted:      0,
				PartitionKeys: resolvedKeys,
				Error: &bbdbv1.Error{
					Code:    1,
					Message: fmt.Sprintf("event_type %d exceeds max value 255", e.GetEventType()),
				},
			}
		}
	}

	// Group by shard and write.
	shardEvents := make(map[meta.ShardID][]block.Event)
	for i, e := range req.GetEvents() {
		shard := block.ShardFor(resolvedKeys[i], uint8(e.GetEventType()))
		shardEvents[shard] = append(shardEvents[shard], blockEvents[i])
	}

	for shard, events := range shardEvents {
		w := s.getOrCreateWriter(shard)
		if err := w.Write(ctx, events); err != nil {
			zap.L().Error("write batch failed",
				zap.Error(err),
				zap.String("batch_id", req.GetBatchId()),
				zap.String("request_id", RequestIDFromContext(ctx)),
			)
			return &bbdbv1.WriteResponse{
				BatchId:       req.GetBatchId(),
				Accepted:      0,
				PartitionKeys: resolvedKeys,
				Error: &bbdbv1.Error{
					Code:    2,
					Message: err.Error(),
				},
			}
		}
	}

	return &bbdbv1.WriteResponse{
		BatchId:       req.GetBatchId(),
		Accepted:      uint32(len(req.GetEvents())),
		PartitionKeys: resolvedKeys,
	}
}

// getOrCreateWriter returns an existing ShardWriter or creates a new one.
func (s *IngestionServer) getOrCreateWriter(shard meta.ShardID) *ingestion.ShardWriter {
	s.mu.RLock()
	w, ok := s.writers[shard]
	s.mu.RUnlock()
	if ok {
		return w
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Re-check after acquiring write lock to avoid double creation.
	if w, ok = s.writers[shard]; ok {
		return w
	}
	w = ingestion.NewShardWriter(s.db, shard, s.writerCfg)
	s.writers[shard] = w
	return w
}

// Stop stops all shard writers.
func (s *IngestionServer) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, w := range s.writers {
		w.Stop()
	}
}
