package grpc

import (
	"context"
	"fmt"
	"io"

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
	// Validate event types.
	for _, e := range req.GetEvents() {
		if e.GetEventType() > 255 {
			return &bbdbv1.WriteResponse{
				BatchId:  req.GetBatchId(),
				Accepted: 0,
				Error: &bbdbv1.Error{
					Code:    1,
					Message: fmt.Sprintf("event_type %d exceeds max value 255", e.GetEventType()),
				},
			}
		}
	}

	blockEvents, resolvedKeys := ProtoEventsToBlock(req.GetEvents())

	// Group by shard and write.
	shardEvents := make(map[meta.ShardID][]block.Event)
	for i, e := range req.GetEvents() {
		shard := block.ShardFor(resolvedKeys[i], uint8(e.GetEventType()))
		shardEvents[shard] = append(shardEvents[shard], blockEvents[i])
	}

	for shard, events := range shardEvents {
		w := s.getOrCreateWriter(shard)
		_ = w.Write(ctx, events)
	}

	return &bbdbv1.WriteResponse{
		BatchId:       req.GetBatchId(),
		Accepted:      uint32(len(req.GetEvents())),
		PartitionKeys: resolvedKeys,
	}
}

// getOrCreateWriter returns an existing ShardWriter or creates a new one.
func (s *IngestionServer) getOrCreateWriter(shard meta.ShardID) *ingestion.ShardWriter {
	if w, ok := s.writers[shard]; ok {
		return w
	}
	w := ingestion.NewShardWriter(s.db, shard, s.writerCfg)
	s.writers[shard] = w
	return w
}

// Stop stops all shard writers.
func (s *IngestionServer) Stop() {
	for _, w := range s.writers {
		w.Stop()
	}
}
