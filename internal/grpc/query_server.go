package grpc

import (
	"time"

	bbdbv1 "BBDB/api/gen/bbdb/v1"
	"BBDB/internal/query"

	"google.golang.org/grpc"
)

const queryBatchSize = 256

// QueryServer implements bbdbv1.EventQueryServer.
type QueryServer struct {
	bbdbv1.UnimplementedEventQueryServer
	engine query.Reader
}

// NewQueryServer creates a QueryServer backed by the given Reader.
func NewQueryServer(engine query.Reader) *QueryServer {
	return &QueryServer{engine: engine}
}

// Query executes the request and streams results back in batches.
// The final message always has is_last=true and carries the total count.
func (s *QueryServer) Query(req *bbdbv1.QueryRequest, stream grpc.ServerStreamingServer[bbdbv1.QueryResponse]) error {
	if len(req.GetPartitionKey()) == 0 {
		return stream.Send(terminalError(0, "partition_key must not be empty"))
	}
	from := time.Unix(0, req.GetFromNs()).UTC()
	to := time.Unix(0, req.GetToNs()).UTC()
	if !from.Before(to) {
		return stream.Send(terminalError(0, "from_ns must be before to_ns"))
	}

	var et *uint8
	if v := req.GetEventType(); v != 0 {
		u := uint8(v)
		et = &u
	}

	qreq := query.QueryRequest{
		PartitionKey: req.GetPartitionKey(),
		EventType:    et,
		From:         from,
		To:           to,
	}

	events, err := s.engine.Query(stream.Context(), qreq)
	if err != nil {
		return stream.Send(terminalError(0, err.Error()))
	}

	total := uint64(len(events))
	protoEvents := BlockEventsToProto(events, req.GetPartitionKey())

	// Stream in batches; last batch carries is_last + total.
	for start := 0; start < len(protoEvents); start += queryBatchSize {
		end := start + queryBatchSize
		if end > len(protoEvents) {
			end = len(protoEvents)
		}
		batch := protoEvents[start:end]
		isLast := end == len(protoEvents)
		resp := &bbdbv1.QueryResponse{
			Events: batch,
			IsLast: isLast,
		}
		if isLast {
			resp.Total = total
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}

	// Empty result: send a single terminal message.
	if len(protoEvents) == 0 {
		return stream.Send(&bbdbv1.QueryResponse{IsLast: true, Total: 0})
	}
	return nil
}

func terminalError(total uint64, msg string) *bbdbv1.QueryResponse {
	return &bbdbv1.QueryResponse{
		IsLast: true,
		Total:  total,
		Error:  &bbdbv1.Error{Code: 1, Message: msg},
	}
}
