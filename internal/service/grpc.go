package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/Team-Deepiri/deepiri-sugar-glider/internal/config"
	"github.com/Team-Deepiri/deepiri-sugar-glider/internal/redisstreams"
	synapsev1 "github.com/Team-Deepiri/deepiri-sugar-glider/proto/synapse/v1"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Sidecar) Publish(ctx context.Context, req *synapsev1.PublishRequest) (*synapsev1.PublishResponse, error) {
	s.incrementPublishAttempts()

	if req == nil {
		s.incrementError()
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	entryID, queued, _, statusCode, err := s.publishInternal(ctx, redisstreams.PublishRequest{
		Stream:    req.GetStream(),
		EventType: req.GetEventType(),
		Sender:    req.GetSender(),
		Recipient: req.GetRecipient(),
		Priority:  req.GetPriority(),
		Payload:   json.RawMessage(req.GetPayload()),
		TTLSec:    req.GetTtlSec(),
	})
	if err != nil {
		return nil, grpcStatusFromHTTPStatus(statusCode, err.Error())
	}
	if queued {
		// Proto response has no queued flag; empty entry_id indicates locally queued publish.
		return &synapsev1.PublishResponse{EntryId: ""}, nil
	}

	return &synapsev1.PublishResponse{EntryId: entryID}, nil
}

func (s *Sidecar) PublishBatch(ctx context.Context, req *synapsev1.PublishBatchRequest) (*synapsev1.PublishBatchResponse, error) {
	if req == nil {
		s.incrementError()
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if len(req.GetEvents()) == 0 {
		s.incrementError()
		return nil, status.Error(codes.InvalidArgument, "events are required")
	}

	stream := strings.TrimSpace(req.GetStream())
	entryIDs := make([]string, 0, len(req.GetEvents()))
	for _, event := range req.GetEvents() {
		if event == nil {
			s.incrementError()
			return nil, status.Error(codes.InvalidArgument, "batch contains nil event")
		}

		eventStream := event.GetStream()
		if strings.TrimSpace(eventStream) == "" {
			eventStream = stream
		}

		s.incrementPublishAttempts()
		entryID, queued, _, statusCode, err := s.publishInternal(ctx, redisstreams.PublishRequest{
			Stream:    eventStream,
			EventType: event.GetEventType(),
			Sender:    event.GetSender(),
			Recipient: event.GetRecipient(),
			Priority:  event.GetPriority(),
			Payload:   json.RawMessage(event.GetPayload()),
			TTLSec:    event.GetTtlSec(),
		})
		if err != nil {
			return nil, grpcStatusFromHTTPStatus(statusCode, err.Error())
		}
		if queued {
			entryIDs = append(entryIDs, "")
			continue
		}
		entryIDs = append(entryIDs, entryID)
	}

	return &synapsev1.PublishBatchResponse{EntryIds: entryIDs}, nil
}

func (s *Sidecar) Subscribe(req *synapsev1.SubscribeRequest, stream grpc.ServerStreamingServer[synapsev1.Event]) error {
	s.incrementReadRequest()

	if req == nil {
		s.incrementError()
		return status.Error(codes.InvalidArgument, "request is required")
	}

	count := int64(req.GetBatchSize())
	if count <= 0 {
		count = 10
	}

	events, statusCode, err := s.consumeGroupOnce(stream.Context(), readRequest{
		Stream:        req.GetStream(),
		ConsumerGroup: req.GetConsumerGroup(),
		ConsumerName:  req.GetConsumerName(),
		Count:         count,
		BlockMS:       1000,
	})
	if err != nil {
		return grpcStatusFromHTTPStatus(statusCode, err.Error())
	}

	for _, event := range events {
		fields := event.Fields
		payload := []byte(toString(fields["payload"]))
		out := &synapsev1.Event{
			Stream:    event.Stream,
			EntryId:   event.Entry,
			EventType: toString(fields["event_type"]),
			Sender:    toString(fields["sender"]),
			Payload:   payload,
			Timestamp: toString(fields["timestamp"]),
		}
		if sendErr := stream.Send(out); sendErr != nil {
			return status.Errorf(codes.Unavailable, "failed to stream event: %v", sendErr)
		}
	}

	return nil
}

func (s *Sidecar) Ack(ctx context.Context, req *synapsev1.AckRequest) (*synapsev1.AckResponse, error) {
	s.incrementAckRequest()

	if req == nil {
		s.incrementError()
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	acked, statusCode, err := s.ackEntriesInternal(ctx, ackRequest{
		Stream:        req.GetStream(),
		ConsumerGroup: req.GetConsumerGroup(),
		EntryIDs:      req.GetEntryIds(),
	})
	if err != nil {
		return nil, grpcStatusFromHTTPStatus(statusCode, err.Error())
	}

	return &synapsev1.AckResponse{Acked: int32(acked)}, nil
}

func (s *Sidecar) StreamInfo(ctx context.Context, req *synapsev1.StreamInfoRequest) (*synapsev1.StreamInfoResponse, error) {
	streamName := ""
	if req != nil {
		streamName = strings.TrimSpace(req.GetStream())
	}
	if streamName == "" {
		s.incrementError()
		return nil, status.Error(codes.InvalidArgument, "stream is required")
	}
	if !config.IsStreamAllowed(s.cfg.PublishStreams, streamName) && !config.IsStreamAllowed(s.cfg.ConsumeStreams, streamName) {
		s.incrementError()
		return nil, status.Error(codes.PermissionDenied, "stream not allowed for this sugar glider")
	}

	info, err := s.redis.Raw().XInfoStream(ctx, streamName).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) || isNoSuchStreamErr(err) {
			return &synapsev1.StreamInfoResponse{}, nil
		}
		s.incrementError()
		return nil, status.Error(codes.Unavailable, "failed to fetch stream info")
	}

	groups, err := s.redis.Raw().XInfoGroups(ctx, streamName).Result()
	if err != nil && !errors.Is(err, redis.Nil) && !isNoSuchStreamErr(err) {
		s.incrementError()
		return nil, status.Error(codes.Unavailable, "failed to fetch stream groups")
	}

	return &synapsev1.StreamInfoResponse{
		Length:       info.Length,
		Groups:       int32(len(groups)),
		FirstEntryId: info.FirstEntry.ID,
		LastEntryId:  info.LastEntry.ID,
	}, nil
}

func (s *Sidecar) Health(ctx context.Context, _ *synapsev1.HealthRequest) (*synapsev1.HealthResponse, error) {
	if err := s.CheckReady(ctx); err != nil {
		return &synapsev1.HealthResponse{
			Healthy:     false,
			RedisStatus: "down",
		}, nil
	}

	return &synapsev1.HealthResponse{
		Healthy:     true,
		RedisStatus: "ok",
	}, nil
}

func grpcStatusFromHTTPStatus(statusCode int, message string) error {
	switch statusCode {
	case http.StatusBadRequest:
		return status.Error(codes.InvalidArgument, message)
	case http.StatusForbidden:
		return status.Error(codes.PermissionDenied, message)
	case http.StatusNotFound:
		return status.Error(codes.NotFound, message)
	case http.StatusServiceUnavailable:
		return status.Error(codes.Unavailable, message)
	default:
		return status.Error(codes.Internal, message)
	}
}

func toString(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprint(v)
	}
}
