package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/Team-Deepiri/deepiri-sugar-glider/internal/config"
	"github.com/redis/go-redis/v9"
)

type readRequest struct {
	Stream        string `json:"stream"`
	ConsumerGroup string `json:"consumer_group"`
	ConsumerName  string `json:"consumer_name"`
	Count         int64  `json:"count,omitempty"`
	BlockMS       int64  `json:"block_ms,omitempty"`
}

type readEvent struct {
	Stream string         `json:"stream"`
	Entry  string         `json:"entry_id"`
	Fields map[string]any `json:"fields"`
}

type ackRequest struct {
	Stream        string   `json:"stream"`
	ConsumerGroup string   `json:"consumer_group"`
	EntryIDs      []string `json:"entry_ids"`
}

func (s *Sidecar) readFromStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	s.incrementReadRequest()

	var req readRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.incrementError()
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid JSON"})
		return
	}

	events, statusCode, err := s.consumeGroupOnce(r.Context(), req)
	if err != nil {
		writeJSON(w, statusCode, map[string]any{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"events": events})
}

func (s *Sidecar) ackEntries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	s.incrementAckRequest()

	var req ackRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.incrementError()
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid JSON"})
		return
	}

	acked, statusCode, err := s.ackEntriesInternal(r.Context(), req)
	if err != nil {
		writeJSON(w, statusCode, map[string]any{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"acked": acked})
}

func (s *Sidecar) consumeGroupOnce(ctx context.Context, req readRequest) ([]readEvent, int, error) {
	req.Stream = strings.TrimSpace(req.Stream)
	req.ConsumerGroup = strings.TrimSpace(req.ConsumerGroup)
	req.ConsumerName = strings.TrimSpace(req.ConsumerName)
	if req.Stream == "" || req.ConsumerGroup == "" || req.ConsumerName == "" {
		s.incrementError()
		return nil, http.StatusBadRequest, errors.New("stream, consumer_group, and consumer_name are required")
	}
	if !config.IsStreamAllowed(s.cfg.ConsumeStreams, req.Stream) {
		s.incrementError()
		return nil, http.StatusForbidden, errors.New("stream not allowed for this sugar glider")
	}
	if req.Count <= 0 {
		req.Count = 10
	}
	if req.Count > 100 {
		req.Count = 100
	}
	if req.BlockMS <= 0 {
		req.BlockMS = 1000
	}

	if err := s.ensureConsumerGroup(ctx, req.Stream, req.ConsumerGroup); err != nil {
		s.incrementError()
		return nil, http.StatusServiceUnavailable, errors.New("failed to ensure consumer group")
	}

	readOnce := func() ([]redis.XStream, error) {
		return s.redis.Raw().XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    req.ConsumerGroup,
			Consumer: req.ConsumerName,
			Streams:  []string{req.Stream, ">"},
			Count:    req.Count,
			Block:    time.Duration(req.BlockMS) * time.Millisecond,
		}).Result()
	}

	streams, err := readOnce()
	if err != nil && isNoGroupError(err) {
		s.forgetEnsuredConsumerGroup(req.Stream, req.ConsumerGroup)
		if ensureErr := s.ensureConsumerGroup(ctx, req.Stream, req.ConsumerGroup); ensureErr == nil {
			streams, err = readOnce()
		}
	}
	if err == redis.Nil {
		return []readEvent{}, http.StatusOK, nil
	}
	if err != nil {
		s.incrementError()
		return nil, http.StatusServiceUnavailable, errors.New("failed to read from stream")
	}

	events := make([]readEvent, 0)
	for _, stream := range streams {
		for _, msg := range stream.Messages {
			fields := make(map[string]any, len(msg.Values))
			for key, value := range msg.Values {
				fields[key] = normalizeRedisValue(value)
			}
			events = append(events, readEvent{
				Stream: stream.Stream,
				Entry:  msg.ID,
				Fields: fields,
			})
		}
	}

	s.incrementReadEvents(uint64(len(events)))
	return events, http.StatusOK, nil
}

func (s *Sidecar) ackEntriesInternal(ctx context.Context, req ackRequest) (int64, int, error) {
	req.Stream = strings.TrimSpace(req.Stream)
	req.ConsumerGroup = strings.TrimSpace(req.ConsumerGroup)
	if req.Stream == "" || req.ConsumerGroup == "" || len(req.EntryIDs) == 0 {
		s.incrementError()
		return 0, http.StatusBadRequest, errors.New("stream, consumer_group, and entry_ids are required")
	}
	if !config.IsStreamAllowed(s.cfg.ConsumeStreams, req.Stream) {
		s.incrementError()
		return 0, http.StatusForbidden, errors.New("stream not allowed for this sugar glider")
	}

	acked, err := s.redis.Raw().XAck(ctx, req.Stream, req.ConsumerGroup, req.EntryIDs...).Result()
	if err != nil {
		s.incrementError()
		return 0, http.StatusServiceUnavailable, errors.New("failed to ack stream entries")
	}

	s.incrementAckedEntries(uint64(acked))
	return acked, http.StatusOK, nil
}

func (s *Sidecar) ensureConsumerGroup(ctx context.Context, stream string, consumerGroup string) error {
	if s.isConsumerGroupEnsured(stream, consumerGroup) {
		return nil
	}
	s.incrementGroupEnsureAttempt()
	err := s.redis.Raw().XGroupCreateMkStream(ctx, stream, consumerGroup, "0").Err()
	if err == nil {
		s.rememberConsumerGroup(stream, consumerGroup)
		return nil
	}
	if strings.Contains(strings.ToUpper(err.Error()), "BUSYGROUP") {
		s.rememberConsumerGroup(stream, consumerGroup)
		return nil
	}
	return err
}

func (s *Sidecar) isConsumerGroupEnsured(stream string, consumerGroup string) bool {
	key := consumerGroupKey(stream, consumerGroup)
	s.consumerGroupMu.RLock()
	_, ok := s.consumerGroupsEnsured[key]
	s.consumerGroupMu.RUnlock()
	return ok
}

func (s *Sidecar) rememberConsumerGroup(stream string, consumerGroup string) {
	key := consumerGroupKey(stream, consumerGroup)
	s.consumerGroupMu.Lock()
	s.consumerGroupsEnsured[key] = struct{}{}
	s.consumerGroupMu.Unlock()
}

func (s *Sidecar) forgetEnsuredConsumerGroup(stream string, consumerGroup string) {
	key := consumerGroupKey(stream, consumerGroup)
	s.consumerGroupMu.Lock()
	delete(s.consumerGroupsEnsured, key)
	s.consumerGroupMu.Unlock()
}

func consumerGroupKey(stream string, consumerGroup string) string {
	return strings.TrimSpace(stream) + "|" + strings.TrimSpace(consumerGroup)
}

func normalizeRedisValue(value any) any {
	switch v := value.(type) {
	case []byte:
		return string(v)
	default:
		return v
	}
}
