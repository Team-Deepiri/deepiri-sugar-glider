package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Team-Deepiri/deepiri-sugar-glider/internal/config"
	"github.com/redis/go-redis/v9"
)

type dlqReplayRequest struct {
	DLQStream     string `json:"dlq_stream"`
	TargetStream  string `json:"target_stream,omitempty"`
	Count         int64  `json:"count,omitempty"`
	Start         string `json:"start,omitempty"`
	End           string `json:"end,omitempty"`
	DeleteFromDLQ bool   `json:"delete_from_dlq,omitempty"`
}

type dlqReplayResult struct {
	Replayed  int
	Skipped   int
	EntryIDs  []string
}

func (s *Sidecar) replayDLQHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req dlqReplayRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.incrementError()
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid JSON"})
		return
	}

	result, statusCode, err := s.replayDLQInternal(r.Context(), req)
	if err != nil {
		writeJSON(w, statusCode, map[string]any{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"replayed":  result.Replayed,
		"skipped":   result.Skipped,
		"entry_ids": result.EntryIDs,
	})
}

func (s *Sidecar) replayDLQInternal(ctx context.Context, req dlqReplayRequest) (dlqReplayResult, int, error) {
	req.DLQStream = strings.TrimSpace(req.DLQStream)
	req.TargetStream = strings.TrimSpace(req.TargetStream)
	req.Start = strings.TrimSpace(req.Start)
	req.End = strings.TrimSpace(req.End)

	if req.DLQStream == "" {
		s.incrementError()
		return dlqReplayResult{}, http.StatusBadRequest, errors.New("dlq_stream is required")
	}
	if req.Count <= 0 {
		req.Count = 100
	}
	if req.Count > 1000 {
		req.Count = 1000
	}
	if req.Start == "" {
		req.Start = "-"
	}
	if req.End == "" {
		req.End = "+"
	}

	messages, err := s.redis.Raw().XRangeN(ctx, req.DLQStream, req.Start, req.End, req.Count).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) || isNoSuchStreamErr(err) {
			return dlqReplayResult{}, http.StatusOK, nil
		}
		s.incrementError()
		return dlqReplayResult{}, http.StatusServiceUnavailable, fmt.Errorf("failed to read dlq stream: %w", err)
	}

	result := dlqReplayResult{EntryIDs: make([]string, 0, len(messages))}
	for _, message := range messages {
		targetStream := req.TargetStream
		if targetStream == "" {
			if raw, ok := message.Values["dlq_original_stream"]; ok {
				targetStream = strings.TrimSpace(fmt.Sprint(raw))
			}
		}
		if targetStream == "" {
			result.Skipped++
			continue
		}
		if !config.IsStreamAllowed(s.cfg.PublishStreams, targetStream) {
			result.Skipped++
			continue
		}

		values := make(map[string]any, len(message.Values)+2)
		for key, value := range message.Values {
			if strings.HasPrefix(key, "dlq_") {
				continue
			}
			values[key] = value
		}
		values["dlq_replayed_from"] = req.DLQStream
		values["dlq_replayed_at"] = time.Now().UTC().Format(time.RFC3339Nano)
		if originalID, ok := message.Values["dlq_original_id"]; ok {
			values["dlq_original_id"] = originalID
		}

		if _, addErr := s.redis.Raw().XAdd(ctx, &redis.XAddArgs{
			Stream: targetStream,
			MaxLen: s.cfg.MaxStreamLen,
			Approx: true,
			Values: values,
		}).Result(); addErr != nil {
			s.incrementError()
			return result, http.StatusServiceUnavailable, fmt.Errorf("failed to requeue dlq entry %s: %w", message.ID, addErr)
		}

		if req.DeleteFromDLQ {
			if _, delErr := s.redis.Raw().XDel(ctx, req.DLQStream, message.ID).Result(); delErr != nil {
				s.incrementError()
				return result, http.StatusServiceUnavailable, fmt.Errorf("requeued but failed to delete dlq entry %s: %w", message.ID, delErr)
			}
		}

		result.Replayed++
		result.EntryIDs = append(result.EntryIDs, message.ID)
	}

	if result.Replayed > 0 {
		s.incrementDLQReplayed(uint64(result.Replayed))
	}
	return result, http.StatusOK, nil
}
