package wal

import (
	"context"
	"errors"
	"testing"

	"github.com/Team-Deepiri/deepiri-platform/platform-services/backend/deepiri-realtime-gateway/synapse-sidecar/internal/redisstreams"
)

func TestReplaySuccessDrainsWAL(t *testing.T) {
	t.Parallel()

	logDir := t.TempDir()
	w, err := New(logDir)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	reqA := redisstreams.PublishRequest{
		Stream:    "platform-events",
		EventType: "a",
		Payload:   []byte(`{"ok":true}`),
	}
	reqB := redisstreams.PublishRequest{
		Stream:    "platform-events",
		EventType: "b",
		Payload:   []byte(`{"ok":true}`),
	}

	if err := w.Append("redis down", reqA); err != nil {
		t.Fatalf("Append(reqA) error = %v", err)
	}
	if err := w.Append("redis down", reqB); err != nil {
		t.Fatalf("Append(reqB) error = %v", err)
	}

	calls := 0
	replayed, err := w.Replay(context.Background(), 10, func(_ context.Context, req redisstreams.PublishRequest) error {
		calls++
		if req.EventType == "" {
			t.Fatalf("unexpected empty event type")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if replayed != 2 {
		t.Fatalf("Replay() replayed = %d, want 2", replayed)
	}
	if calls != 2 {
		t.Fatalf("publish calls = %d, want 2", calls)
	}

	depth, err := w.Depth()
	if err != nil {
		t.Fatalf("Depth() error = %v", err)
	}
	if depth != 0 {
		t.Fatalf("Depth() = %d, want 0", depth)
	}
}

func TestReplayFailureRetainsEntries(t *testing.T) {
	t.Parallel()

	logDir := t.TempDir()
	w, err := New(logDir)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	reqA := redisstreams.PublishRequest{
		Stream:    "platform-events",
		EventType: "a",
		Payload:   []byte(`{"ok":true}`),
	}
	reqB := redisstreams.PublishRequest{
		Stream:    "platform-events",
		EventType: "b",
		Payload:   []byte(`{"ok":true}`),
	}

	if err := w.Append("redis down", reqA); err != nil {
		t.Fatalf("Append(reqA) error = %v", err)
	}
	if err := w.Append("redis down", reqB); err != nil {
		t.Fatalf("Append(reqB) error = %v", err)
	}

	replayed, err := w.Replay(context.Background(), 10, func(_ context.Context, req redisstreams.PublishRequest) error {
		if req.EventType == "a" {
			return errors.New("still down")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if replayed != 0 {
		t.Fatalf("Replay() replayed = %d, want 0", replayed)
	}

	depth, err := w.Depth()
	if err != nil {
		t.Fatalf("Depth() error = %v", err)
	}
	if depth != 2 {
		t.Fatalf("Depth() = %d, want 2", depth)
	}
}
