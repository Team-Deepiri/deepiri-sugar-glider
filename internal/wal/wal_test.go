package wal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Team-Deepiri/deepiri-sugar-glider/internal/redisstreams"
)

func TestNewUsesSugarGliderWalByDefault(t *testing.T) {
	t.Parallel()

	logDir := t.TempDir()
	w, err := New(logDir)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if !strings.HasSuffix(w.Path(), sugarGliderWALFilename) {
		t.Fatalf("Path() = %q, want suffix %q", w.Path(), sugarGliderWALFilename)
	}
}

func TestNewUsesLegacyWalWhenPresent(t *testing.T) {
	t.Parallel()

	logDir := t.TempDir()
	legacyPath := filepath.Join(logDir, legacyWALFilename)
	if err := os.WriteFile(legacyPath, []byte(""), 0o644); err != nil {
		t.Fatalf("WriteFile(legacy) error = %v", err)
	}

	w, err := New(logDir)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if w.Path() != legacyPath {
		t.Fatalf("Path() = %q, want %q", w.Path(), legacyPath)
	}
}

func TestNewPrefersSugarGliderWalWhenBothPresent(t *testing.T) {
	t.Parallel()

	logDir := t.TempDir()
	sugarPath := filepath.Join(logDir, sugarGliderWALFilename)
	legacyPath := filepath.Join(logDir, legacyWALFilename)
	if err := os.WriteFile(sugarPath, []byte(""), 0o644); err != nil {
		t.Fatalf("WriteFile(sugar) error = %v", err)
	}
	if err := os.WriteFile(legacyPath, []byte(""), 0o644); err != nil {
		t.Fatalf("WriteFile(legacy) error = %v", err)
	}

	w, err := New(logDir)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if w.Path() != sugarPath {
		t.Fatalf("Path() = %q, want %q", w.Path(), sugarPath)
	}
}

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

func TestAppendRespectsMaxEntries(t *testing.T) {
	t.Parallel()

	logDir := t.TempDir()
	w, err := New(logDir, Options{MaxEntries: 2})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	req := redisstreams.PublishRequest{
		Stream:    "platform-events",
		EventType: "a",
		Payload:   []byte(`{"ok":true}`),
	}
	for i := 0; i < 2; i++ {
		if err := w.Append("redis down", req); err != nil {
			t.Fatalf("Append(%d) error = %v", i, err)
		}
	}
	if err := w.Append("redis down", req); !errors.Is(err, ErrWALFull) {
		t.Fatalf("Append() error = %v, want ErrWALFull", err)
	}

	depth, err := w.Depth()
	if err != nil {
		t.Fatalf("Depth() error = %v", err)
	}
	if depth != 2 {
		t.Fatalf("Depth() = %d, want 2", depth)
	}
}

func TestDepthUsesIncrementalCounter(t *testing.T) {
	t.Parallel()

	logDir := t.TempDir()
	w, err := New(logDir)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	req := redisstreams.PublishRequest{
		Stream:    "platform-events",
		EventType: "a",
		Payload:   []byte(`{"ok":true}`),
	}
	if err := w.Append("redis down", req); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	depth, err := w.Depth()
	if err != nil {
		t.Fatalf("Depth() error = %v", err)
	}
	if depth != 1 {
		t.Fatalf("Depth() = %d, want 1", depth)
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
