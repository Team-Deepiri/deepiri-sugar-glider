package wal

import (
	"testing"

	"github.com/Team-Deepiri/deepiri-sugar-glider/internal/redisstreams"
)

func BenchmarkDepthIncremental(b *testing.B) {
	logDir := b.TempDir()
	w, err := New(logDir)
	if err != nil {
		b.Fatalf("New() error = %v", err)
	}

	req := redisstreams.PublishRequest{
		Stream:    "platform-events",
		EventType: "bench",
		Payload:   []byte(`{"ok":true}`),
	}
	for i := 0; i < 1000; i++ {
		if err := w.Append("bench", req); err != nil {
			b.Fatalf("Append() error = %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := w.Depth(); err != nil {
			b.Fatalf("Depth() error = %v", err)
		}
	}
}
