package config

import (
	"testing"
	"time"
)

func TestLoad_PublishPipelineDefaults(t *testing.T) {
	setBaselineEnv(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.PublishPipelineEnabled {
		t.Fatalf("PublishPipelineEnabled = true, want false")
	}
	if cfg.PublishPipelineAdaptiveEnabled {
		t.Fatalf("PublishPipelineAdaptiveEnabled = true, want false")
	}
	if cfg.PublishPipelineMaxBatch != 64 {
		t.Fatalf("PublishPipelineMaxBatch = %d, want 64", cfg.PublishPipelineMaxBatch)
	}
	if cfg.PublishPipelineMinBatch != 2 {
		t.Fatalf("PublishPipelineMinBatch = %d, want 2", cfg.PublishPipelineMinBatch)
	}
	if cfg.PublishPipelineFlushInterval != 0 {
		t.Fatalf("PublishPipelineFlushInterval = %v, want 0", cfg.PublishPipelineFlushInterval)
	}
	if cfg.PublishPipelineQueueSize != 8192 {
		t.Fatalf("PublishPipelineQueueSize = %d, want 8192", cfg.PublishPipelineQueueSize)
	}
	if cfg.PublishPipelineMaxBytes != 1048576 {
		t.Fatalf("PublishPipelineMaxBytes = %d, want 1048576", cfg.PublishPipelineMaxBytes)
	}
}

func TestLoad_PublishPipelineEnvOverrides(t *testing.T) {
	setBaselineEnv(t)
	t.Setenv("SIDECAR_PUBLISH_PIPELINE_ENABLED", "true")
	t.Setenv("SIDECAR_PUBLISH_PIPELINE_ADAPTIVE_ENABLED", "true")
	t.Setenv("SIDECAR_PUBLISH_PIPELINE_MAX_BATCH", "128")
	t.Setenv("SIDECAR_PUBLISH_PIPELINE_MIN_BATCH", "4")
	t.Setenv("SIDECAR_PUBLISH_PIPELINE_FLUSH_MS", "5")
	t.Setenv("SIDECAR_PUBLISH_PIPELINE_QUEUE_SIZE", "4096")
	t.Setenv("SIDECAR_PUBLISH_PIPELINE_MAX_BYTES", "2097152")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !cfg.PublishPipelineEnabled {
		t.Fatalf("PublishPipelineEnabled = false, want true")
	}
	if !cfg.PublishPipelineAdaptiveEnabled {
		t.Fatalf("PublishPipelineAdaptiveEnabled = false, want true")
	}
	if cfg.PublishPipelineMaxBatch != 128 {
		t.Fatalf("PublishPipelineMaxBatch = %d, want 128", cfg.PublishPipelineMaxBatch)
	}
	if cfg.PublishPipelineMinBatch != 4 {
		t.Fatalf("PublishPipelineMinBatch = %d, want 4", cfg.PublishPipelineMinBatch)
	}
	if cfg.PublishPipelineFlushInterval != 5*time.Millisecond {
		t.Fatalf("PublishPipelineFlushInterval = %v, want %v", cfg.PublishPipelineFlushInterval, 5*time.Millisecond)
	}
	if cfg.PublishPipelineQueueSize != 4096 {
		t.Fatalf("PublishPipelineQueueSize = %d, want 4096", cfg.PublishPipelineQueueSize)
	}
	if cfg.PublishPipelineMaxBytes != 2097152 {
		t.Fatalf("PublishPipelineMaxBytes = %d, want 2097152", cfg.PublishPipelineMaxBytes)
	}
}

func TestLoad_PublishPipelineValidation(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
	}{
		{
			name: "max batch must be positive",
			env:  map[string]string{"SIDECAR_PUBLISH_PIPELINE_MAX_BATCH": "0"},
		},
		{
			name: "min batch must be positive",
			env:  map[string]string{"SIDECAR_PUBLISH_PIPELINE_MIN_BATCH": "0"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setBaselineEnv(t)
			for key, value := range tt.env {
				t.Setenv(key, value)
			}

			_, err := Load()
			if err == nil {
				t.Fatalf("Load() error = nil, want validation error")
			}
		})
	}
}

func setBaselineEnv(t *testing.T) {
	t.Helper()

	t.Setenv("SIDECAR_SERVICE_NAME", "real-time-gateway")
	t.Setenv("SIDECAR_REDIS_URL", "redis://127.0.0.1:6379/0")
	t.Setenv("SIDECAR_LISTEN_ADDR", "tcp://0.0.0.0:8081")
	t.Setenv("SIDECAR_GRPC_ADDR", "tcp://0.0.0.0:50051")

	t.Setenv("SIDECAR_PUBLISH_PIPELINE_ENABLED", "false")
	t.Setenv("SIDECAR_PUBLISH_PIPELINE_ADAPTIVE_ENABLED", "false")
	t.Setenv("SIDECAR_PUBLISH_PIPELINE_MAX_BATCH", "64")
	t.Setenv("SIDECAR_PUBLISH_PIPELINE_MIN_BATCH", "2")
	t.Setenv("SIDECAR_PUBLISH_PIPELINE_FLUSH_MS", "0")
	t.Setenv("SIDECAR_PUBLISH_PIPELINE_QUEUE_SIZE", "8192")
	t.Setenv("SIDECAR_PUBLISH_PIPELINE_MAX_BYTES", "1048576")

	t.Setenv("SIDECAR_CONSUME_MODE", "stateless")
	t.Setenv("SIDECAR_WAL_REPLAY_MODE", "background")
	t.Setenv("SIDECAR_DISPATCHER_CONSUMER_NAME", "sugar-glider-dispatcher")
	t.Setenv("SIDECAR_DISPATCHER_READ_COUNT", "100")
	t.Setenv("SIDECAR_DISPATCHER_BLOCK_MS", "1000")
	t.Setenv("SIDECAR_DISPATCHER_SUBSCRIBER_BUFFER", "256")
	t.Setenv("SIDECAR_DISPATCHER_ACK_BATCH_SIZE", "64")
	t.Setenv("SIDECAR_DISPATCHER_ACK_FLUSH_CONCURRENCY", "2")
	t.Setenv("SIDECAR_DISPATCHER_ACK_FLUSH_MS", "10")
	t.Setenv("SIDECAR_DISPATCHER_ACK_QUEUE_SIZE", "4096")

	t.Setenv("SIDECAR_WAL_DIR", t.TempDir())
	t.Setenv("SIDECAR_WAL_REPLAY_BATCH", "100")
	t.Setenv("SIDECAR_WAL_REPLAY_INTERVAL_MS", "2000")
	t.Setenv("SIDECAR_PUBLISH_STREAMS", "platform-events")
	t.Setenv("SIDECAR_CONSUME_STREAMS", "platform-events")
	t.Setenv("SIDECAR_MAX_STREAM_LEN", "10000")
	t.Setenv("SIDECAR_DLQ_MAX_RETRIES", "3")
	t.Setenv("SIDECAR_DLQ_MIN_IDLE_MS", "30000")
	t.Setenv("SIDECAR_DLQ_SCAN_INTERVAL_MS", "5000")
	t.Setenv("SIDECAR_DLQ_SCAN_BATCH", "100")
	t.Setenv("SIDECAR_READINESS_TIMEOUT_MS", "1500")
}
