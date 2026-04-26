package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ServiceName                    string
	RedisURL                       string
	ListenAddr                     string
	GRPCListenAddr                 string
	PublishPipelineEnabled         bool
	PublishPipelineAdaptiveEnabled bool
	PublishPipelineMaxBatch        int64
	PublishPipelineMinBatch        int64
	PublishPipelineFlushInterval   time.Duration
	PublishPipelineQueueSize       int64
	PublishPipelineMaxBytes        int64
	ConsumeMode                    string
	WALReplayMode                  string
	DispatcherConsumerName         string
	DispatcherReadCount            int64
	DispatcherBlockMS              int64
	DispatcherSubscriberBuffer     int64
	DispatcherAckBatchSize         int64
	DispatcherAckFlushConcurrency  int64
	DispatcherAckFlushInterval     time.Duration
	DispatcherAckQueueSize         int64
	WALDir                         string
	WALReplayBatch                 int64
	WALReplayInterval              time.Duration
	PublishStreams                 []string
	ConsumeStreams                 []string
	MaxStreamLen                   int64
	DLQMaxRetries                  int64
	DLQMinIdle                     time.Duration
	DLQScanInterval                time.Duration
	ReadinessTimeout               time.Duration
}

const (
	ConsumeModeStateless       = "stateless"
	ConsumeModeDispatcher      = "dispatcher"
	WALReplayModeBackground    = "background"
	WALReplayModeSyncOnSuccess = "sync_on_success"
)

func Load() (Config, error) {
	cfg := Config{
		ServiceName:                    getEnv("SIDECAR_SERVICE_NAME", "real-time-gateway"),
		RedisURL:                       os.Getenv("SIDECAR_REDIS_URL"),
		ListenAddr:                     getEnv("SIDECAR_LISTEN_ADDR", "tcp://0.0.0.0:8081"),
		GRPCListenAddr:                 getEnv("SIDECAR_GRPC_ADDR", "tcp://0.0.0.0:50051"),
		PublishPipelineEnabled:         getEnvBool("SIDECAR_PUBLISH_PIPELINE_ENABLED", false),
		PublishPipelineAdaptiveEnabled: getEnvBool("SIDECAR_PUBLISH_PIPELINE_ADAPTIVE_ENABLED", false),
		PublishPipelineMaxBatch:        getEnvInt64("SIDECAR_PUBLISH_PIPELINE_MAX_BATCH", 64),
		PublishPipelineMinBatch:        getEnvInt64("SIDECAR_PUBLISH_PIPELINE_MIN_BATCH", 2),
		PublishPipelineFlushInterval:   time.Duration(getEnvInt64("SIDECAR_PUBLISH_PIPELINE_FLUSH_MS", 0)) * time.Millisecond,
		PublishPipelineQueueSize:       getEnvInt64("SIDECAR_PUBLISH_PIPELINE_QUEUE_SIZE", 8192),
		PublishPipelineMaxBytes:        getEnvInt64("SIDECAR_PUBLISH_PIPELINE_MAX_BYTES", 1048576),
		ConsumeMode:                    strings.ToLower(getEnv("SIDECAR_CONSUME_MODE", ConsumeModeStateless)),
		WALReplayMode:                  strings.ToLower(getEnv("SIDECAR_WAL_REPLAY_MODE", WALReplayModeBackground)),
		DispatcherConsumerName:         getEnv("SIDECAR_DISPATCHER_CONSUMER_NAME", "sugar-glider-dispatcher"),
		DispatcherReadCount:            getEnvInt64("SIDECAR_DISPATCHER_READ_COUNT", 100),
		DispatcherBlockMS:              getEnvInt64("SIDECAR_DISPATCHER_BLOCK_MS", 1000),
		DispatcherSubscriberBuffer:     getEnvInt64("SIDECAR_DISPATCHER_SUBSCRIBER_BUFFER", 256),
		DispatcherAckBatchSize:         getEnvInt64("SIDECAR_DISPATCHER_ACK_BATCH_SIZE", 64),
		DispatcherAckFlushConcurrency:  getEnvInt64("SIDECAR_DISPATCHER_ACK_FLUSH_CONCURRENCY", 2),
		DispatcherAckFlushInterval:     time.Duration(getEnvInt64("SIDECAR_DISPATCHER_ACK_FLUSH_MS", 10)) * time.Millisecond,
		DispatcherAckQueueSize:         getEnvInt64("SIDECAR_DISPATCHER_ACK_QUEUE_SIZE", 4096),
		WALDir:                         getEnv("SIDECAR_WAL_DIR", "/data/synapse-wal"),
		WALReplayBatch:                 getEnvInt64("SIDECAR_WAL_REPLAY_BATCH", 100),
		WALReplayInterval:              time.Duration(getEnvInt64("SIDECAR_WAL_REPLAY_INTERVAL_MS", 2000)) * time.Millisecond,
		PublishStreams:                 splitCSV(getEnv("SIDECAR_PUBLISH_STREAMS", "platform-events")),
		ConsumeStreams:                 splitCSV(getEnv("SIDECAR_CONSUME_STREAMS", "")),
		MaxStreamLen:                   getEnvInt64("SIDECAR_MAX_STREAM_LEN", 10000),
		DLQMaxRetries:                  getEnvInt64("SIDECAR_DLQ_MAX_RETRIES", 3),
		DLQMinIdle:                     time.Duration(getEnvInt64("SIDECAR_DLQ_MIN_IDLE_MS", 30000)) * time.Millisecond,
		DLQScanInterval:                time.Duration(getEnvInt64("SIDECAR_DLQ_SCAN_INTERVAL_MS", 5000)) * time.Millisecond,
		ReadinessTimeout:               time.Duration(getEnvInt64("SIDECAR_READINESS_TIMEOUT_MS", 1500)) * time.Millisecond,
	}

	if cfg.RedisURL == "" {
		return Config{}, fmt.Errorf("SIDECAR_REDIS_URL is required")
	}
	if cfg.ConsumeMode != ConsumeModeStateless && cfg.ConsumeMode != ConsumeModeDispatcher {
		return Config{}, fmt.Errorf("SIDECAR_CONSUME_MODE must be one of: %s, %s", ConsumeModeStateless, ConsumeModeDispatcher)
	}
	if cfg.PublishPipelineMaxBatch <= 0 {
		return Config{}, fmt.Errorf("SIDECAR_PUBLISH_PIPELINE_MAX_BATCH must be > 0")
	}
	if cfg.PublishPipelineMinBatch <= 0 {
		return Config{}, fmt.Errorf("SIDECAR_PUBLISH_PIPELINE_MIN_BATCH must be > 0")
	}
	if cfg.PublishPipelineFlushInterval < 0 {
		return Config{}, fmt.Errorf("SIDECAR_PUBLISH_PIPELINE_FLUSH_MS must be >= 0")
	}
	if cfg.PublishPipelineQueueSize <= 0 {
		return Config{}, fmt.Errorf("SIDECAR_PUBLISH_PIPELINE_QUEUE_SIZE must be > 0")
	}
	if cfg.PublishPipelineMaxBytes <= 0 {
		return Config{}, fmt.Errorf("SIDECAR_PUBLISH_PIPELINE_MAX_BYTES must be > 0")
	}
	if cfg.WALReplayMode != WALReplayModeBackground && cfg.WALReplayMode != WALReplayModeSyncOnSuccess {
		return Config{}, fmt.Errorf("SIDECAR_WAL_REPLAY_MODE must be one of: %s, %s", WALReplayModeBackground, WALReplayModeSyncOnSuccess)
	}
	if strings.TrimSpace(cfg.DispatcherConsumerName) == "" {
		return Config{}, fmt.Errorf("SIDECAR_DISPATCHER_CONSUMER_NAME must be non-empty")
	}
	if cfg.DispatcherReadCount <= 0 {
		return Config{}, fmt.Errorf("SIDECAR_DISPATCHER_READ_COUNT must be > 0")
	}
	if cfg.DispatcherBlockMS <= 0 {
		return Config{}, fmt.Errorf("SIDECAR_DISPATCHER_BLOCK_MS must be > 0")
	}
	if cfg.DispatcherSubscriberBuffer <= 0 {
		return Config{}, fmt.Errorf("SIDECAR_DISPATCHER_SUBSCRIBER_BUFFER must be > 0")
	}
	if cfg.DispatcherAckBatchSize <= 0 {
		return Config{}, fmt.Errorf("SIDECAR_DISPATCHER_ACK_BATCH_SIZE must be > 0")
	}
	if cfg.DispatcherAckFlushConcurrency <= 0 {
		return Config{}, fmt.Errorf("SIDECAR_DISPATCHER_ACK_FLUSH_CONCURRENCY must be > 0")
	}
	if cfg.DispatcherAckFlushInterval <= 0 {
		return Config{}, fmt.Errorf("SIDECAR_DISPATCHER_ACK_FLUSH_MS must be > 0")
	}
	if cfg.DispatcherAckQueueSize <= 0 {
		return Config{}, fmt.Errorf("SIDECAR_DISPATCHER_ACK_QUEUE_SIZE must be > 0")
	}
	if cfg.MaxStreamLen <= 0 {
		return Config{}, fmt.Errorf("SIDECAR_MAX_STREAM_LEN must be > 0")
	}
	if cfg.WALReplayBatch < 0 {
		return Config{}, fmt.Errorf("SIDECAR_WAL_REPLAY_BATCH must be >= 0")
	}
	if cfg.WALReplayInterval < 0 {
		return Config{}, fmt.Errorf("SIDECAR_WAL_REPLAY_INTERVAL_MS must be >= 0")
	}
	if cfg.DLQMaxRetries < 0 {
		return Config{}, fmt.Errorf("SIDECAR_DLQ_MAX_RETRIES must be >= 0")
	}
	if cfg.DLQMinIdle < 0 {
		return Config{}, fmt.Errorf("SIDECAR_DLQ_MIN_IDLE_MS must be >= 0")
	}
	if cfg.DLQScanInterval < 0 {
		return Config{}, fmt.Errorf("SIDECAR_DLQ_SCAN_INTERVAL_MS must be >= 0")
	}

	return cfg, nil
}

func IsStreamAllowed(allowed []string, stream string) bool {
	if len(allowed) == 0 {
		return true
	}
	for _, s := range allowed {
		if s == stream {
			return true
		}
	}
	return false
}

func ParseListenAddress(value string) (network, address string, err error) {
	parts := strings.SplitN(value, "://", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("SIDECAR_LISTEN_ADDR must be formatted as <network>://<address>")
	}
	network, address = parts[0], parts[1]
	if network != "tcp" && network != "unix" {
		return "", "", fmt.Errorf("unsupported network %q, expected tcp or unix", network)
	}
	if address == "" {
		return "", "", fmt.Errorf("listen address cannot be empty")
	}
	return network, address, nil
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func getEnvInt64(key string, fallback int64) int64 {
	if val := os.Getenv(key); val != "" {
		parsed, err := strconv.ParseInt(val, 10, 64)
		if err == nil {
			return parsed
		}
	}
	return fallback
}

func getEnvBool(key string, fallback bool) bool {
	val := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if val == "" {
		return fallback
	}
	switch val {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func splitCSV(value string) []string {
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
