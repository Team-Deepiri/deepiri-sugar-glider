package config

import (
	"fmt"
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
	WALMaxEntries                  int64
	WALMaxBytes                    int64
	WALReplayBatch                 int64
	WALReplayInterval              time.Duration
	PublishStreams                 []string
	ConsumeStreams                 []string
	MaxStreamLen                   int64
	DLQMaxRetries                  int64
	DLQMinIdle                     time.Duration
	DLQScanInterval                time.Duration
	DLQScanBatch                   int64
	DLQStreamPolicies              map[string]StreamDLQPolicy
	ReadinessTimeout               time.Duration
	ReadyMaxWALDepth               int64
	ReadyMaxPublishQueueDepth      int64
}

const (
	ConsumeModeStateless       = "stateless"
	ConsumeModeDispatcher      = "dispatcher"
	WALReplayModeBackground    = "background"
	WALReplayModeSyncOnSuccess = "sync_on_success"
)

func Load() (Config, error) {
	cfg := Config{
		ServiceName:                    getEnvDual("SUGAR_GLIDER_SERVICE_NAME", "SIDECAR_SERVICE_NAME", "real-time-gateway"),
		RedisURL:                       firstNonEmptyEnv("SUGAR_GLIDER_REDIS_URL", "SIDECAR_REDIS_URL"),
		ListenAddr:                     getEnvDual("SUGAR_GLIDER_LISTEN_ADDR", "SIDECAR_LISTEN_ADDR", "tcp://0.0.0.0:8081"),
		GRPCListenAddr:                 getEnvDual("SUGAR_GLIDER_GRPC_ADDR", "SIDECAR_GRPC_ADDR", "tcp://0.0.0.0:50051"),
		PublishPipelineEnabled:         getEnvBoolDual("SUGAR_GLIDER_PUBLISH_PIPELINE_ENABLED", "SIDECAR_PUBLISH_PIPELINE_ENABLED", false),
		PublishPipelineAdaptiveEnabled: getEnvBoolDual("SUGAR_GLIDER_PUBLISH_PIPELINE_ADAPTIVE_ENABLED", "SIDECAR_PUBLISH_PIPELINE_ADAPTIVE_ENABLED", false),
		PublishPipelineMaxBatch:        getEnvInt64Dual("SUGAR_GLIDER_PUBLISH_PIPELINE_MAX_BATCH", "SIDECAR_PUBLISH_PIPELINE_MAX_BATCH", 64),
		PublishPipelineMinBatch:        getEnvInt64Dual("SUGAR_GLIDER_PUBLISH_PIPELINE_MIN_BATCH", "SIDECAR_PUBLISH_PIPELINE_MIN_BATCH", 2),
		PublishPipelineFlushInterval:   time.Duration(getEnvInt64Dual("SUGAR_GLIDER_PUBLISH_PIPELINE_FLUSH_MS", "SIDECAR_PUBLISH_PIPELINE_FLUSH_MS", 0)) * time.Millisecond,
		PublishPipelineQueueSize:       getEnvInt64Dual("SUGAR_GLIDER_PUBLISH_PIPELINE_QUEUE_SIZE", "SIDECAR_PUBLISH_PIPELINE_QUEUE_SIZE", 8192),
		PublishPipelineMaxBytes:        getEnvInt64Dual("SUGAR_GLIDER_PUBLISH_PIPELINE_MAX_BYTES", "SIDECAR_PUBLISH_PIPELINE_MAX_BYTES", 1048576),
		ConsumeMode:                    strings.ToLower(getEnvDual("SUGAR_GLIDER_CONSUME_MODE", "SIDECAR_CONSUME_MODE", ConsumeModeStateless)),
		WALReplayMode:                  strings.ToLower(getEnvDual("SUGAR_GLIDER_WAL_REPLAY_MODE", "SIDECAR_WAL_REPLAY_MODE", WALReplayModeBackground)),
		DispatcherConsumerName:         getEnvDual("SUGAR_GLIDER_DISPATCHER_CONSUMER_NAME", "SIDECAR_DISPATCHER_CONSUMER_NAME", "sugar-glider-dispatcher"),
		DispatcherReadCount:            getEnvInt64Dual("SUGAR_GLIDER_DISPATCHER_READ_COUNT", "SIDECAR_DISPATCHER_READ_COUNT", 100),
		DispatcherBlockMS:              getEnvInt64Dual("SUGAR_GLIDER_DISPATCHER_BLOCK_MS", "SIDECAR_DISPATCHER_BLOCK_MS", 1000),
		DispatcherSubscriberBuffer:     getEnvInt64Dual("SUGAR_GLIDER_DISPATCHER_SUBSCRIBER_BUFFER", "SIDECAR_DISPATCHER_SUBSCRIBER_BUFFER", 256),
		DispatcherAckBatchSize:         getEnvInt64Dual("SUGAR_GLIDER_DISPATCHER_ACK_BATCH_SIZE", "SIDECAR_DISPATCHER_ACK_BATCH_SIZE", 64),
		DispatcherAckFlushConcurrency:  getEnvInt64Dual("SUGAR_GLIDER_DISPATCHER_ACK_FLUSH_CONCURRENCY", "SIDECAR_DISPATCHER_ACK_FLUSH_CONCURRENCY", 2),
		DispatcherAckFlushInterval:     time.Duration(getEnvInt64Dual("SUGAR_GLIDER_DISPATCHER_ACK_FLUSH_MS", "SIDECAR_DISPATCHER_ACK_FLUSH_MS", 10)) * time.Millisecond,
		DispatcherAckQueueSize:         getEnvInt64Dual("SUGAR_GLIDER_DISPATCHER_ACK_QUEUE_SIZE", "SIDECAR_DISPATCHER_ACK_QUEUE_SIZE", 4096),
		WALDir:                         getEnvDual("SUGAR_GLIDER_WAL_DIR", "SIDECAR_WAL_DIR", "/data/synapse-wal"),
		WALMaxEntries:                  getEnvInt64Dual("SUGAR_GLIDER_WAL_MAX_ENTRIES", "SIDECAR_WAL_MAX_ENTRIES", 0),
		WALMaxBytes:                    getEnvInt64Dual("SUGAR_GLIDER_WAL_MAX_BYTES", "SIDECAR_WAL_MAX_BYTES", 0),
		WALReplayBatch:                 getEnvInt64Dual("SUGAR_GLIDER_WAL_REPLAY_BATCH", "SIDECAR_WAL_REPLAY_BATCH", 100),
		WALReplayInterval:              time.Duration(getEnvInt64Dual("SUGAR_GLIDER_WAL_REPLAY_INTERVAL_MS", "SIDECAR_WAL_REPLAY_INTERVAL_MS", 2000)) * time.Millisecond,
		PublishStreams:                 splitCSV(getEnvDual("SUGAR_GLIDER_PUBLISH_STREAMS", "SIDECAR_PUBLISH_STREAMS", "platform-events")),
		ConsumeStreams:                 splitCSV(getEnvDual("SUGAR_GLIDER_CONSUME_STREAMS", "SIDECAR_CONSUME_STREAMS", "")),
		MaxStreamLen:                   getEnvInt64Dual("SUGAR_GLIDER_MAX_STREAM_LEN", "SIDECAR_MAX_STREAM_LEN", 10000),
		DLQMaxRetries:                  getEnvInt64Dual("SUGAR_GLIDER_DLQ_MAX_RETRIES", "SIDECAR_DLQ_MAX_RETRIES", 3),
		DLQMinIdle:                     time.Duration(getEnvInt64Dual("SUGAR_GLIDER_DLQ_MIN_IDLE_MS", "SIDECAR_DLQ_MIN_IDLE_MS", 30000)) * time.Millisecond,
		DLQScanInterval:                time.Duration(getEnvInt64Dual("SUGAR_GLIDER_DLQ_SCAN_INTERVAL_MS", "SIDECAR_DLQ_SCAN_INTERVAL_MS", 5000)) * time.Millisecond,
		DLQScanBatch:                   getEnvInt64Dual("SUGAR_GLIDER_DLQ_SCAN_BATCH", "SIDECAR_DLQ_SCAN_BATCH", 100),
		ReadinessTimeout:               time.Duration(getEnvInt64Dual("SUGAR_GLIDER_READINESS_TIMEOUT_MS", "SIDECAR_READINESS_TIMEOUT_MS", 1500)) * time.Millisecond,
		ReadyMaxWALDepth:               getEnvInt64Dual("SUGAR_GLIDER_READY_MAX_WAL_DEPTH", "SIDECAR_READY_MAX_WAL_DEPTH", 0),
		ReadyMaxPublishQueueDepth:      getEnvInt64Dual("SUGAR_GLIDER_READY_MAX_PUBLISH_QUEUE_DEPTH", "SIDECAR_READY_MAX_PUBLISH_QUEUE_DEPTH", 0),
	}

	dlqPolicies, err := ParseDLQStreamPolicies(firstNonEmptyEnv("SUGAR_GLIDER_DLQ_STREAM_POLICIES", "SIDECAR_DLQ_STREAM_POLICIES"))
	if err != nil {
		return Config{}, err
	}
	cfg.DLQStreamPolicies = dlqPolicies

	if cfg.RedisURL == "" {
		return Config{}, fmt.Errorf("SUGAR_GLIDER_REDIS_URL or SIDECAR_REDIS_URL is required")
	}
	if cfg.ConsumeMode != ConsumeModeStateless && cfg.ConsumeMode != ConsumeModeDispatcher {
		return Config{}, fmt.Errorf("SUGAR_GLIDER_CONSUME_MODE/SIDECAR_CONSUME_MODE must be one of: %s, %s", ConsumeModeStateless, ConsumeModeDispatcher)
	}
	if cfg.PublishPipelineMaxBatch <= 0 {
		return Config{}, fmt.Errorf("SUGAR_GLIDER_PUBLISH_PIPELINE_MAX_BATCH must be > 0")
	}
	if cfg.PublishPipelineMinBatch <= 0 {
		return Config{}, fmt.Errorf("SUGAR_GLIDER_PUBLISH_PIPELINE_MIN_BATCH must be > 0")
	}
	if cfg.PublishPipelineFlushInterval < 0 {
		return Config{}, fmt.Errorf("SUGAR_GLIDER_PUBLISH_PIPELINE_FLUSH_MS must be >= 0")
	}
	if cfg.PublishPipelineQueueSize <= 0 {
		return Config{}, fmt.Errorf("SUGAR_GLIDER_PUBLISH_PIPELINE_QUEUE_SIZE must be > 0")
	}
	if cfg.PublishPipelineMaxBytes <= 0 {
		return Config{}, fmt.Errorf("SUGAR_GLIDER_PUBLISH_PIPELINE_MAX_BYTES must be > 0")
	}
	if cfg.WALReplayMode != WALReplayModeBackground && cfg.WALReplayMode != WALReplayModeSyncOnSuccess {
		return Config{}, fmt.Errorf("SUGAR_GLIDER_WAL_REPLAY_MODE must be one of: %s, %s", WALReplayModeBackground, WALReplayModeSyncOnSuccess)
	}
	if strings.TrimSpace(cfg.DispatcherConsumerName) == "" {
		return Config{}, fmt.Errorf("SUGAR_GLIDER_DISPATCHER_CONSUMER_NAME must be non-empty")
	}
	if cfg.DispatcherReadCount <= 0 {
		return Config{}, fmt.Errorf("SUGAR_GLIDER_DISPATCHER_READ_COUNT must be > 0")
	}
	if cfg.DispatcherBlockMS <= 0 {
		return Config{}, fmt.Errorf("SUGAR_GLIDER_DISPATCHER_BLOCK_MS must be > 0")
	}
	if cfg.DispatcherSubscriberBuffer <= 0 {
		return Config{}, fmt.Errorf("SUGAR_GLIDER_DISPATCHER_SUBSCRIBER_BUFFER must be > 0")
	}
	if cfg.DispatcherAckBatchSize <= 0 {
		return Config{}, fmt.Errorf("SUGAR_GLIDER_DISPATCHER_ACK_BATCH_SIZE must be > 0")
	}
	if cfg.DispatcherAckFlushConcurrency <= 0 {
		return Config{}, fmt.Errorf("SUGAR_GLIDER_DISPATCHER_ACK_FLUSH_CONCURRENCY must be > 0")
	}
	if cfg.DispatcherAckFlushInterval <= 0 {
		return Config{}, fmt.Errorf("SUGAR_GLIDER_DISPATCHER_ACK_FLUSH_MS must be > 0")
	}
	if cfg.DispatcherAckQueueSize <= 0 {
		return Config{}, fmt.Errorf("SUGAR_GLIDER_DISPATCHER_ACK_QUEUE_SIZE must be > 0")
	}
	if cfg.MaxStreamLen <= 0 {
		return Config{}, fmt.Errorf("SUGAR_GLIDER_MAX_STREAM_LEN must be > 0")
	}
	if cfg.WALMaxEntries < 0 {
		return Config{}, fmt.Errorf("SUGAR_GLIDER_WAL_MAX_ENTRIES must be >= 0")
	}
	if cfg.WALMaxBytes < 0 {
		return Config{}, fmt.Errorf("SUGAR_GLIDER_WAL_MAX_BYTES must be >= 0")
	}
	if cfg.WALReplayBatch < 0 {
		return Config{}, fmt.Errorf("SUGAR_GLIDER_WAL_REPLAY_BATCH must be >= 0")
	}
	if cfg.WALReplayInterval < 0 {
		return Config{}, fmt.Errorf("SUGAR_GLIDER_WAL_REPLAY_INTERVAL_MS must be >= 0")
	}
	if cfg.DLQMaxRetries < 0 {
		return Config{}, fmt.Errorf("SUGAR_GLIDER_DLQ_MAX_RETRIES must be >= 0")
	}
	if cfg.DLQMinIdle < 0 {
		return Config{}, fmt.Errorf("SUGAR_GLIDER_DLQ_MIN_IDLE_MS must be >= 0")
	}
	if cfg.DLQScanInterval < 0 {
		return Config{}, fmt.Errorf("SUGAR_GLIDER_DLQ_SCAN_INTERVAL_MS must be >= 0")
	}
	if cfg.DLQScanBatch <= 0 {
		return Config{}, fmt.Errorf("SUGAR_GLIDER_DLQ_SCAN_BATCH must be > 0")
	}
	if cfg.ReadyMaxWALDepth < 0 {
		return Config{}, fmt.Errorf("SUGAR_GLIDER_READY_MAX_WAL_DEPTH must be >= 0")
	}
	if cfg.ReadyMaxPublishQueueDepth < 0 {
		return Config{}, fmt.Errorf("SUGAR_GLIDER_READY_MAX_PUBLISH_QUEUE_DEPTH must be >= 0")
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
		return "", "", fmt.Errorf("listen addr must be formatted as <network>://<address>")
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
	return getEnvDual(key, "", fallback)
}

func getEnvInt64(key string, fallback int64) int64 {
	return getEnvInt64Dual(key, "", fallback)
}

func getEnvBool(key string, fallback bool) bool {
	return getEnvBoolDual(key, "", fallback)
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
