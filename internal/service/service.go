package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Team-Deepiri/deepiri-sugar-glider/internal/config"
	"github.com/Team-Deepiri/deepiri-sugar-glider/internal/health"
	"github.com/Team-Deepiri/deepiri-sugar-glider/internal/metrics"
	"github.com/Team-Deepiri/deepiri-sugar-glider/internal/redisstreams"
	"github.com/Team-Deepiri/deepiri-sugar-glider/internal/wal"
	synapsev1 "github.com/Team-Deepiri/deepiri-sugar-glider/proto/synapse/v1"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

const defaultPublishRedisTimeout = 3 * time.Second

func shouldSkipWALForPublishError(callerCtx context.Context, pubErr error) bool {
	if errors.Is(pubErr, context.Canceled) {
		return true
	}
	if callerCtx.Err() != nil && errors.Is(pubErr, context.DeadlineExceeded) {
		return true
	}
	return false
}

func (s *Sidecar) publishRedisContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), defaultPublishRedisTimeout)
}

type Sidecar struct {
	synapsev1.UnimplementedSynapseSidecarServer

	cfg                   config.Config
	redis                 *redisstreams.Client
	publisher             publishClient
	publishPipeline       publishPipelineClient
	wal                   *wal.Log
	replayMu              sync.Mutex
	consumerGroupMu       sync.RWMutex
	consumerGroupsEnsured map[string]struct{}
	startedAt             time.Time
	dispatcherManager     *consumeDispatcherManager
	collector             *metrics.Collector
}

type metricsSnapshot struct {
	PublishAttempts               uint64 `json:"publish_attempts"`
	PublishSuccess                uint64 `json:"publish_success"`
	PublishQueued                 uint64 `json:"publish_queued"`
	PublishPipelineEnqueued       uint64 `json:"publish_pipeline_enqueued"`
	PublishPipelineFlushedBatch   uint64 `json:"publish_pipeline_flushed_batches"`
	PublishPipelineFlushedEntry   uint64 `json:"publish_pipeline_flushed_entries"`
	PublishPipelineFallback       uint64 `json:"publish_pipeline_fallback_direct"`
	PublishPipelineAdaptiveDirect uint64 `json:"publish_pipeline_adaptive_direct"`
	PublishPipelineError          uint64 `json:"publish_pipeline_errors"`
	PublishPipelineQueueDepth     int64  `json:"publish_pipeline_queue_depth"`
	ReadRequests                  uint64 `json:"read_requests"`
	ReadEvents                    uint64 `json:"read_events"`
	AckRequests                   uint64 `json:"ack_requests"`
	AckRPCRequests                uint64 `json:"ack_rpc_requests"`
	AckedEntries                  uint64 `json:"acked_entries"`
	DispatcherReadSamples         uint64 `json:"dispatcher_read_samples"`
	DispatcherReadDurationMS      uint64 `json:"dispatcher_read_duration_ms_total"`
	DispatcherReadDurationMSMax   uint64 `json:"dispatcher_read_duration_ms_max"`
	DispatcherFanOutSamples       uint64 `json:"dispatcher_fanout_samples"`
	DispatcherFanOutDurationMS    uint64 `json:"dispatcher_fanout_duration_ms_total"`
	DispatcherFanOutDurationMSMax uint64 `json:"dispatcher_fanout_duration_ms_max"`
	DispatcherAckFlushCalls       uint64 `json:"dispatcher_ack_flush_calls"`
	DispatcherAckFlushChunks      uint64 `json:"dispatcher_ack_flush_chunks"`
	DispatcherAckFlushDurationMS  uint64 `json:"dispatcher_ack_flush_duration_ms_total"`
	DispatcherAckFlushDurationMax uint64 `json:"dispatcher_ack_flush_duration_ms_max"`
	DispatcherAckExecSamples      uint64 `json:"dispatcher_ack_exec_samples"`
	DispatcherAckExecDurationMS   uint64 `json:"dispatcher_ack_exec_duration_ms_total"`
	DispatcherAckExecDurationMax  uint64 `json:"dispatcher_ack_exec_duration_ms_max"`
	DispatcherAckQueueDepthPeak   uint64 `json:"dispatcher_ack_queue_depth_peak"`
	DispatcherAckInputEntries     uint64 `json:"dispatcher_ack_input_entries"`
	DispatcherAckDedupedEntries   uint64 `json:"dispatcher_ack_deduped_entries"`
	DispatcherAckDuplicateEntries uint64 `json:"dispatcher_ack_duplicate_entries"`
	DispatcherAckContiguousSpans  uint64 `json:"dispatcher_ack_contiguous_spans"`
	DispatcherAckContiguousSaved  uint64 `json:"dispatcher_ack_contiguous_saved_entries"`
	GroupEnsureAttempts           uint64 `json:"group_ensure_attempts"`
	WALReplayed                   uint64 `json:"wal_replayed"`
	WALReplaySyncCalls            uint64 `json:"wal_replay_sync_calls"`
	DLQMoved                      uint64 `json:"dlq_moved"`
	DispatcherDroppedSubscribers  uint64 `json:"dispatcher_dropped_subscribers"`
	Errors                        uint64 `json:"errors"`
}

type publishClient interface {
	Publish(ctx context.Context, req redisstreams.PublishRequest) (string, error)
}

type publishPipelineClient interface {
	Publish(ctx context.Context, req redisstreams.PublishRequest) (string, error)
	PublishMany(ctx context.Context, reqs []redisstreams.PublishRequest) ([]publishResult, error)
	QueueLength() int
	Close()
}

func New(cfg config.Config) (*Sidecar, error) {
	client, err := redisstreams.New(cfg.RedisURL)
	if err != nil {
		return nil, fmt.Errorf("redis client: %w", err)
	}

	w, err := wal.New(cfg.WALDir, wal.Options{MaxEntries: cfg.WALMaxEntries})
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("wal init: %w", err)
	}

	sidecar := &Sidecar{
		cfg:                   cfg,
		redis:                 client,
		publisher:             redisstreams.NewPublisher(client, cfg.MaxStreamLen),
		wal:                   w,
		consumerGroupsEnsured: make(map[string]struct{}),
		startedAt:             time.Now(),
		collector:             metrics.New(),
	}
	if cfg.PublishPipelineEnabled {
		sidecar.publishPipeline = newPublishPipeline(sidecar, publishPipelineConfig{
			redis:         client.Raw(),
			maxStreamLen:  cfg.MaxStreamLen,
			maxBatch:      int(cfg.PublishPipelineMaxBatch),
			maxBytes:      int(cfg.PublishPipelineMaxBytes),
			flushInterval: cfg.PublishPipelineFlushInterval,
			queueSize:     int(cfg.PublishPipelineQueueSize),
		})
	}
	if cfg.ConsumeMode == config.ConsumeModeDispatcher {
		sidecar.dispatcherManager = newConsumeDispatcherManager(sidecar)
	}
	return sidecar, nil
}

func (s *Sidecar) Close() {
	if s.publishPipeline != nil {
		s.publishPipeline.Close()
	}
	if s.dispatcherManager != nil {
		s.dispatcherManager.Close()
	}
	if s.redis != nil {
		_ = s.redis.Close()
	}
}

func (s *Sidecar) CheckReady(ctx context.Context) error {
	return s.redis.Ping(ctx)
}

func (s *Sidecar) Run(ctx context.Context) error {
	httpNetwork, httpAddress, err := config.ParseListenAddress(s.cfg.ListenAddr)
	if err != nil {
		return err
	}
	grpcNetwork, grpcAddress, err := config.ParseListenAddress(s.cfg.GRPCListenAddr)
	if err != nil {
		return err
	}

	if httpNetwork == "unix" {
		_ = os.Remove(httpAddress)
	}
	if grpcNetwork == "unix" {
		_ = os.Remove(grpcAddress)
	}

	httpLn, err := net.Listen(httpNetwork, httpAddress)
	if err != nil {
		return err
	}
	defer httpLn.Close()

	grpcLn, err := net.Listen(grpcNetwork, grpcAddress)
	if err != nil {
		return err
	}
	defer grpcLn.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.healthz)
	mux.HandleFunc("/readyz", s.readyz)
	mux.HandleFunc("/metrics", s.metrics)
	mux.HandleFunc("/v1/publish", s.publish)
	mux.HandleFunc("/v1/read", s.readFromStream)
	mux.HandleFunc("/v1/ack", s.ackEntries)
	mux.HandleFunc("/v1/config", s.currentConfig)

	httpServer := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 3 * time.Second,
	}

	grpcServer := grpc.NewServer()
	synapsev1.RegisterSynapseSidecarServer(grpcServer, s)

	errCh := make(chan error, 1)
	go func() {
		errCh <- httpServer.Serve(httpLn)
	}()
	go func() {
		errCh <- grpcServer.Serve(grpcLn)
	}()
	go s.runReplayLoop(ctx)
	go s.runDLQLoop(ctx)

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
		done := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			grpcServer.Stop()
		}
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func (s *Sidecar) runReplayLoop(ctx context.Context) {
	if s.cfg.WALReplayBatch == 0 || s.cfg.WALReplayInterval == 0 {
		return
	}

	ticker := time.NewTicker(s.cfg.WALReplayInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.replayWAL(ctx)
		}
	}
}

func (s *Sidecar) runDLQLoop(ctx context.Context) {
	if s.cfg.DLQMaxRetries == 0 || s.cfg.DLQScanInterval == 0 {
		return
	}

	ticker := time.NewTicker(s.cfg.DLQScanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.scanAndMoveToDLQ(ctx)
		}
	}
}

func (s *Sidecar) replayWAL(ctx context.Context) {
	if s.cfg.WALReplayBatch == 0 {
		return
	}
	if !s.replayMu.TryLock() {
		return
	}
	defer s.replayMu.Unlock()

	replayed, err := s.wal.Replay(ctx, s.cfg.WALReplayBatch, func(replayCtx context.Context, req redisstreams.PublishRequest) error {
		_, pubErr := s.publisher.Publish(replayCtx, req)
		return pubErr
	})
	if err != nil {
		s.incrementError()
		slog.Warn("wal replay failed", "error", err)
		return
	}
	if replayed > 0 {
		s.incrementWALReplayed(uint64(replayed))
		depth, _ := s.wal.Depth()
		slog.Info("wal replay completed", "replayed", replayed, "remaining_depth", depth)
	}
}

func (s *Sidecar) scanAndMoveToDLQ(ctx context.Context) {
	streams := s.trackedStreamsForDLQ()
	if len(streams) == 0 {
		return
	}

	for _, stream := range streams {
		policy := s.cfg.ResolveDLQPolicy(stream)
		groups, err := s.redis.Raw().XInfoGroups(ctx, stream).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) || isNoSuchStreamErr(err) {
				continue
			}
			s.incrementError()
			slog.Warn("dlq scan failed to list groups", "stream", stream, "error", err)
			continue
		}

		for _, group := range groups {
			start := "-"
			for {
				pendingEntries, err := s.redis.Raw().XPendingExt(ctx, &redis.XPendingExtArgs{
					Stream: stream,
					Group:  group.Name,
					Start:  start,
					End:    "+",
					Count:  s.cfg.DLQScanBatch,
				}).Result()
				if err != nil {
					if strings.Contains(strings.ToLower(err.Error()), "nogroup") {
						break
					}
					s.incrementError()
					slog.Warn("dlq scan failed to read pending", "stream", stream, "group", group.Name, "error", err)
					break
				}
				if len(pendingEntries) == 0 {
					break
				}

				for _, pending := range pendingEntries {
					if pending.RetryCount < policy.MaxRetries {
						continue
					}
					if policy.MinIdle > 0 && pending.Idle < policy.MinIdle {
						continue
					}

					moved, moveErr := s.movePendingEntryToDLQ(ctx, stream, group.Name, pending, policy.DLQStream)
					if moveErr != nil {
						s.incrementError()
						slog.Warn(
							"dlq move failed",
							"stream",
							stream,
							"group",
							group.Name,
							"entry_id",
							pending.ID,
							"error",
							moveErr,
						)
						continue
					}
					if moved {
						s.incrementDLQMoved()
						slog.Warn(
							"moved stream entry to dlq",
							"stream",
							stream,
							"group",
							group.Name,
							"dlq_stream",
							policy.DLQStream,
							"entry_id",
							pending.ID,
							"retry_count",
							pending.RetryCount,
							"idle_ms",
							pending.Idle.Milliseconds(),
						)
					}
				}

				if int64(len(pendingEntries)) < s.cfg.DLQScanBatch {
					break
				}
				start = "(" + pendingEntries[len(pendingEntries)-1].ID
			}
		}
	}
}

func (s *Sidecar) movePendingEntryToDLQ(
	ctx context.Context,
	stream string,
	group string,
	pending redis.XPendingExt,
	dlqStream string,
) (bool, error) {
	messages, err := s.redis.Raw().XRange(ctx, stream, pending.ID, pending.ID).Result()
	if err != nil {
		return false, err
	}
	if len(messages) == 0 {
		return false, nil
	}

	message := messages[0]
	dlqValues := map[string]any{
		"dlq_original_stream": stream,
		"dlq_original_group":  group,
		"dlq_original_id":     message.ID,
		"dlq_reason":          "max_retries_exceeded",
		"dlq_retry_count":     pending.RetryCount,
		"dlq_idle_ms":         pending.Idle.Milliseconds(),
		"dlq_moved_at":        time.Now().UTC().Format(time.RFC3339Nano),
	}
	for key, value := range message.Values {
		dlqValues[key] = value
	}

	if _, err := s.redis.Raw().XAdd(ctx, &redis.XAddArgs{
		Stream: dlqStream,
		MaxLen: s.cfg.MaxStreamLen,
		Approx: true,
		Values: dlqValues,
	}).Result(); err != nil {
		return false, err
	}

	if _, err := s.redis.Raw().XAck(ctx, stream, group, message.ID).Result(); err != nil {
		return false, err
	}

	return true, nil
}

func (s *Sidecar) trackedStreamsForDLQ() []string {
	seen := make(map[string]struct{})
	for _, stream := range s.cfg.ConsumeStreams {
		trimmed := strings.TrimSpace(stream)
		if trimmed != "" {
			seen[trimmed] = struct{}{}
		}
	}
	for _, stream := range s.cfg.PublishStreams {
		trimmed := strings.TrimSpace(stream)
		if trimmed != "" {
			seen[trimmed] = struct{}{}
		}
	}

	streams := make([]string, 0, len(seen))
	for stream := range seen {
		streams = append(streams, stream)
	}
	sort.Strings(streams)
	return streams
}

func isNoSuchStreamErr(err error) bool {
	if err == nil {
		return false
	}
	normalized := strings.ToLower(err.Error())
	return strings.Contains(normalized, "no such key")
}

func (s *Sidecar) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"healthy": true,
		"service": s.cfg.ServiceName,
		"uptime":  time.Since(s.startedAt).String(),
	})
}

func (s *Sidecar) readyz(w http.ResponseWriter, _ *http.Request) {
	err := health.CheckReady(s.cfg.ReadinessTimeout, s.redis.Ping)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"ready":        false,
			"redis_status": "down",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"ready":        true,
		"redis_status": "ok",
	})
}

func (s *Sidecar) metrics(w http.ResponseWriter, r *http.Request) {
	depth, _ := s.wal.Depth()
	s.collector.SetWALDepth(depth)
	s.collector.SetUptime(s.startedAt)
	s.collector.SetPublishPipelineQueueDepth(s.publishPipelineQueueDepth())
	s.collector.Handler().ServeHTTP(w, r)
}

func (s *Sidecar) publish(w http.ResponseWriter, r *http.Request) {
	s.incrementPublishAttempts()

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req redisstreams.PublishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.incrementError()
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid JSON"})
		return
	}

	id, queued, walDepth, statusCode, err := s.publishInternal(r.Context(), req)
	if err != nil {
		writeJSON(w, statusCode, map[string]any{"error": err.Error()})
		return
	}

	if queued {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"queued":    true,
			"wal_depth": walDepth,
			"error":     "redis unavailable; message queued in WAL",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"entry_id": id})
}

func (s *Sidecar) publishInternal(
	ctx context.Context,
	req redisstreams.PublishRequest,
) (entryID string, queued bool, walDepth int, statusCode int, err error) {
	req, code, normErr := s.normalizePublishRequest(req, "")
	if normErr != nil {
		return "", false, 0, code, normErr
	}

	pubCtx, cancel := s.publishRedisContext(ctx)
	defer cancel()

	id, pubErr := s.publishToRedis(pubCtx, req)
	if pubErr != nil {
		if shouldSkipWALForPublishError(ctx, pubErr) {
			s.incrementError()
			return "", false, 0, http.StatusGatewayTimeout, pubErr
		}

		slog.Warn("publish failed, writing WAL", "error", pubErr, "stream", req.Stream)
		if appendErr := s.wal.Append(pubErr.Error(), req); appendErr != nil {
			s.incrementError()
			if errors.Is(appendErr, wal.ErrWALFull) {
				return "", false, 0, http.StatusServiceUnavailable, errors.New("redis unavailable and wal is full")
			}
			return "", false, 0, http.StatusServiceUnavailable, errors.New("redis unavailable and wal append failed")
		}
		s.incrementPublishQueued()
		depth, _ := s.wal.Depth()
		return "", true, depth, http.StatusServiceUnavailable, nil
	}

	s.incrementPublishSuccess()
	if s.cfg.WALReplayMode == config.WALReplayModeSyncOnSuccess {
		s.incrementWALReplaySyncCall()
		s.replayWAL(ctx)
	}
	return id, false, 0, http.StatusOK, nil
}

func (s *Sidecar) normalizePublishRequest(req redisstreams.PublishRequest, defaultStream string) (redisstreams.PublishRequest, int, error) {
	req.Stream = strings.TrimSpace(req.Stream)
	req.EventType = strings.TrimSpace(req.EventType)
	req.Sender = strings.TrimSpace(req.Sender)
	req.Recipient = strings.TrimSpace(req.Recipient)
	req.Priority = strings.TrimSpace(req.Priority)

	if req.Stream == "" {
		if defaultStream != "" {
			req.Stream = defaultStream
		} else if len(s.cfg.PublishStreams) == 1 {
			req.Stream = s.cfg.PublishStreams[0]
		} else {
			s.incrementError()
			return req, http.StatusBadRequest, errors.New("stream is required")
		}
	}
	if !config.IsStreamAllowed(s.cfg.PublishStreams, req.Stream) {
		s.incrementError()
		return req, http.StatusForbidden, errors.New("stream not allowed for this sugar glider")
	}
	if req.EventType == "" {
		s.incrementError()
		return req, http.StatusBadRequest, errors.New("event_type is required")
	}
	if len(req.Payload) == 0 {
		s.incrementError()
		return req, http.StatusBadRequest, errors.New("payload is required")
	}
	return req, http.StatusOK, nil
}

func (s *Sidecar) publishBatchToRedis(ctx context.Context, reqs []redisstreams.PublishRequest) ([]string, error) {
	if len(reqs) == 0 {
		return nil, nil
	}
	if s.publishPipeline == nil || len(reqs) == 1 {
		entryIDs := make([]string, 0, len(reqs))
		for _, req := range reqs {
			id, err := s.publishToRedis(ctx, req)
			if err != nil {
				return entryIDs, err
			}
			entryIDs = append(entryIDs, id)
		}
		return entryIDs, nil
	}

	results, err := s.publishPipeline.PublishMany(ctx, reqs)
	if err != nil {
		if errors.Is(err, errPublishPipelineQueueFull) || errors.Is(err, errPublishPipelineStopped) {
			s.incrementPublishPipelineFallback()
			entryIDs := make([]string, 0, len(reqs))
			for _, req := range reqs {
				id, directErr := s.publisher.Publish(ctx, req)
				if directErr != nil {
					s.incrementPublishPipelineError(1)
					return entryIDs, directErr
				}
				entryIDs = append(entryIDs, id)
			}
			return entryIDs, nil
		}
		s.incrementPublishPipelineError(uint64(len(reqs)))
		return nil, err
	}

	entryIDs := make([]string, 0, len(results))
	for _, result := range results {
		if result.err != nil {
			s.incrementPublishPipelineError(1)
			return entryIDs, result.err
		}
		entryIDs = append(entryIDs, result.entryID)
	}
	return entryIDs, nil
}

func (s *Sidecar) publishToRedis(ctx context.Context, req redisstreams.PublishRequest) (string, error) {
	if s.publishPipeline == nil {
		return s.publisher.Publish(ctx, req)
	}

	adaptiveActive := int64(0)
	if s.cfg.PublishPipelineAdaptiveEnabled {
		adaptiveActive = s.collector.AddPublishPipelineAdaptiveActive(1)
		defer s.collector.AddPublishPipelineAdaptiveActive(-1)
	}

	if s.shouldUseAdaptiveDirectPublish(adaptiveActive) {
		s.incrementPublishPipelineAdaptiveDirect()
		return s.publisher.Publish(ctx, req)
	}

	id, err := s.publishPipeline.Publish(ctx, req)
	if err == nil {
		return id, nil
	}

	if errors.Is(err, errPublishPipelineQueueFull) || errors.Is(err, errPublishPipelineStopped) {
		s.incrementPublishPipelineFallback()
		id, directErr := s.publisher.Publish(ctx, req)
		if directErr != nil {
			s.incrementPublishPipelineError(1)
		}
		return id, directErr
	}

	s.incrementPublishPipelineError(1)
	return "", err
}

func (s *Sidecar) shouldUseAdaptiveDirectPublish(activePublishes int64) bool {
	if !s.cfg.PublishPipelineAdaptiveEnabled || s.publishPipeline == nil {
		return false
	}

	return activePublishes < s.cfg.PublishPipelineMinBatch &&
		int64(s.publishPipeline.QueueLength()+1) < s.cfg.PublishPipelineMinBatch
}

func (s *Sidecar) currentConfig(w http.ResponseWriter, _ *http.Request) {
	depth, _ := s.wal.Depth()
	activeDispatchers := 0
	if s.dispatcherManager != nil {
		activeDispatchers = s.dispatcherManager.Count()
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"service_name":                      s.cfg.ServiceName,
		"listen_addr":                       s.cfg.ListenAddr,
		"grpc_listen_addr":                  s.cfg.GRPCListenAddr,
		"publish_pipeline_enabled":          s.cfg.PublishPipelineEnabled,
		"publish_pipeline_adaptive_enabled": s.cfg.PublishPipelineAdaptiveEnabled,
		"publish_pipeline_max_batch":        s.cfg.PublishPipelineMaxBatch,
		"publish_pipeline_min_batch":        s.cfg.PublishPipelineMinBatch,
		"publish_pipeline_flush_ms":         s.cfg.PublishPipelineFlushInterval.Milliseconds(),
		"publish_pipeline_queue_size":       s.cfg.PublishPipelineQueueSize,
		"publish_pipeline_max_bytes":        s.cfg.PublishPipelineMaxBytes,
		"publish_pipeline_queue_depth":      s.publishPipelineQueueDepth(),
		"consume_mode":                      s.cfg.ConsumeMode,
		"wal_replay_mode":                   s.cfg.WALReplayMode,
		"dispatcher_consumer_name":          s.cfg.DispatcherConsumerName,
		"dispatcher_read_count":             s.cfg.DispatcherReadCount,
		"dispatcher_block_ms":               s.cfg.DispatcherBlockMS,
		"dispatcher_subscriber_buffer":      s.cfg.DispatcherSubscriberBuffer,
		"dispatcher_ack_batch_size":         s.cfg.DispatcherAckBatchSize,
		"dispatcher_ack_flush_concurrency":  s.cfg.DispatcherAckFlushConcurrency,
		"dispatcher_ack_flush_ms":           s.cfg.DispatcherAckFlushInterval.Milliseconds(),
		"dispatcher_ack_queue_size":         s.cfg.DispatcherAckQueueSize,
		"dispatcher_active":                 activeDispatchers,
		"publish_streams":                   s.cfg.PublishStreams,
		"consume_streams":                   s.cfg.ConsumeStreams,
		"max_stream_len":                    s.cfg.MaxStreamLen,
		"wal_replay_batch":                  s.cfg.WALReplayBatch,
		"wal_replay_interval_ms":            s.cfg.WALReplayInterval.Milliseconds(),
		"dlq_max_retries":                   s.cfg.DLQMaxRetries,
		"dlq_min_idle_ms":                   s.cfg.DLQMinIdle.Milliseconds(),
		"dlq_scan_interval_ms":              s.cfg.DLQScanInterval.Milliseconds(),
		"dlq_scan_batch":                    s.cfg.DLQScanBatch,
		"dlq_stream_policies":               s.cfg.DLQStreamPolicies,
		"wal_path":                          s.wal.Path(),
		"wal_max_entries":                   s.cfg.WALMaxEntries,
		"wal_depth":                         depth,
		"metrics":                           s.getMetricsSnapshot(),
	})
}

func (s *Sidecar) incrementPublishAttempts()               { s.collector.IncPublishAttempts() }
func (s *Sidecar) incrementPublishSuccess()                { s.collector.IncPublishSuccess() }
func (s *Sidecar) incrementPublishQueued()                 { s.collector.IncPublishQueued() }
func (s *Sidecar) incrementPublishPipelineEnqueued()       { s.collector.IncPublishPipelineEnqueued() }
func (s *Sidecar) incrementPublishPipelineFlushedBatch()   { s.collector.IncPublishPipelineFlushedBatch() }
func (s *Sidecar) incrementPublishPipelineFlushedEntries(n uint64) {
	s.collector.IncPublishPipelineFlushedEntries(n)
}
func (s *Sidecar) incrementPublishPipelineFallback()       { s.collector.IncPublishPipelineFallback() }
func (s *Sidecar) incrementPublishPipelineAdaptiveDirect() { s.collector.IncPublishPipelineAdaptiveDirect() }
func (s *Sidecar) incrementPublishPipelineError(n uint64)  { s.collector.IncPublishPipelineError(n) }
func (s *Sidecar) incrementReadRequest()                   { s.collector.IncReadRequest() }
func (s *Sidecar) incrementReadEvents(n uint64)            { s.collector.IncReadEvents(n) }
func (s *Sidecar) incrementAckRequest()                    { s.collector.IncAckRequest() }
func (s *Sidecar) incrementAckRPCRequest()                 { s.collector.IncAckRPCRequest() }
func (s *Sidecar) incrementAckedEntries(n uint64)          { s.collector.IncAckedEntries(n) }
func (s *Sidecar) incrementGroupEnsureAttempt()            { s.collector.IncGroupEnsureAttempt() }
func (s *Sidecar) incrementWALReplayed(n uint64)           { s.collector.IncWALReplayed(n) }
func (s *Sidecar) incrementWALReplaySyncCall()             { s.collector.IncWALReplaySyncCall() }
func (s *Sidecar) incrementDLQMoved()                      { s.collector.IncDLQMoved() }
func (s *Sidecar) incrementDispatcherDroppedSubscribers()  { s.collector.IncDispatcherDroppedSubscribers() }
func (s *Sidecar) observeDispatcherReadDuration(duration time.Duration) {
	s.collector.ObserveDispatcherReadDuration(duration)
}
func (s *Sidecar) observeDispatcherFanOutDuration(duration time.Duration) {
	s.collector.ObserveDispatcherFanOutDuration(duration)
}
func (s *Sidecar) observeDispatcherAckFlush(duration time.Duration, chunks int) {
	s.collector.ObserveDispatcherAckFlush(duration, chunks)
}
func (s *Sidecar) observeDispatcherAckExecDuration(duration time.Duration) {
	s.collector.ObserveDispatcherAckExecDuration(duration)
}
func (s *Sidecar) observeDispatcherAckQueueDepth(depth int) {
	s.collector.ObserveDispatcherAckQueueDepth(depth)
}
func (s *Sidecar) observeDispatcherAckCompression(inputEntries, dedupedEntries, contiguousSpans, contiguousSavedEntries int) {
	s.collector.ObserveDispatcherAckCompression(inputEntries, dedupedEntries, contiguousSpans, contiguousSavedEntries)
}
func (s *Sidecar) incrementError() { s.collector.IncError() }

func (s *Sidecar) publishPipelineQueueDepth() int64 {
	if s.publishPipeline == nil {
		return 0
	}
	return int64(s.publishPipeline.QueueLength())
}

func (s *Sidecar) getMetricsSnapshot() metricsSnapshot {
	snapshot := s.collector.Snapshot(s.publishPipelineQueueDepth())
	return metricsSnapshot{
		PublishAttempts:               snapshot.PublishAttempts,
		PublishSuccess:                snapshot.PublishSuccess,
		PublishQueued:                 snapshot.PublishQueued,
		PublishPipelineEnqueued:       snapshot.PublishPipelineEnqueued,
		PublishPipelineFlushedBatch:   snapshot.PublishPipelineFlushedBatch,
		PublishPipelineFlushedEntry:   snapshot.PublishPipelineFlushedEntry,
		PublishPipelineFallback:       snapshot.PublishPipelineFallback,
		PublishPipelineAdaptiveDirect: snapshot.PublishPipelineAdaptiveDirect,
		PublishPipelineError:          snapshot.PublishPipelineError,
		PublishPipelineQueueDepth:     snapshot.PublishPipelineQueueDepth,
		ReadRequests:                  snapshot.ReadRequests,
		ReadEvents:                    snapshot.ReadEvents,
		AckRequests:                   snapshot.AckRequests,
		AckRPCRequests:                snapshot.AckRPCRequests,
		AckedEntries:                  snapshot.AckedEntries,
		DispatcherReadSamples:         snapshot.DispatcherReadSamples,
		DispatcherReadDurationMS:      snapshot.DispatcherReadDurationMS,
		DispatcherReadDurationMSMax:   snapshot.DispatcherReadDurationMSMax,
		DispatcherFanOutSamples:       snapshot.DispatcherFanOutSamples,
		DispatcherFanOutDurationMS:    snapshot.DispatcherFanOutDurationMS,
		DispatcherFanOutDurationMSMax: snapshot.DispatcherFanOutDurationMSMax,
		DispatcherAckFlushCalls:       snapshot.DispatcherAckFlushCalls,
		DispatcherAckFlushChunks:      snapshot.DispatcherAckFlushChunks,
		DispatcherAckFlushDurationMS:  snapshot.DispatcherAckFlushDurationMS,
		DispatcherAckFlushDurationMax: snapshot.DispatcherAckFlushDurationMax,
		DispatcherAckExecSamples:      snapshot.DispatcherAckExecSamples,
		DispatcherAckExecDurationMS:   snapshot.DispatcherAckExecDurationMS,
		DispatcherAckExecDurationMax:  snapshot.DispatcherAckExecDurationMax,
		DispatcherAckQueueDepthPeak:   snapshot.DispatcherAckQueueDepthPeak,
		DispatcherAckInputEntries:     snapshot.DispatcherAckInputEntries,
		DispatcherAckDedupedEntries:   snapshot.DispatcherAckDedupedEntries,
		DispatcherAckDuplicateEntries: snapshot.DispatcherAckDuplicateEntries,
		DispatcherAckContiguousSpans:  snapshot.DispatcherAckContiguousSpans,
		DispatcherAckContiguousSaved:  snapshot.DispatcherAckContiguousSaved,
		GroupEnsureAttempts:           snapshot.GroupEnsureAttempts,
		WALReplayed:                   snapshot.WALReplayed,
		WALReplaySyncCalls:            snapshot.WALReplaySyncCalls,
		DLQMoved:                      snapshot.DLQMoved,
		DispatcherDroppedSubscribers:  snapshot.DispatcherDroppedSubscribers,
		Errors:                        snapshot.Errors,
	}
}

func writeJSON(w http.ResponseWriter, status int, payload map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

