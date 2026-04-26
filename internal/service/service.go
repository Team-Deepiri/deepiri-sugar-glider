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
	"sync/atomic"
	"time"

	"github.com/Team-Deepiri/deepiri-sugar-glider/internal/config"
	"github.com/Team-Deepiri/deepiri-sugar-glider/internal/health"
	"github.com/Team-Deepiri/deepiri-sugar-glider/internal/redisstreams"
	"github.com/Team-Deepiri/deepiri-sugar-glider/internal/wal"
	synapsev1 "github.com/Team-Deepiri/deepiri-sugar-glider/proto/synapse/v1"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

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

	publishAttempts               uint64
	publishSuccess                uint64
	publishQueued                 uint64
	publishPipelineEnqueued       uint64
	publishPipelineFlushedBatch   uint64
	publishPipelineFlushedEntry   uint64
	publishPipelineFallback       uint64
	publishPipelineAdaptiveDirect uint64
	publishPipelineAdaptiveActive int64
	publishPipelineError          uint64
	readRequests                  uint64
	readEvents                    uint64
	ackRequests                   uint64
	ackRPCRequests                uint64
	ackedEntries                  uint64
	groupEnsureAttempts           uint64
	walReplayed                   uint64
	walReplaySyncCalls            uint64
	dlqMoved                      uint64
	dispatcherDroppedSubscribers  uint64
	errorCount                    uint64
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
	QueueLength() int
	Close()
}

func New(cfg config.Config) (*Sidecar, error) {
	client, err := redisstreams.New(cfg.RedisURL)
	if err != nil {
		return nil, fmt.Errorf("redis client: %w", err)
	}

	w, err := wal.New(cfg.WALDir)
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
			pendingEntries, err := s.redis.Raw().XPendingExt(ctx, &redis.XPendingExtArgs{
				Stream: stream,
				Group:  group.Name,
				Start:  "-",
				End:    "+",
				Count:  100,
			}).Result()
			if err != nil {
				if strings.Contains(strings.ToLower(err.Error()), "nogroup") {
					continue
				}
				s.incrementError()
				slog.Warn("dlq scan failed to read pending", "stream", stream, "group", group.Name, "error", err)
				continue
			}

			for _, pending := range pendingEntries {
				if pending.RetryCount < s.cfg.DLQMaxRetries {
					continue
				}
				if s.cfg.DLQMinIdle > 0 && pending.Idle < s.cfg.DLQMinIdle {
					continue
				}

				moved, moveErr := s.movePendingEntryToDLQ(ctx, stream, group.Name, pending)
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
						"entry_id",
						pending.ID,
						"retry_count",
						pending.RetryCount,
						"idle_ms",
						pending.Idle.Milliseconds(),
					)
				}
			}
		}
	}
}

func (s *Sidecar) movePendingEntryToDLQ(
	ctx context.Context,
	stream string,
	group string,
	pending redis.XPendingExt,
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

	dlqStream := stream + ":dlq"
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

func (s *Sidecar) metrics(w http.ResponseWriter, _ *http.Request) {
	snapshot := s.getMetricsSnapshot()
	depth, _ := s.wal.Depth()
	uptimeSeconds := time.Since(s.startedAt).Seconds()

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.WriteHeader(http.StatusOK)

	fmt.Fprintf(w, "# HELP synapse_sidecar_publish_attempts_total Total publish attempts.\n")
	fmt.Fprintf(w, "# TYPE synapse_sidecar_publish_attempts_total counter\n")
	fmt.Fprintf(w, "synapse_sidecar_publish_attempts_total %d\n", snapshot.PublishAttempts)

	fmt.Fprintf(w, "# HELP synapse_sidecar_publish_success_total Total successful publishes.\n")
	fmt.Fprintf(w, "# TYPE synapse_sidecar_publish_success_total counter\n")
	fmt.Fprintf(w, "synapse_sidecar_publish_success_total %d\n", snapshot.PublishSuccess)

	fmt.Fprintf(w, "# HELP synapse_sidecar_publish_queued_total Total publishes queued in WAL.\n")
	fmt.Fprintf(w, "# TYPE synapse_sidecar_publish_queued_total counter\n")
	fmt.Fprintf(w, "synapse_sidecar_publish_queued_total %d\n", snapshot.PublishQueued)

	fmt.Fprintf(w, "# HELP synapse_sidecar_publish_pipeline_enqueued_total Total publish requests enqueued into the publish pipeline.\n")
	fmt.Fprintf(w, "# TYPE synapse_sidecar_publish_pipeline_enqueued_total counter\n")
	fmt.Fprintf(w, "synapse_sidecar_publish_pipeline_enqueued_total %d\n", snapshot.PublishPipelineEnqueued)

	fmt.Fprintf(w, "# HELP synapse_sidecar_publish_pipeline_flushed_batches_total Total publish pipeline flush batches.\n")
	fmt.Fprintf(w, "# TYPE synapse_sidecar_publish_pipeline_flushed_batches_total counter\n")
	fmt.Fprintf(w, "synapse_sidecar_publish_pipeline_flushed_batches_total %d\n", snapshot.PublishPipelineFlushedBatch)

	fmt.Fprintf(w, "# HELP synapse_sidecar_publish_pipeline_flushed_entries_total Total publish entries flushed through pipeline.\n")
	fmt.Fprintf(w, "# TYPE synapse_sidecar_publish_pipeline_flushed_entries_total counter\n")
	fmt.Fprintf(w, "synapse_sidecar_publish_pipeline_flushed_entries_total %d\n", snapshot.PublishPipelineFlushedEntry)

	fmt.Fprintf(w, "# HELP synapse_sidecar_publish_pipeline_fallback_direct_total Total publish requests that fell back to direct publish path.\n")
	fmt.Fprintf(w, "# TYPE synapse_sidecar_publish_pipeline_fallback_direct_total counter\n")
	fmt.Fprintf(w, "synapse_sidecar_publish_pipeline_fallback_direct_total %d\n", snapshot.PublishPipelineFallback)

	fmt.Fprintf(w, "# HELP synapse_sidecar_publish_pipeline_adaptive_direct_total Total publish requests sent directly by adaptive publish routing.\n")
	fmt.Fprintf(w, "# TYPE synapse_sidecar_publish_pipeline_adaptive_direct_total counter\n")
	fmt.Fprintf(w, "synapse_sidecar_publish_pipeline_adaptive_direct_total %d\n", snapshot.PublishPipelineAdaptiveDirect)

	fmt.Fprintf(w, "# HELP synapse_sidecar_publish_pipeline_errors_total Total publish pipeline errors.\n")
	fmt.Fprintf(w, "# TYPE synapse_sidecar_publish_pipeline_errors_total counter\n")
	fmt.Fprintf(w, "synapse_sidecar_publish_pipeline_errors_total %d\n", snapshot.PublishPipelineError)

	fmt.Fprintf(w, "# HELP synapse_sidecar_publish_pipeline_queue_depth Current publish pipeline queue depth.\n")
	fmt.Fprintf(w, "# TYPE synapse_sidecar_publish_pipeline_queue_depth gauge\n")
	fmt.Fprintf(w, "synapse_sidecar_publish_pipeline_queue_depth %d\n", snapshot.PublishPipelineQueueDepth)

	fmt.Fprintf(w, "# HELP synapse_sidecar_read_requests_total Total read requests.\n")
	fmt.Fprintf(w, "# TYPE synapse_sidecar_read_requests_total counter\n")
	fmt.Fprintf(w, "synapse_sidecar_read_requests_total %d\n", snapshot.ReadRequests)

	fmt.Fprintf(w, "# HELP synapse_sidecar_read_events_total Total events returned from reads.\n")
	fmt.Fprintf(w, "# TYPE synapse_sidecar_read_events_total counter\n")
	fmt.Fprintf(w, "synapse_sidecar_read_events_total %d\n", snapshot.ReadEvents)

	fmt.Fprintf(w, "# HELP synapse_sidecar_ack_requests_total Total ack requests.\n")
	fmt.Fprintf(w, "# TYPE synapse_sidecar_ack_requests_total counter\n")
	fmt.Fprintf(w, "synapse_sidecar_ack_requests_total %d\n", snapshot.AckRequests)

	fmt.Fprintf(w, "# HELP synapse_sidecar_ack_rpc_requests_total Total gRPC ack requests.\n")
	fmt.Fprintf(w, "# TYPE synapse_sidecar_ack_rpc_requests_total counter\n")
	fmt.Fprintf(w, "synapse_sidecar_ack_rpc_requests_total %d\n", snapshot.AckRPCRequests)

	fmt.Fprintf(w, "# HELP synapse_sidecar_acked_entries_total Total acknowledged entries.\n")
	fmt.Fprintf(w, "# TYPE synapse_sidecar_acked_entries_total counter\n")
	fmt.Fprintf(w, "synapse_sidecar_acked_entries_total %d\n", snapshot.AckedEntries)

	fmt.Fprintf(w, "# HELP synapse_sidecar_group_ensure_attempts_total Total Redis consumer-group ensure attempts.\n")
	fmt.Fprintf(w, "# TYPE synapse_sidecar_group_ensure_attempts_total counter\n")
	fmt.Fprintf(w, "synapse_sidecar_group_ensure_attempts_total %d\n", snapshot.GroupEnsureAttempts)

	fmt.Fprintf(w, "# HELP synapse_sidecar_wal_replayed_total Total WAL entries replayed.\n")
	fmt.Fprintf(w, "# TYPE synapse_sidecar_wal_replayed_total counter\n")
	fmt.Fprintf(w, "synapse_sidecar_wal_replayed_total %d\n", snapshot.WALReplayed)

	fmt.Fprintf(w, "# HELP synapse_sidecar_wal_replay_sync_calls_total Total synchronous WAL replay calls from publish success path.\n")
	fmt.Fprintf(w, "# TYPE synapse_sidecar_wal_replay_sync_calls_total counter\n")
	fmt.Fprintf(w, "synapse_sidecar_wal_replay_sync_calls_total %d\n", snapshot.WALReplaySyncCalls)

	fmt.Fprintf(w, "# HELP synapse_sidecar_dlq_moved_total Total entries moved to DLQ.\n")
	fmt.Fprintf(w, "# TYPE synapse_sidecar_dlq_moved_total counter\n")
	fmt.Fprintf(w, "synapse_sidecar_dlq_moved_total %d\n", snapshot.DLQMoved)

	fmt.Fprintf(w, "# HELP synapse_sidecar_dispatcher_dropped_subscribers_total Total dispatcher subscribers dropped due to full buffers.\n")
	fmt.Fprintf(w, "# TYPE synapse_sidecar_dispatcher_dropped_subscribers_total counter\n")
	fmt.Fprintf(w, "synapse_sidecar_dispatcher_dropped_subscribers_total %d\n", snapshot.DispatcherDroppedSubscribers)

	fmt.Fprintf(w, "# HELP synapse_sidecar_errors_total Total sugar glider operation errors.\n")
	fmt.Fprintf(w, "# TYPE synapse_sidecar_errors_total counter\n")
	fmt.Fprintf(w, "synapse_sidecar_errors_total %d\n", snapshot.Errors)

	fmt.Fprintf(w, "# HELP synapse_sidecar_wal_depth Current WAL depth.\n")
	fmt.Fprintf(w, "# TYPE synapse_sidecar_wal_depth gauge\n")
	fmt.Fprintf(w, "synapse_sidecar_wal_depth %d\n", depth)

	fmt.Fprintf(w, "# HELP synapse_sidecar_uptime_seconds Sugar Glider uptime in seconds.\n")
	fmt.Fprintf(w, "# TYPE synapse_sidecar_uptime_seconds gauge\n")
	fmt.Fprintf(w, "synapse_sidecar_uptime_seconds %.0f\n", uptimeSeconds)
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
	req.Stream = strings.TrimSpace(req.Stream)
	req.EventType = strings.TrimSpace(req.EventType)
	req.Sender = strings.TrimSpace(req.Sender)
	req.Recipient = strings.TrimSpace(req.Recipient)
	req.Priority = strings.TrimSpace(req.Priority)

	if req.Stream == "" {
		if len(s.cfg.PublishStreams) == 1 {
			req.Stream = s.cfg.PublishStreams[0]
		} else {
			s.incrementError()
			return "", false, 0, http.StatusBadRequest, errors.New("stream is required")
		}
	}
	if !config.IsStreamAllowed(s.cfg.PublishStreams, req.Stream) {
		s.incrementError()
		return "", false, 0, http.StatusForbidden, errors.New("stream not allowed for this sugar glider")
	}
	if req.EventType == "" {
		s.incrementError()
		return "", false, 0, http.StatusBadRequest, errors.New("event_type is required")
	}
	if len(req.Payload) == 0 {
		s.incrementError()
		return "", false, 0, http.StatusBadRequest, errors.New("payload is required")
	}

	id, pubErr := s.publishToRedis(ctx, req)
	if pubErr != nil {
		if errors.Is(pubErr, context.Canceled) || errors.Is(pubErr, context.DeadlineExceeded) {
			s.incrementError()
			return "", false, 0, http.StatusGatewayTimeout, pubErr
		}

		slog.Warn("publish failed, writing WAL", "error", pubErr, "stream", req.Stream)
		if appendErr := s.wal.Append(pubErr.Error(), req); appendErr != nil {
			s.incrementError()
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

func (s *Sidecar) publishToRedis(ctx context.Context, req redisstreams.PublishRequest) (string, error) {
	if s.publishPipeline == nil {
		return s.publisher.Publish(ctx, req)
	}

	adaptiveActive := int64(0)
	if s.cfg.PublishPipelineAdaptiveEnabled {
		adaptiveActive = atomic.AddInt64(&s.publishPipelineAdaptiveActive, 1)
		defer atomic.AddInt64(&s.publishPipelineAdaptiveActive, -1)
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
		"wal_path":                          s.wal.Path(),
		"wal_depth":                         depth,
		"metrics":                           s.getMetricsSnapshot(),
	})
}

func (s *Sidecar) incrementPublishAttempts() {
	atomic.AddUint64(&s.publishAttempts, 1)
}

func (s *Sidecar) incrementPublishSuccess() {
	atomic.AddUint64(&s.publishSuccess, 1)
}

func (s *Sidecar) incrementPublishQueued() {
	atomic.AddUint64(&s.publishQueued, 1)
}

func (s *Sidecar) incrementPublishPipelineEnqueued() {
	atomic.AddUint64(&s.publishPipelineEnqueued, 1)
}

func (s *Sidecar) incrementPublishPipelineFlushedBatch() {
	atomic.AddUint64(&s.publishPipelineFlushedBatch, 1)
}

func (s *Sidecar) incrementPublishPipelineFlushedEntries(n uint64) {
	atomic.AddUint64(&s.publishPipelineFlushedEntry, n)
}

func (s *Sidecar) incrementPublishPipelineFallback() {
	atomic.AddUint64(&s.publishPipelineFallback, 1)
}

func (s *Sidecar) incrementPublishPipelineAdaptiveDirect() {
	atomic.AddUint64(&s.publishPipelineAdaptiveDirect, 1)
}

func (s *Sidecar) incrementPublishPipelineError(n uint64) {
	atomic.AddUint64(&s.publishPipelineError, n)
}

func (s *Sidecar) incrementReadRequest() {
	atomic.AddUint64(&s.readRequests, 1)
}

func (s *Sidecar) incrementReadEvents(n uint64) {
	atomic.AddUint64(&s.readEvents, n)
}

func (s *Sidecar) incrementAckRequest() {
	atomic.AddUint64(&s.ackRequests, 1)
}

func (s *Sidecar) incrementAckRPCRequest() {
	atomic.AddUint64(&s.ackRPCRequests, 1)
}

func (s *Sidecar) incrementAckedEntries(n uint64) {
	atomic.AddUint64(&s.ackedEntries, n)
}

func (s *Sidecar) incrementGroupEnsureAttempt() {
	atomic.AddUint64(&s.groupEnsureAttempts, 1)
}

func (s *Sidecar) incrementWALReplayed(n uint64) {
	atomic.AddUint64(&s.walReplayed, n)
}

func (s *Sidecar) incrementWALReplaySyncCall() {
	atomic.AddUint64(&s.walReplaySyncCalls, 1)
}

func (s *Sidecar) incrementDLQMoved() {
	atomic.AddUint64(&s.dlqMoved, 1)
}

func (s *Sidecar) incrementDispatcherDroppedSubscribers() {
	atomic.AddUint64(&s.dispatcherDroppedSubscribers, 1)
}

func (s *Sidecar) incrementError() {
	atomic.AddUint64(&s.errorCount, 1)
}

func (s *Sidecar) publishPipelineQueueDepth() int64 {
	if s.publishPipeline == nil {
		return 0
	}
	return int64(s.publishPipeline.QueueLength())
}

func (s *Sidecar) getMetricsSnapshot() metricsSnapshot {
	return metricsSnapshot{
		PublishAttempts:               atomic.LoadUint64(&s.publishAttempts),
		PublishSuccess:                atomic.LoadUint64(&s.publishSuccess),
		PublishQueued:                 atomic.LoadUint64(&s.publishQueued),
		PublishPipelineEnqueued:       atomic.LoadUint64(&s.publishPipelineEnqueued),
		PublishPipelineFlushedBatch:   atomic.LoadUint64(&s.publishPipelineFlushedBatch),
		PublishPipelineFlushedEntry:   atomic.LoadUint64(&s.publishPipelineFlushedEntry),
		PublishPipelineFallback:       atomic.LoadUint64(&s.publishPipelineFallback),
		PublishPipelineAdaptiveDirect: atomic.LoadUint64(&s.publishPipelineAdaptiveDirect),
		PublishPipelineError:          atomic.LoadUint64(&s.publishPipelineError),
		PublishPipelineQueueDepth:     s.publishPipelineQueueDepth(),
		ReadRequests:                  atomic.LoadUint64(&s.readRequests),
		ReadEvents:                    atomic.LoadUint64(&s.readEvents),
		AckRequests:                   atomic.LoadUint64(&s.ackRequests),
		AckRPCRequests:                atomic.LoadUint64(&s.ackRPCRequests),
		AckedEntries:                  atomic.LoadUint64(&s.ackedEntries),
		GroupEnsureAttempts:           atomic.LoadUint64(&s.groupEnsureAttempts),
		WALReplayed:                   atomic.LoadUint64(&s.walReplayed),
		WALReplaySyncCalls:            atomic.LoadUint64(&s.walReplaySyncCalls),
		DLQMoved:                      atomic.LoadUint64(&s.dlqMoved),
		DispatcherDroppedSubscribers:  atomic.LoadUint64(&s.dispatcherDroppedSubscribers),
		Errors:                        atomic.LoadUint64(&s.errorCount),
	}
}

func writeJSON(w http.ResponseWriter, status int, payload map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
