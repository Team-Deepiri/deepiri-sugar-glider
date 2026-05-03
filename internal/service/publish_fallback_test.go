package service

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/Team-Deepiri/deepiri-sugar-glider/internal/config"
	"github.com/Team-Deepiri/deepiri-sugar-glider/internal/redisstreams"
	"github.com/Team-Deepiri/deepiri-sugar-glider/internal/wal"
)

type fakePublishClient struct {
	entryID string
	err     error
	calls   int
}

func (f *fakePublishClient) Publish(_ context.Context, _ redisstreams.PublishRequest) (string, error) {
	f.calls++
	return f.entryID, f.err
}

type fakePipelineClient struct {
	entryID    string
	err        error
	queueDepth int
	calls      int
}

func (f *fakePipelineClient) Publish(_ context.Context, _ redisstreams.PublishRequest) (string, error) {
	f.calls++
	return f.entryID, f.err
}

func (f *fakePipelineClient) QueueLength() int {
	return f.queueDepth
}

func (f *fakePipelineClient) Close() {}

func TestPublishToRedis_DirectPathWhenPipelineDisabled(t *testing.T) {
	t.Parallel()

	direct := &fakePublishClient{entryID: "1-0"}
	sidecar := &Sidecar{publisher: direct}

	entryID, err := sidecar.publishToRedis(context.Background(), validPublishRequest())
	if err != nil {
		t.Fatalf("publishToRedis() error = %v", err)
	}
	if entryID != "1-0" {
		t.Fatalf("publishToRedis() entryID = %q, want %q", entryID, "1-0")
	}
	if direct.calls != 1 {
		t.Fatalf("direct Publish calls = %d, want 1", direct.calls)
	}
}

func TestPublishToRedis_AdaptiveDirectPathBelowMinBatch(t *testing.T) {
	t.Parallel()

	direct := &fakePublishClient{entryID: "direct-0"}
	pipeline := &fakePipelineClient{entryID: "pipeline-0", queueDepth: 0}
	sidecar := &Sidecar{
		cfg: config.Config{
			PublishPipelineAdaptiveEnabled: true,
			PublishPipelineMinBatch:        2,
		},
		publisher:       direct,
		publishPipeline: pipeline,
	}

	entryID, err := sidecar.publishToRedis(context.Background(), validPublishRequest())
	if err != nil {
		t.Fatalf("publishToRedis() error = %v", err)
	}
	if entryID != "direct-0" {
		t.Fatalf("publishToRedis() entryID = %q, want %q", entryID, "direct-0")
	}
	if pipeline.calls != 0 {
		t.Fatalf("pipeline Publish calls = %d, want 0", pipeline.calls)
	}
	if direct.calls != 1 {
		t.Fatalf("direct Publish calls = %d, want 1", direct.calls)
	}

	snapshot := sidecar.getMetricsSnapshot()
	if snapshot.PublishPipelineAdaptiveDirect != 1 {
		t.Fatalf("publish pipeline adaptive direct = %d, want 1", snapshot.PublishPipelineAdaptiveDirect)
	}
}

func TestPublishToRedis_AdaptiveUsesPipelineAtMinBatch(t *testing.T) {
	t.Parallel()

	direct := &fakePublishClient{entryID: "direct-0"}
	pipeline := &fakePipelineClient{entryID: "pipeline-0", queueDepth: 1}
	sidecar := &Sidecar{
		cfg: config.Config{
			PublishPipelineAdaptiveEnabled: true,
			PublishPipelineMinBatch:        2,
		},
		publisher:       direct,
		publishPipeline: pipeline,
	}

	entryID, err := sidecar.publishToRedis(context.Background(), validPublishRequest())
	if err != nil {
		t.Fatalf("publishToRedis() error = %v", err)
	}
	if entryID != "pipeline-0" {
		t.Fatalf("publishToRedis() entryID = %q, want %q", entryID, "pipeline-0")
	}
	if pipeline.calls != 1 {
		t.Fatalf("pipeline Publish calls = %d, want 1", pipeline.calls)
	}
	if direct.calls != 0 {
		t.Fatalf("direct Publish calls = %d, want 0", direct.calls)
	}

	snapshot := sidecar.getMetricsSnapshot()
	if snapshot.PublishPipelineAdaptiveDirect != 0 {
		t.Fatalf("publish pipeline adaptive direct = %d, want 0", snapshot.PublishPipelineAdaptiveDirect)
	}
}

func TestPublishToRedis_AdaptiveUsesPipelineAtActiveMinBatch(t *testing.T) {
	t.Parallel()

	direct := &fakePublishClient{entryID: "direct-0"}
	pipeline := &fakePipelineClient{entryID: "pipeline-0", queueDepth: 0}
	sidecar := &Sidecar{
		cfg: config.Config{
			PublishPipelineAdaptiveEnabled: true,
			PublishPipelineMinBatch:        2,
		},
		publisher:                     direct,
		publishPipeline:               pipeline,
		publishPipelineAdaptiveActive: 1,
	}

	entryID, err := sidecar.publishToRedis(context.Background(), validPublishRequest())
	if err != nil {
		t.Fatalf("publishToRedis() error = %v", err)
	}
	if entryID != "pipeline-0" {
		t.Fatalf("publishToRedis() entryID = %q, want %q", entryID, "pipeline-0")
	}
	if pipeline.calls != 1 {
		t.Fatalf("pipeline Publish calls = %d, want 1", pipeline.calls)
	}
	if direct.calls != 0 {
		t.Fatalf("direct Publish calls = %d, want 0", direct.calls)
	}

	snapshot := sidecar.getMetricsSnapshot()
	if snapshot.PublishPipelineAdaptiveDirect != 0 {
		t.Fatalf("publish pipeline adaptive direct = %d, want 0", snapshot.PublishPipelineAdaptiveDirect)
	}
}

func TestPublishToRedis_FallbackToDirectOnQueueFull(t *testing.T) {
	t.Parallel()

	direct := &fakePublishClient{entryID: "2-0"}
	pipeline := &fakePipelineClient{err: errPublishPipelineQueueFull}
	sidecar := &Sidecar{
		publisher:       direct,
		publishPipeline: pipeline,
	}

	entryID, err := sidecar.publishToRedis(context.Background(), validPublishRequest())
	if err != nil {
		t.Fatalf("publishToRedis() error = %v", err)
	}
	if entryID != "2-0" {
		t.Fatalf("publishToRedis() entryID = %q, want %q", entryID, "2-0")
	}
	if pipeline.calls != 1 {
		t.Fatalf("pipeline Publish calls = %d, want 1", pipeline.calls)
	}
	if direct.calls != 1 {
		t.Fatalf("direct Publish calls = %d, want 1", direct.calls)
	}

	snapshot := sidecar.getMetricsSnapshot()
	if snapshot.PublishPipelineFallback != 1 {
		t.Fatalf("publish pipeline fallback = %d, want 1", snapshot.PublishPipelineFallback)
	}
	if snapshot.PublishPipelineError != 0 {
		t.Fatalf("publish pipeline errors = %d, want 0", snapshot.PublishPipelineError)
	}
}

func TestPublishToRedis_FallbackFailureIncrementsPipelineErrors(t *testing.T) {
	t.Parallel()

	direct := &fakePublishClient{err: errors.New("direct publish failed")}
	pipeline := &fakePipelineClient{err: errPublishPipelineStopped}
	sidecar := &Sidecar{
		publisher:       direct,
		publishPipeline: pipeline,
	}

	_, err := sidecar.publishToRedis(context.Background(), validPublishRequest())
	if err == nil {
		t.Fatalf("publishToRedis() error = nil, want error")
	}
	if direct.calls != 1 {
		t.Fatalf("direct Publish calls = %d, want 1", direct.calls)
	}

	snapshot := sidecar.getMetricsSnapshot()
	if snapshot.PublishPipelineFallback != 1 {
		t.Fatalf("publish pipeline fallback = %d, want 1", snapshot.PublishPipelineFallback)
	}
	if snapshot.PublishPipelineError != 1 {
		t.Fatalf("publish pipeline errors = %d, want 1", snapshot.PublishPipelineError)
	}
}

func TestPublishToRedis_PipelineErrorSkipsDirectPublish(t *testing.T) {
	t.Parallel()

	direct := &fakePublishClient{entryID: "3-0"}
	pipeline := &fakePipelineClient{err: errors.New("pipeline flush failed")}
	sidecar := &Sidecar{
		publisher:       direct,
		publishPipeline: pipeline,
	}

	_, err := sidecar.publishToRedis(context.Background(), validPublishRequest())
	if err == nil {
		t.Fatalf("publishToRedis() error = nil, want error")
	}
	if direct.calls != 0 {
		t.Fatalf("direct Publish calls = %d, want 0", direct.calls)
	}

	snapshot := sidecar.getMetricsSnapshot()
	if snapshot.PublishPipelineError != 1 {
		t.Fatalf("publish pipeline errors = %d, want 1", snapshot.PublishPipelineError)
	}
}

func TestPublishInternal_ContextCanceledSkipsWALQueue(t *testing.T) {
	t.Parallel()

	w, err := wal.New(t.TempDir())
	if err != nil {
		t.Fatalf("wal.New() error = %v", err)
	}
	sidecar := &Sidecar{
		cfg: config.Config{
			PublishStreams: []string{"platform-events"},
			WALReplayMode:  config.WALReplayModeBackground,
		},
		publisher: &fakePublishClient{err: context.Canceled},
		wal:       w,
	}

	_, queued, walDepth, statusCode, publishErr := sidecar.publishInternal(context.Background(), validPublishRequest())
	if !errors.Is(publishErr, context.Canceled) {
		t.Fatalf("publishInternal() error = %v, want context.Canceled", publishErr)
	}
	if queued {
		t.Fatalf("publishInternal() queued = true, want false")
	}
	if walDepth != 0 {
		t.Fatalf("publishInternal() walDepth = %d, want 0", walDepth)
	}
	if statusCode != http.StatusGatewayTimeout {
		t.Fatalf("publishInternal() statusCode = %d, want %d", statusCode, http.StatusGatewayTimeout)
	}

	depth, depthErr := sidecar.wal.Depth()
	if depthErr != nil {
		t.Fatalf("wal.Depth() error = %v", depthErr)
	}
	if depth != 0 {
		t.Fatalf("wal.Depth() = %d, want 0", depth)
	}
}

func TestPublishInternal_PublishFailureQueuesToWAL(t *testing.T) {
	t.Parallel()

	w, err := wal.New(t.TempDir())
	if err != nil {
		t.Fatalf("wal.New() error = %v", err)
	}
	sidecar := &Sidecar{
		cfg: config.Config{
			PublishStreams: []string{"platform-events"},
			WALReplayMode:  config.WALReplayModeBackground,
		},
		publisher: &fakePublishClient{err: errors.New("redis down")},
		wal:       w,
	}

	_, queued, walDepth, statusCode, publishErr := sidecar.publishInternal(context.Background(), validPublishRequest())
	if publishErr != nil {
		t.Fatalf("publishInternal() error = %v, want nil", publishErr)
	}
	if !queued {
		t.Fatalf("publishInternal() queued = false, want true")
	}
	if walDepth != 1 {
		t.Fatalf("publishInternal() walDepth = %d, want 1", walDepth)
	}
	if statusCode != http.StatusServiceUnavailable {
		t.Fatalf("publishInternal() statusCode = %d, want %d", statusCode, http.StatusServiceUnavailable)
	}
}

func validPublishRequest() redisstreams.PublishRequest {
	return redisstreams.PublishRequest{
		EventType: "message.sent",
		Sender:    "realtime-gateway",
		Payload:   []byte(`{"hello":"world"}`),
	}
}
