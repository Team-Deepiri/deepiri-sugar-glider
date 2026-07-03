package metrics

import (
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
)

type Snapshot struct {
	PublishAttempts               uint64
	PublishSuccess                uint64
	PublishQueued                 uint64
	PublishPipelineEnqueued       uint64
	PublishPipelineFlushedBatch   uint64
	PublishPipelineFlushedEntry   uint64
	PublishPipelineFallback       uint64
	PublishPipelineAdaptiveDirect uint64
	PublishPipelineError          uint64
	PublishPipelineQueueDepth     int64
	ReadRequests                  uint64
	ReadEvents                    uint64
	AckRequests                   uint64
	AckRPCRequests                uint64
	AckedEntries                  uint64
	DispatcherReadSamples         uint64
	DispatcherReadDurationMS      uint64
	DispatcherReadDurationMSMax   uint64
	DispatcherFanOutSamples       uint64
	DispatcherFanOutDurationMS    uint64
	DispatcherFanOutDurationMSMax uint64
	DispatcherAckFlushCalls       uint64
	DispatcherAckFlushChunks      uint64
	DispatcherAckFlushDurationMS  uint64
	DispatcherAckFlushDurationMax uint64
	DispatcherAckExecSamples      uint64
	DispatcherAckExecDurationMS   uint64
	DispatcherAckExecDurationMax  uint64
	DispatcherAckQueueDepthPeak   uint64
	DispatcherAckInputEntries     uint64
	DispatcherAckDedupedEntries   uint64
	DispatcherAckDuplicateEntries uint64
	DispatcherAckContiguousSpans  uint64
	DispatcherAckContiguousSaved  uint64
	GroupEnsureAttempts           uint64
	WALReplayed                   uint64
	WALReplaySyncCalls            uint64
	DLQMoved                      uint64
	DispatcherDroppedSubscribers  uint64
	Errors                        uint64
}

type Collector struct {
	registry *prometheus.Registry
	handler  http.Handler

	publishAttempts               prometheus.Counter
	publishSuccess                prometheus.Counter
	publishQueued                 prometheus.Counter
	publishPipelineEnqueued       prometheus.Counter
	publishPipelineFlushedBatch   prometheus.Counter
	publishPipelineFlushedEntry   prometheus.Counter
	publishPipelineFallback       prometheus.Counter
	publishPipelineAdaptiveDirect prometheus.Counter
	publishPipelineError          prometheus.Counter
	publishPipelineQueueDepth     prometheus.Gauge
	readRequests                  prometheus.Counter
	readEvents                    prometheus.Counter
	ackRequests                   prometheus.Counter
	ackRPCRequests                prometheus.Counter
	ackedEntries                  prometheus.Counter
	groupEnsureAttempts           prometheus.Counter
	walReplayed                   prometheus.Counter
	walReplaySyncCalls            prometheus.Counter
	dlqMoved                      prometheus.Counter
	dispatcherDroppedSubscribers  prometheus.Counter
	errorsTotal                   prometheus.Counter

	dispatcherReadSamples         prometheus.Counter
	dispatcherReadDurationMS      prometheus.Counter
	dispatcherReadDurationMSMax   prometheus.Gauge
	dispatcherFanOutSamples       prometheus.Counter
	dispatcherFanOutDurationMS    prometheus.Counter
	dispatcherFanOutDurationMSMax prometheus.Gauge
	dispatcherAckFlushCalls       prometheus.Counter
	dispatcherAckFlushChunks      prometheus.Counter
	dispatcherAckFlushDurationMS prometheus.Counter
	dispatcherAckFlushDurationMax prometheus.Gauge
	dispatcherAckExecSamples      prometheus.Counter
	dispatcherAckExecDurationMS   prometheus.Counter
	dispatcherAckExecDurationMax  prometheus.Gauge
	dispatcherAckQueueDepthPeak   prometheus.Gauge
	dispatcherAckInputEntries     prometheus.Counter
	dispatcherAckDedupedEntries   prometheus.Counter
	dispatcherAckDuplicateEntries prometheus.Counter
	dispatcherAckContiguousSpans  prometheus.Counter
	dispatcherAckContiguousSaved  prometheus.Counter

	walDepth      prometheus.Gauge
	uptimeSeconds prometheus.Gauge

	publishPipelineAdaptiveActive int64
}

func New() *Collector {
	registry := prometheus.NewRegistry()
	c := &Collector{registry: registry}

	newCounter := func(name, help string) prometheus.Counter {
		counter := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "synapse_sidecar_" + name,
			Help: help,
		})
		registry.MustRegister(counter)
		return counter
	}
	newGauge := func(name, help string) prometheus.Gauge {
		gauge := prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "synapse_sidecar_" + name,
			Help: help,
		})
		registry.MustRegister(gauge)
		return gauge
	}

	c.publishAttempts = newCounter("publish_attempts_total", "Total publish attempts.")
	c.publishSuccess = newCounter("publish_success_total", "Total successful publishes.")
	c.publishQueued = newCounter("publish_queued_total", "Total publishes queued in WAL.")
	c.publishPipelineEnqueued = newCounter("publish_pipeline_enqueued_total", "Total publish requests enqueued into the publish pipeline.")
	c.publishPipelineFlushedBatch = newCounter("publish_pipeline_flushed_batches_total", "Total publish pipeline flush batches.")
	c.publishPipelineFlushedEntry = newCounter("publish_pipeline_flushed_entries_total", "Total publish entries flushed through pipeline.")
	c.publishPipelineFallback = newCounter("publish_pipeline_fallback_direct_total", "Total publish requests that fell back to direct publish path.")
	c.publishPipelineAdaptiveDirect = newCounter("publish_pipeline_adaptive_direct_total", "Total publish requests sent directly by adaptive publish routing.")
	c.publishPipelineError = newCounter("publish_pipeline_errors_total", "Total publish pipeline errors.")
	c.publishPipelineQueueDepth = newGauge("publish_pipeline_queue_depth", "Current publish pipeline queue depth.")
	c.readRequests = newCounter("read_requests_total", "Total read requests.")
	c.readEvents = newCounter("read_events_total", "Total events returned from reads.")
	c.ackRequests = newCounter("ack_requests_total", "Total ack requests.")
	c.ackRPCRequests = newCounter("ack_rpc_requests_total", "Total gRPC ack requests.")
	c.ackedEntries = newCounter("acked_entries_total", "Total acknowledged entries.")
	c.groupEnsureAttempts = newCounter("group_ensure_attempts_total", "Total Redis consumer-group ensure attempts.")
	c.walReplayed = newCounter("wal_replayed_total", "Total WAL entries replayed.")
	c.walReplaySyncCalls = newCounter("wal_replay_sync_calls_total", "Total synchronous WAL replay calls from publish success path.")
	c.dlqMoved = newCounter("dlq_moved_total", "Total entries moved to DLQ.")
	c.dispatcherDroppedSubscribers = newCounter("dispatcher_dropped_subscribers_total", "Total dispatcher subscribers dropped due to full buffers.")
	c.errorsTotal = newCounter("errors_total", "Total sugar glider operation errors.")

	c.dispatcherReadSamples = newCounter("dispatcher_read_samples_total", "Total dispatcher read samples.")
	c.dispatcherReadDurationMS = newCounter("dispatcher_read_duration_ms_total", "Total dispatcher read-loop Redis read duration in ms.")
	c.dispatcherReadDurationMSMax = newGauge("dispatcher_read_duration_ms_max", "Max dispatcher read-loop Redis read duration in ms.")
	c.dispatcherFanOutSamples = newCounter("dispatcher_fanout_samples_total", "Total dispatcher fan-out samples.")
	c.dispatcherFanOutDurationMS = newCounter("dispatcher_fanout_duration_ms_total", "Total dispatcher fan-out duration in ms.")
	c.dispatcherFanOutDurationMSMax = newGauge("dispatcher_fanout_duration_ms_max", "Max dispatcher fan-out duration in ms.")
	c.dispatcherAckFlushCalls = newCounter("dispatcher_ack_flush_calls_total", "Total dispatcher ack flush invocations.")
	c.dispatcherAckFlushChunks = newCounter("dispatcher_ack_flush_chunks_total", "Total dispatcher ack chunks processed across flush calls.")
	c.dispatcherAckFlushDurationMS = newCounter("dispatcher_ack_flush_duration_ms_total", "Total dispatcher ack flush duration in ms.")
	c.dispatcherAckFlushDurationMax = newGauge("dispatcher_ack_flush_duration_ms_max", "Max dispatcher ack flush duration in ms.")
	c.dispatcherAckExecSamples = newCounter("dispatcher_ack_exec_samples_total", "Total dispatcher Redis ack pipeline exec samples.")
	c.dispatcherAckExecDurationMS = newCounter("dispatcher_ack_exec_duration_ms_total", "Total dispatcher Redis ack pipeline exec duration in ms.")
	c.dispatcherAckExecDurationMax = newGauge("dispatcher_ack_exec_duration_ms_max", "Max dispatcher Redis ack pipeline exec duration in ms.")
	c.dispatcherAckQueueDepthPeak = newGauge("dispatcher_ack_queue_depth_peak", "Peak dispatcher ack queue depth.")
	c.dispatcherAckInputEntries = newCounter("dispatcher_ack_input_entries_total", "Total dispatcher ack entry IDs received before dedupe.")
	c.dispatcherAckDedupedEntries = newCounter("dispatcher_ack_deduped_entries_total", "Total dispatcher ack entry IDs after pending-map dedupe.")
	c.dispatcherAckDuplicateEntries = newCounter("dispatcher_ack_duplicate_entries_total", "Total dispatcher ack entry IDs removed by pending-map dedupe.")
	c.dispatcherAckContiguousSpans = newCounter("dispatcher_ack_contiguous_spans_total", "Total contiguous Redis stream ID spans observed in ack flushes.")
	c.dispatcherAckContiguousSaved = newCounter("dispatcher_ack_contiguous_saved_entries_total", "Total ack IDs that could be saved if Redis supported range-style XACK for observed spans.")

	c.walDepth = newGauge("wal_depth", "Current WAL depth.")
	c.uptimeSeconds = newGauge("uptime_seconds", "Sugar Glider uptime in seconds.")

	c.handler = promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
	return c
}

func (c *Collector) Handler() http.Handler { return c.handler }

func (c *Collector) SetWALDepth(depth int) { c.walDepth.Set(float64(depth)) }

func (c *Collector) SetUptime(startedAt time.Time) {
	c.uptimeSeconds.Set(time.Since(startedAt).Seconds())
}

func (c *Collector) SetPublishPipelineQueueDepth(depth int64) {
	c.publishPipelineQueueDepth.Set(float64(depth))
}

func (c *Collector) IncPublishAttempts()               { c.publishAttempts.Inc() }
func (c *Collector) IncPublishSuccess()                { c.publishSuccess.Inc() }
func (c *Collector) IncPublishQueued()                 { c.publishQueued.Inc() }
func (c *Collector) IncPublishPipelineEnqueued()       { c.publishPipelineEnqueued.Inc() }
func (c *Collector) IncPublishPipelineFlushedBatch()   { c.publishPipelineFlushedBatch.Inc() }
func (c *Collector) IncPublishPipelineFlushedEntries(n uint64) {
	c.publishPipelineFlushedEntry.Add(float64(n))
}
func (c *Collector) IncPublishPipelineFallback()       { c.publishPipelineFallback.Inc() }
func (c *Collector) IncPublishPipelineAdaptiveDirect() { c.publishPipelineAdaptiveDirect.Inc() }
func (c *Collector) IncPublishPipelineError(n uint64)  { c.publishPipelineError.Add(float64(n)) }
func (c *Collector) IncReadRequest()                   { c.readRequests.Inc() }
func (c *Collector) IncReadEvents(n uint64)            { c.readEvents.Add(float64(n)) }
func (c *Collector) IncAckRequest()                    { c.ackRequests.Inc() }
func (c *Collector) IncAckRPCRequest()                 { c.ackRPCRequests.Inc() }
func (c *Collector) IncAckedEntries(n uint64)          { c.ackedEntries.Add(float64(n)) }
func (c *Collector) IncGroupEnsureAttempt()            { c.groupEnsureAttempts.Inc() }
func (c *Collector) IncWALReplayed(n uint64)           { c.walReplayed.Add(float64(n)) }
func (c *Collector) IncWALReplaySyncCall()             { c.walReplaySyncCalls.Inc() }
func (c *Collector) IncDLQMoved()                      { c.dlqMoved.Inc() }
func (c *Collector) IncDispatcherDroppedSubscribers()  { c.dispatcherDroppedSubscribers.Inc() }
func (c *Collector) IncError()                         { c.errorsTotal.Inc() }

func (c *Collector) AddPublishPipelineAdaptiveActive(delta int64) int64 {
	return atomic.AddInt64(&c.publishPipelineAdaptiveActive, delta)
}

func (c *Collector) LoadPublishPipelineAdaptiveActive() int64 {
	return atomic.LoadInt64(&c.publishPipelineAdaptiveActive)
}

func (c *Collector) ObserveDispatcherReadDuration(duration time.Duration) {
	observeDurationMillis(c.dispatcherReadSamples, c.dispatcherReadDurationMS, c.dispatcherReadDurationMSMax, duration)
}

func (c *Collector) ObserveDispatcherFanOutDuration(duration time.Duration) {
	observeDurationMillis(c.dispatcherFanOutSamples, c.dispatcherFanOutDurationMS, c.dispatcherFanOutDurationMSMax, duration)
}

func (c *Collector) ObserveDispatcherAckFlush(duration time.Duration, chunks int) {
	c.dispatcherAckFlushCalls.Inc()
	if chunks > 0 {
		c.dispatcherAckFlushChunks.Add(float64(chunks))
	}
	observeDurationMillis(nil, c.dispatcherAckFlushDurationMS, c.dispatcherAckFlushDurationMax, duration)
}

func (c *Collector) ObserveDispatcherAckExecDuration(duration time.Duration) {
	observeDurationMillis(c.dispatcherAckExecSamples, c.dispatcherAckExecDurationMS, c.dispatcherAckExecDurationMax, duration)
}

func (c *Collector) ObserveDispatcherAckQueueDepth(depth int) {
	if depth <= 0 {
		return
	}
	if float64(depth) > gaugeValueGauge(c.dispatcherAckQueueDepthPeak) {
		c.dispatcherAckQueueDepthPeak.Set(float64(depth))
	}
}

func (c *Collector) ObserveDispatcherAckCompression(inputEntries, dedupedEntries, contiguousSpans, contiguousSavedEntries int) {
	if inputEntries > 0 {
		c.dispatcherAckInputEntries.Add(float64(inputEntries))
	}
	if dedupedEntries > 0 {
		c.dispatcherAckDedupedEntries.Add(float64(dedupedEntries))
	}
	if inputEntries > dedupedEntries {
		c.dispatcherAckDuplicateEntries.Add(float64(inputEntries - dedupedEntries))
	}
	if contiguousSpans > 0 {
		c.dispatcherAckContiguousSpans.Add(float64(contiguousSpans))
	}
	if contiguousSavedEntries > 0 {
		c.dispatcherAckContiguousSaved.Add(float64(contiguousSavedEntries))
	}
}

func (c *Collector) Snapshot(queueDepth int64) Snapshot {
	return Snapshot{
		PublishAttempts:               counterValue(c.publishAttempts),
		PublishSuccess:                counterValue(c.publishSuccess),
		PublishQueued:                 counterValue(c.publishQueued),
		PublishPipelineEnqueued:       counterValue(c.publishPipelineEnqueued),
		PublishPipelineFlushedBatch:   counterValue(c.publishPipelineFlushedBatch),
		PublishPipelineFlushedEntry:   counterValue(c.publishPipelineFlushedEntry),
		PublishPipelineFallback:       counterValue(c.publishPipelineFallback),
		PublishPipelineAdaptiveDirect: counterValue(c.publishPipelineAdaptiveDirect),
		PublishPipelineError:          counterValue(c.publishPipelineError),
		PublishPipelineQueueDepth:     queueDepth,
		ReadRequests:                  counterValue(c.readRequests),
		ReadEvents:                    counterValue(c.readEvents),
		AckRequests:                   counterValue(c.ackRequests),
		AckRPCRequests:                counterValue(c.ackRPCRequests),
		AckedEntries:                  counterValue(c.ackedEntries),
		DispatcherReadSamples:         counterValue(c.dispatcherReadSamples),
		DispatcherReadDurationMS:      counterValue(c.dispatcherReadDurationMS),
		DispatcherReadDurationMSMax:   gaugeValue(c.dispatcherReadDurationMSMax),
		DispatcherFanOutSamples:       counterValue(c.dispatcherFanOutSamples),
		DispatcherFanOutDurationMS:    counterValue(c.dispatcherFanOutDurationMS),
		DispatcherFanOutDurationMSMax: gaugeValue(c.dispatcherFanOutDurationMSMax),
		DispatcherAckFlushCalls:       counterValue(c.dispatcherAckFlushCalls),
		DispatcherAckFlushChunks:      counterValue(c.dispatcherAckFlushChunks),
		DispatcherAckFlushDurationMS:  counterValue(c.dispatcherAckFlushDurationMS),
		DispatcherAckFlushDurationMax: gaugeValue(c.dispatcherAckFlushDurationMax),
		DispatcherAckExecSamples:      counterValue(c.dispatcherAckExecSamples),
		DispatcherAckExecDurationMS:   counterValue(c.dispatcherAckExecDurationMS),
		DispatcherAckExecDurationMax:  gaugeValue(c.dispatcherAckExecDurationMax),
		DispatcherAckQueueDepthPeak:   gaugeValue(c.dispatcherAckQueueDepthPeak),
		DispatcherAckInputEntries:     counterValue(c.dispatcherAckInputEntries),
		DispatcherAckDedupedEntries:   counterValue(c.dispatcherAckDedupedEntries),
		DispatcherAckDuplicateEntries: counterValue(c.dispatcherAckDuplicateEntries),
		DispatcherAckContiguousSpans:  counterValue(c.dispatcherAckContiguousSpans),
		DispatcherAckContiguousSaved:  counterValue(c.dispatcherAckContiguousSaved),
		GroupEnsureAttempts:           counterValue(c.groupEnsureAttempts),
		WALReplayed:                   counterValue(c.walReplayed),
		WALReplaySyncCalls:            counterValue(c.walReplaySyncCalls),
		DLQMoved:                      counterValue(c.dlqMoved),
		DispatcherDroppedSubscribers:  counterValue(c.dispatcherDroppedSubscribers),
		Errors:                        counterValue(c.errorsTotal),
	}
}

func observeDurationMillis(samples prometheus.Counter, totalMS prometheus.Counter, maxGauge prometheus.Gauge, duration time.Duration) {
	if duration <= 0 {
		return
	}
	ms := float64(duration.Milliseconds())
	if samples != nil {
		samples.Inc()
	}
	totalMS.Add(ms)
	atomicMaxGauge(maxGauge, ms)
}

func atomicMaxGauge(gauge prometheus.Gauge, candidate float64) bool {
	// Gauges don't expose CAS; read current via dto and set if larger.
	current := gaugeValueGauge(gauge)
	if candidate <= current {
		return true
	}
	gauge.Set(candidate)
	return gaugeValueGauge(gauge) >= candidate
}

func counterValue(counter prometheus.Counter) uint64 {
	metric := &dto.Metric{}
	if err := counter.Write(metric); err != nil {
		return 0
	}
	return uint64(metric.GetCounter().GetValue())
}

func gaugeValue(gauge prometheus.Gauge) uint64 {
	return uint64(gaugeValueGauge(gauge))
}

func gaugeValueGauge(gauge prometheus.Gauge) float64 {
	metric := &dto.Metric{}
	if err := gauge.Write(metric); err != nil {
		return 0
	}
	return metric.GetGauge().GetValue()
}
