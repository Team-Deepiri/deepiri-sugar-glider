package service

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Team-Deepiri/deepiri-sugar-glider/internal/redisstreams"
	"github.com/redis/go-redis/v9"
)

var (
	errPublishPipelineStopped   = errors.New("publish pipeline is stopped")
	errPublishPipelineQueueFull = errors.New("publish pipeline queue is full")
)

type publishResult struct {
	entryID string
	err     error
}

type publishJob struct {
	ctx            context.Context
	req            redisstreams.PublishRequest
	resultCh       chan publishResult
	estimatedBytes int
}

type publishPipeline struct {
	sidecar      *Sidecar
	redis        *redis.Client
	maxStreamLen int64

	maxBatch      int
	maxBytes      int
	flushInterval time.Duration

	queue chan *publishJob

	stopCh    chan struct{}
	doneCh    chan struct{}
	closeOnce sync.Once
}

func newPublishPipeline(sidecar *Sidecar, cfg publishPipelineConfig) *publishPipeline {
	pipeline := &publishPipeline{
		sidecar:       sidecar,
		redis:         cfg.redis,
		maxStreamLen:  cfg.maxStreamLen,
		maxBatch:      cfg.maxBatch,
		maxBytes:      cfg.maxBytes,
		flushInterval: cfg.flushInterval,
		queue:         make(chan *publishJob, cfg.queueSize),
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}
	go pipeline.run()
	return pipeline
}

type publishPipelineConfig struct {
	redis         *redis.Client
	maxStreamLen  int64
	maxBatch      int
	maxBytes      int
	flushInterval time.Duration
	queueSize     int
}

func (p *publishPipeline) Publish(ctx context.Context, req redisstreams.PublishRequest) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	job := &publishJob{
		ctx:            ctx,
		req:            req,
		resultCh:       make(chan publishResult, 1),
		estimatedBytes: estimatePublishRequestBytes(req),
	}

	select {
	case <-p.doneCh:
		return "", errPublishPipelineStopped
	case <-ctx.Done():
		return "", ctx.Err()
	case p.queue <- job:
		p.sidecar.incrementPublishPipelineEnqueued()
	default:
		return "", errPublishPipelineQueueFull
	}

	select {
	case result := <-job.resultCh:
		return result.entryID, result.err
	case <-ctx.Done():
		return "", ctx.Err()
	case <-p.doneCh:
		select {
		case result := <-job.resultCh:
			return result.entryID, result.err
		default:
			return "", errPublishPipelineStopped
		}
	}
}

func (p *publishPipeline) QueueLength() int {
	return len(p.queue)
}

func (p *publishPipeline) Close() {
	p.closeOnce.Do(func() {
		close(p.stopCh)
		<-p.doneCh
	})
}

func (p *publishPipeline) run() {
	defer close(p.doneCh)

	var tick <-chan time.Time
	var ticker *time.Ticker
	if p.flushInterval > 0 {
		ticker = time.NewTicker(p.flushInterval)
		defer ticker.Stop()
		tick = ticker.C
	}

	buffer := make([]*publishJob, 0, p.maxBatch)
	bufferBytes := 0

	flush := func() {
		if len(buffer) == 0 {
			return
		}

		batch := buffer
		buffer = make([]*publishJob, 0, p.maxBatch)
		bufferBytes = 0
		p.flushBatch(batch)
	}

	drain := func() {
		for {
			select {
			case job := <-p.queue:
				buffer = append(buffer, job)
				bufferBytes += job.estimatedBytes
			default:
				return
			}
		}
	}

	for {
		select {
		case <-p.stopCh:
			drain()
			flush()
			return
		case <-tick:
			flush()
		case job := <-p.queue:
			buffer = append(buffer, job)
			bufferBytes += job.estimatedBytes
			drain()
			if p.flushInterval == 0 || len(buffer) >= p.maxBatch || bufferBytes >= p.maxBytes {
				flush()
			}
		}
	}
}

func (p *publishPipeline) flushBatch(batch []*publishJob) {
	if len(batch) == 0 {
		return
	}

	p.sidecar.incrementPublishPipelineFlushedBatch()

	flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pipe := p.redis.Pipeline()
	jobs := make([]*publishJob, 0, len(batch))
	cmds := make([]*redis.StringCmd, 0, len(batch))
	for _, job := range batch {
		if err := job.ctx.Err(); err != nil {
			job.resultCh <- publishResult{err: err}
			continue
		}

		args := redisstreams.BuildXAddArgs(job.req, p.maxStreamLen)
		cmds = append(cmds, pipe.XAdd(flushCtx, args))
		jobs = append(jobs, job)
	}

	if len(jobs) == 0 {
		return
	}

	_, execErr := pipe.Exec(flushCtx)
	if execErr != nil && !errors.Is(execErr, redis.Nil) {
		p.sidecar.incrementPublishPipelineError(uint64(len(jobs)))
		for _, job := range jobs {
			job.resultCh <- publishResult{err: execErr}
		}
		return
	}

	p.sidecar.incrementPublishPipelineFlushedEntries(uint64(len(jobs)))

	for idx, job := range jobs {
		entryID, err := cmds[idx].Result()
		if err != nil {
			p.sidecar.incrementPublishPipelineError(1)
			job.resultCh <- publishResult{err: err}
			continue
		}
		job.resultCh <- publishResult{entryID: entryID}
	}
}

func estimatePublishRequestBytes(req redisstreams.PublishRequest) int {
	// Keep this lightweight. It is only used for queue flush heuristics.
	size := len(req.Stream) + len(req.EventType) + len(req.Sender) + len(req.Recipient) + len(req.Priority)
	size += len(req.Payload)
	if req.TTLSec > 0 {
		size += 8
	}
	if size < 128 {
		return 128
	}
	return size
}
