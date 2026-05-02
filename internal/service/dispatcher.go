package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Team-Deepiri/deepiri-sugar-glider/internal/config"
	synapsev1 "github.com/Team-Deepiri/deepiri-sugar-glider/proto/synapse/v1"
	"github.com/redis/go-redis/v9"
)

var (
	errDispatcherNotFound   = errors.New("dispatcher not found for stream/group")
	errDispatcherClosed     = errors.New("dispatcher is closed")
	errDispatcherAckBacklog = errors.New("dispatcher ack queue is full")
)

type consumeDispatcherManager struct {
	sidecar     *Sidecar
	mu          sync.Mutex
	dispatchers map[string]*streamDispatcher
	closed      bool
}

func newConsumeDispatcherManager(sidecar *Sidecar) *consumeDispatcherManager {
	return &consumeDispatcherManager{
		sidecar:     sidecar,
		dispatchers: make(map[string]*streamDispatcher),
	}
}

func (m *consumeDispatcherManager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.dispatchers)
}

func (m *consumeDispatcherManager) Close() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	dispatchers := make([]*streamDispatcher, 0, len(m.dispatchers))
	for _, dispatcher := range m.dispatchers {
		dispatchers = append(dispatchers, dispatcher)
	}
	m.dispatchers = make(map[string]*streamDispatcher)
	m.mu.Unlock()

	for _, dispatcher := range dispatchers {
		dispatcher.stop()
	}
}

func (m *consumeDispatcherManager) Subscribe(req readRequest) (*dispatcherSubscription, error) {
	req.Stream = strings.TrimSpace(req.Stream)
	req.ConsumerGroup = strings.TrimSpace(req.ConsumerGroup)
	if req.Stream == "" || req.ConsumerGroup == "" {
		return nil, errors.New("stream and consumer_group are required")
	}
	if !config.IsStreamAllowed(m.sidecar.cfg.ConsumeStreams, req.Stream) {
		return nil, errors.New("stream not allowed for this sugar glider")
	}

	readCount := req.Count
	if readCount <= 0 {
		readCount = m.sidecar.cfg.DispatcherReadCount
	}
	blockMS := req.BlockMS
	if blockMS <= 0 {
		blockMS = m.sidecar.cfg.DispatcherBlockMS
	}

	dispatcher, err := m.getOrCreate(req.Stream, req.ConsumerGroup, readCount, blockMS)
	if err != nil {
		return nil, err
	}
	return dispatcher.subscribe(), nil
}

func (m *consumeDispatcherManager) QueueAck(req ackRequest) (int, error) {
	req.Stream = strings.TrimSpace(req.Stream)
	req.ConsumerGroup = strings.TrimSpace(req.ConsumerGroup)
	if req.Stream == "" || req.ConsumerGroup == "" || len(req.EntryIDs) == 0 {
		return 0, errors.New("stream, consumer_group, and entry_ids are required")
	}
	if !config.IsStreamAllowed(m.sidecar.cfg.ConsumeStreams, req.Stream) {
		return 0, errors.New("stream not allowed for this sugar glider")
	}

	key := dispatcherKey(req.Stream, req.ConsumerGroup)
	m.mu.Lock()
	dispatcher := m.dispatchers[key]
	m.mu.Unlock()
	if dispatcher == nil {
		return 0, errDispatcherNotFound
	}

	return dispatcher.queueAck(req.EntryIDs)
}

func (m *consumeDispatcherManager) getOrCreate(
	streamName string,
	consumerGroup string,
	readCount int64,
	blockMS int64,
) (*streamDispatcher, error) {
	key := dispatcherKey(streamName, consumerGroup)

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, errDispatcherClosed
	}
	if existing := m.dispatchers[key]; existing != nil {
		return existing, nil
	}

	dispatcher := newStreamDispatcher(m, streamName, consumerGroup, readCount, blockMS)
	m.dispatchers[key] = dispatcher
	dispatcher.start()
	return dispatcher, nil
}

func (m *consumeDispatcherManager) removeIfSame(key string, dispatcher *streamDispatcher) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if current := m.dispatchers[key]; current == dispatcher {
		delete(m.dispatchers, key)
	}
}

type streamDispatcher struct {
	manager       *consumeDispatcherManager
	sidecar       *Sidecar
	key           string
	streamName    string
	consumerGroup string
	consumerName  string
	readCount     int64
	blockDuration time.Duration

	subscriberBuffer    int
	ackBatchSize        int
	ackFlushConcurrency int
	ackFlushInterval    time.Duration
	ackQueue            chan []string

	ctx       context.Context
	cancel    context.CancelFunc
	startOnce sync.Once
	stopOnce  sync.Once

	subscribersMu sync.RWMutex
	subscribers   map[uint64]chan *synapsev1.Event
	nextSubID     uint64
}

func newStreamDispatcher(
	manager *consumeDispatcherManager,
	streamName string,
	consumerGroup string,
	readCount int64,
	blockMS int64,
) *streamDispatcher {
	ctx, cancel := context.WithCancel(context.Background())
	cfg := manager.sidecar.cfg

	return &streamDispatcher{
		manager:             manager,
		sidecar:             manager.sidecar,
		key:                 dispatcherKey(streamName, consumerGroup),
		streamName:          streamName,
		consumerGroup:       consumerGroup,
		consumerName:        cfg.DispatcherConsumerName,
		readCount:           readCount,
		blockDuration:       time.Duration(blockMS) * time.Millisecond,
		subscriberBuffer:    int(cfg.DispatcherSubscriberBuffer),
		ackBatchSize:        int(cfg.DispatcherAckBatchSize),
		ackFlushConcurrency: int(cfg.DispatcherAckFlushConcurrency),
		ackFlushInterval:    cfg.DispatcherAckFlushInterval,
		ackQueue:            make(chan []string, cfg.DispatcherAckQueueSize),
		ctx:                 ctx,
		cancel:              cancel,
		subscribers:         make(map[uint64]chan *synapsev1.Event),
	}
}

func (d *streamDispatcher) start() {
	d.startOnce.Do(func() {
		go d.runReadLoop()
		go d.runAckLoop()
	})
}

func (d *streamDispatcher) stop() {
	d.stopOnce.Do(func() {
		d.cancel()
		d.subscribersMu.Lock()
		for id, ch := range d.subscribers {
			close(ch)
			delete(d.subscribers, id)
		}
		d.subscribersMu.Unlock()
	})
}

func (d *streamDispatcher) subscribe() *dispatcherSubscription {
	subID := atomic.AddUint64(&d.nextSubID, 1)
	ch := make(chan *synapsev1.Event, d.subscriberBuffer)

	d.subscribersMu.Lock()
	d.subscribers[subID] = ch
	d.subscribersMu.Unlock()

	return &dispatcherSubscription{
		dispatcher: d,
		id:         subID,
		events:     ch,
	}
}

func (d *streamDispatcher) removeSubscriber(subID uint64) {
	d.subscribersMu.Lock()
	ch, ok := d.subscribers[subID]
	if ok {
		delete(d.subscribers, subID)
		close(ch)
	}
	remaining := len(d.subscribers)
	d.subscribersMu.Unlock()

	if remaining == 0 {
		d.manager.removeIfSame(d.key, d)
		d.stop()
	}
}

func (d *streamDispatcher) subscriberCount() int {
	d.subscribersMu.RLock()
	defer d.subscribersMu.RUnlock()
	return len(d.subscribers)
}

func (d *streamDispatcher) queueAck(entryIDs []string) (int, error) {
	cleaned := make([]string, 0, len(entryIDs))
	for _, entryID := range entryIDs {
		trimmed := strings.TrimSpace(entryID)
		if trimmed != "" {
			cleaned = append(cleaned, trimmed)
		}
	}
	if len(cleaned) == 0 {
		return 0, errors.New("entry_ids are required")
	}

	select {
	case <-d.ctx.Done():
		return 0, errDispatcherClosed
	case d.ackQueue <- cleaned:
		d.sidecar.observeDispatcherAckQueueDepth(len(d.ackQueue))
		return len(cleaned), nil
	default:
		return 0, errDispatcherAckBacklog
	}
}

func (d *streamDispatcher) runReadLoop() {
	for {
		if d.ctx.Err() != nil {
			return
		}
		if d.subscriberCount() == 0 {
			select {
			case <-d.ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}

		if err := d.sidecar.ensureConsumerGroup(d.ctx, d.streamName, d.consumerGroup); err != nil {
			d.sidecar.incrementError()
			slog.Warn("dispatcher failed to ensure consumer group", "stream", d.streamName, "group", d.consumerGroup, "error", err)
			select {
			case <-d.ctx.Done():
				return
			case <-time.After(250 * time.Millisecond):
			}
			continue
		}

		readStart := time.Now()
		streams, err := d.sidecar.redis.Raw().XReadGroup(d.ctx, &redis.XReadGroupArgs{
			Group:    d.consumerGroup,
			Consumer: d.consumerName,
			Streams:  []string{d.streamName, ">"},
			Count:    d.readCount,
			Block:    d.blockDuration,
		}).Result()
		d.sidecar.observeDispatcherReadDuration(time.Since(readStart))
		if err != nil {
			if err == redis.Nil {
				continue
			}
			if d.ctx.Err() != nil {
				return
			}
			d.sidecar.incrementError()
			if isNoGroupError(err) {
				d.sidecar.forgetEnsuredConsumerGroup(d.streamName, d.consumerGroup)
				slog.Warn("dispatcher consumer group missing; retrying group creation", "stream", d.streamName, "group", d.consumerGroup)
			} else {
				slog.Warn("dispatcher read failed", "stream", d.streamName, "group", d.consumerGroup, "error", err)
			}
			select {
			case <-d.ctx.Done():
				return
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}

		events := make([]*synapsev1.Event, 0)
		for _, stream := range streams {
			for _, msg := range stream.Messages {
				events = append(events, redisMessageToProtoEvent(stream.Stream, msg))
			}
		}
		if len(events) == 0 {
			continue
		}

		d.sidecar.incrementReadEvents(uint64(len(events)))
		fanOutStart := time.Now()
		d.fanOut(events)
		d.sidecar.observeDispatcherFanOutDuration(time.Since(fanOutStart))
	}
}

func (d *streamDispatcher) runAckLoop() {
	pending := make(map[string]struct{})
	pendingInputEntries := 0
	ticker := time.NewTicker(d.ackFlushInterval)
	defer ticker.Stop()

	addPending := func(entryIDs []string) {
		pendingInputEntries += len(entryIDs)
		for _, entryID := range entryIDs {
			pending[entryID] = struct{}{}
		}
	}

	drainQueue := func() {
		for {
			select {
			case more := <-d.ackQueue:
				addPending(more)
			default:
				return
			}
		}
	}

	flush := func() {
		if len(pending) == 0 {
			return
		}
		flushStart := time.Now()
		chunkCount := 0

		entryIDs := make([]string, 0, len(pending))
		for entryID := range pending {
			entryIDs = append(entryIDs, entryID)
		}
		sort.Strings(entryIDs)

		chunks := chunkStringSlice(entryIDs, d.ackBatchSize)
		chunkCount = len(chunks)
		contiguousSpans, contiguousSavedEntries := countContiguousAckSpans(entryIDs)
		batchChunks := d.ackFlushConcurrency
		if batchChunks <= 0 {
			batchChunks = 1
		}
		for i := 0; i < len(chunks); i += batchChunks {
			end := i + batchChunks
			if end > len(chunks) {
				end = len(chunks)
			}

			if err := d.flushAckChunkBatch(chunks[i:end], pending); err != nil {
				d.sidecar.incrementError()
				slog.Warn(
					"dispatcher ack flush failed",
					"stream",
					d.streamName,
					"group",
					d.consumerGroup,
					"chunks",
					end-i,
					"error",
					err,
				)
				return
			}
		}
		d.sidecar.observeDispatcherAckCompression(
			pendingInputEntries,
			len(entryIDs),
			contiguousSpans,
			contiguousSavedEntries,
		)
		pendingInputEntries = len(pending)
		d.sidecar.observeDispatcherAckFlush(time.Since(flushStart), chunkCount)
	}

	for {
		select {
		case <-d.ctx.Done():
			drainQueue()
			flush()
			return
		case entryIDs := <-d.ackQueue:
			addPending(entryIDs)
			drainQueue()
			if len(pending) >= d.ackBatchSize {
				flush()
			}
		case <-ticker.C:
			drainQueue()
			flush()
		}
	}
}

func (d *streamDispatcher) fanOut(events []*synapsev1.Event) {
	dropped := make(map[uint64]struct{})
	d.subscribersMu.RLock()
	for _, event := range events {
		for subID, ch := range d.subscribers {
			if _, alreadyDropped := dropped[subID]; alreadyDropped {
				continue
			}
			select {
			case ch <- event:
			default:
				dropped[subID] = struct{}{}
			}
		}
	}
	d.subscribersMu.RUnlock()

	for subID := range dropped {
		slog.Warn(
			"dispatcher subscriber buffer full; removing subscriber",
			"stream",
			d.streamName,
			"group",
			d.consumerGroup,
			"subscriber_id",
			subID,
		)
		d.sidecar.incrementError()
		d.sidecar.incrementDispatcherDroppedSubscribers()
		d.removeSubscriber(subID)
	}
}

func (d *streamDispatcher) flushAckChunkBatch(chunks [][]string, pending map[string]struct{}) error {
	if len(chunks) == 0 {
		return nil
	}
	execStart := time.Now()
	defer func() {
		d.sidecar.observeDispatcherAckExecDuration(time.Since(execStart))
	}()

	flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pipe := d.sidecar.redis.Raw().Pipeline()
	cmds := make([]*redis.IntCmd, len(chunks))
	for i, chunk := range chunks {
		cmds[i] = pipe.XAck(flushCtx, d.streamName, d.consumerGroup, chunk...)
	}

	if _, err := pipe.Exec(flushCtx); err != nil && !errors.Is(err, redis.Nil) {
		return err
	}

	for i, cmd := range cmds {
		acked, err := cmd.Result()
		if err != nil {
			d.sidecar.incrementError()
			slog.Warn(
				"dispatcher ack command failed",
				"stream",
				d.streamName,
				"group",
				d.consumerGroup,
				"entries",
				len(chunks[i]),
				"error",
				err,
			)
			continue
		}
		if acked > 0 {
			d.sidecar.incrementAckedEntries(uint64(acked))
		}
		for _, entryID := range chunks[i] {
			delete(pending, entryID)
		}
	}

	return nil
}

type streamIDParts struct {
	ms  uint64
	seq uint64
}

func countContiguousAckSpans(entryIDs []string) (int, int) {
	if len(entryIDs) == 0 {
		return 0, 0
	}

	parts := make([]streamIDParts, 0, len(entryIDs))
	for _, entryID := range entryIDs {
		parsed, ok := parseStreamIDParts(entryID)
		if ok {
			parts = append(parts, parsed)
		}
	}
	if len(parts) == 0 {
		return 0, 0
	}

	sort.Slice(parts, func(i, j int) bool {
		if parts[i].ms != parts[j].ms {
			return parts[i].ms < parts[j].ms
		}
		return parts[i].seq < parts[j].seq
	})

	spans := 1
	currentSpanLength := 1
	savedEntries := 0
	for i := 1; i < len(parts); i++ {
		previous := parts[i-1]
		current := parts[i]
		if current.ms == previous.ms && current.seq == previous.seq+1 {
			currentSpanLength++
			continue
		}
		if currentSpanLength > 1 {
			savedEntries += currentSpanLength - 1
		}
		spans++
		currentSpanLength = 1
	}
	if currentSpanLength > 1 {
		savedEntries += currentSpanLength - 1
	}

	return spans, savedEntries
}

func parseStreamIDParts(entryID string) (streamIDParts, bool) {
	msRaw, seqRaw, ok := strings.Cut(entryID, "-")
	if !ok {
		return streamIDParts{}, false
	}
	ms, err := strconv.ParseUint(msRaw, 10, 64)
	if err != nil {
		return streamIDParts{}, false
	}
	seq, err := strconv.ParseUint(seqRaw, 10, 64)
	if err != nil {
		return streamIDParts{}, false
	}
	return streamIDParts{ms: ms, seq: seq}, true
}

type dispatcherSubscription struct {
	dispatcher *streamDispatcher
	id         uint64
	events     <-chan *synapsev1.Event
}

func (s *dispatcherSubscription) Events() <-chan *synapsev1.Event {
	return s.events
}

func (s *dispatcherSubscription) Close() {
	if s == nil || s.dispatcher == nil {
		return
	}
	s.dispatcher.removeSubscriber(s.id)
}

func dispatcherKey(streamName string, consumerGroup string) string {
	return fmt.Sprintf("%s|%s", streamName, consumerGroup)
}

func chunkStringSlice(items []string, chunkSize int) [][]string {
	safeChunkSize := chunkSize
	if safeChunkSize <= 0 {
		safeChunkSize = 1
	}

	out := make([][]string, 0, (len(items)+safeChunkSize-1)/safeChunkSize)
	for i := 0; i < len(items); i += safeChunkSize {
		end := i + safeChunkSize
		if end > len(items) {
			end = len(items)
		}
		out = append(out, items[i:end])
	}
	return out
}

func isNoGroupError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "nogroup")
}

func redisMessageToProtoEvent(streamName string, message redis.XMessage) *synapsev1.Event {
	fields := message.Values
	payloadValue := normalizeRedisValue(fields["payload"])
	var payload []byte
	switch v := payloadValue.(type) {
	case []byte:
		payload = v
	case string:
		payload = []byte(v)
	default:
		payload = []byte(toString(v))
	}

	timestamp := toString(normalizeRedisValue(fields["timestamp"]))
	if timestamp == "" {
		timestamp = time.Now().UTC().Format(time.RFC3339Nano)
	}

	return &synapsev1.Event{
		Stream:    streamName,
		EntryId:   message.ID,
		EventType: toString(normalizeRedisValue(fields["event_type"])),
		Sender:    toString(normalizeRedisValue(fields["sender"])),
		Payload:   payload,
		Timestamp: timestamp,
	}
}
