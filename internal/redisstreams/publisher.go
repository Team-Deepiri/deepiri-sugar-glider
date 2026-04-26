package redisstreams

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

type PublishRequest struct {
	Stream    string          `json:"stream"`
	EventType string          `json:"event_type"`
	Sender    string          `json:"sender"`
	Recipient string          `json:"recipient,omitempty"`
	Priority  string          `json:"priority,omitempty"`
	Payload   json.RawMessage `json:"payload"`
	TTLSec    int32           `json:"ttl_sec,omitempty"`
}

type Publisher struct {
	redis        *redis.Client
	maxStreamLen int64
}

func NewPublisher(client *Client, maxStreamLen int64) *Publisher {
	return &Publisher{redis: client.Raw(), maxStreamLen: maxStreamLen}
}

func (p *Publisher) Publish(ctx context.Context, req PublishRequest) (string, error) {
	args := BuildXAddArgs(req, p.maxStreamLen)
	return p.redis.XAdd(ctx, args).Result()
}

func BuildXAddArgs(req PublishRequest, maxStreamLen int64) *redis.XAddArgs {
	if req.Priority == "" {
		req.Priority = "normal"
	}
	if req.Sender == "" {
		req.Sender = "real-time-gateway"
	}

	// Pre-size XADD field/value pairs to avoid extra allocations on the hot path.
	fieldPairs := 5
	if req.Recipient != "" {
		fieldPairs++
	}
	if req.TTLSec > 0 {
		fieldPairs++
	}
	values := make([]any, 0, fieldPairs*2)
	values = append(
		values,
		"event_type", req.EventType,
		"sender", req.Sender,
		"priority", req.Priority,
		"payload", string(req.Payload),
		"timestamp", time.Now().UTC().Format(time.RFC3339Nano),
	)
	if req.Recipient != "" {
		values = append(values, "recipient", req.Recipient)
	}
	if req.TTLSec > 0 {
		values = append(values, "ttl_sec", req.TTLSec)
	}

	return &redis.XAddArgs{
		Stream: req.Stream,
		MaxLen: maxStreamLen,
		Approx: true,
		Values: values,
	}
}
