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
	if req.Priority == "" {
		req.Priority = "normal"
	}
	if req.Sender == "" {
		req.Sender = "real-time-gateway"
	}

	values := map[string]any{
		"event_type": req.EventType,
		"sender":     req.Sender,
		"recipient":  req.Recipient,
		"priority":   req.Priority,
		"payload":    []byte(req.Payload),
		"timestamp":  time.Now().UTC().Format(time.RFC3339Nano),
	}
	if req.TTLSec > 0 {
		values["ttl_sec"] = req.TTLSec
	}

	args := &redis.XAddArgs{
		Stream: req.Stream,
		MaxLen: p.maxStreamLen,
		Approx: true,
		Values: values,
	}
	return p.redis.XAdd(ctx, args).Result()
}
