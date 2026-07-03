package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type StreamDLQPolicy struct {
	MaxRetries int64
	MinIdle    time.Duration
	DLQStream  string
}

func ParseDLQStreamPolicies(raw string) (map[string]StreamDLQPolicy, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	out := make(map[string]StreamDLQPolicy)
	entries := splitCSV(raw)
	for _, entry := range entries {
		parts := strings.Split(entry, ":")
		if len(parts) < 3 || len(parts) > 4 {
			return nil, fmt.Errorf(
				"invalid DLQ stream policy %q: expected stream:max_retries:min_idle_ms[:dlq_stream]",
				entry,
			)
		}

		stream := strings.TrimSpace(parts[0])
		if stream == "" {
			return nil, fmt.Errorf("invalid DLQ stream policy %q: stream is required", entry)
		}

		maxRetries, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
		if err != nil || maxRetries < 0 {
			return nil, fmt.Errorf("invalid DLQ stream policy %q: max_retries must be >= 0", entry)
		}

		minIdleMS, err := strconv.ParseInt(strings.TrimSpace(parts[2]), 10, 64)
		if err != nil || minIdleMS < 0 {
			return nil, fmt.Errorf("invalid DLQ stream policy %q: min_idle_ms must be >= 0", entry)
		}

		dlqStream := stream + ":dlq"
		if len(parts) == 4 {
			dlqStream = strings.TrimSpace(parts[3])
			if dlqStream == "" {
				return nil, fmt.Errorf("invalid DLQ stream policy %q: dlq_stream cannot be empty", entry)
			}
		}

		out[stream] = StreamDLQPolicy{
			MaxRetries: maxRetries,
			MinIdle:    time.Duration(minIdleMS) * time.Millisecond,
			DLQStream:  dlqStream,
		}
	}

	return out, nil
}

func (c Config) ResolveDLQPolicy(stream string) StreamDLQPolicy {
	if policy, ok := c.DLQStreamPolicies[stream]; ok {
		return policy
	}
	return StreamDLQPolicy{
		MaxRetries: c.DLQMaxRetries,
		MinIdle:    c.DLQMinIdle,
		DLQStream:  stream + ":dlq",
	}
}
