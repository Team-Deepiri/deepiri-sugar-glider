package config

import (
	"testing"
	"time"
)

func TestParseDLQStreamPolicies(t *testing.T) {
	t.Parallel()

	policies, err := ParseDLQStreamPolicies("platform-events:5:30000,inference-events:10:60000:infer-dlq")
	if err != nil {
		t.Fatalf("ParseDLQStreamPolicies() error = %v", err)
	}

	platform := policies["platform-events"]
	if platform.MaxRetries != 5 {
		t.Fatalf("platform MaxRetries = %d, want 5", platform.MaxRetries)
	}
	if platform.MinIdle != 30*time.Second {
		t.Fatalf("platform MinIdle = %v, want 30s", platform.MinIdle)
	}
	if platform.DLQStream != "platform-events:dlq" {
		t.Fatalf("platform DLQStream = %q, want platform-events:dlq", platform.DLQStream)
	}

	inference := policies["inference-events"]
	if inference.DLQStream != "infer-dlq" {
		t.Fatalf("inference DLQStream = %q, want infer-dlq", inference.DLQStream)
	}
}

func TestResolveDLQPolicyFallsBackToGlobalDefaults(t *testing.T) {
	t.Parallel()

	cfg := Config{
		DLQMaxRetries: 3,
		DLQMinIdle:    45 * time.Second,
		DLQStreamPolicies: map[string]StreamDLQPolicy{
			"platform-events": {
				MaxRetries: 7,
				MinIdle:    10 * time.Second,
				DLQStream:  "platform-dlq",
			},
		},
	}

	override := cfg.ResolveDLQPolicy("platform-events")
	if override.MaxRetries != 7 || override.DLQStream != "platform-dlq" {
		t.Fatalf("override policy = %+v, want platform override", override)
	}

	defaults := cfg.ResolveDLQPolicy("training-events")
	if defaults.MaxRetries != 3 || defaults.MinIdle != 45*time.Second || defaults.DLQStream != "training-events:dlq" {
		t.Fatalf("default policy = %+v, want global defaults", defaults)
	}
}
