package redisstreams

import (
	"encoding/json"
	"testing"
	"time"
)

func TestBuildXAddArgs_DefaultsAndShape(t *testing.T) {
	req := PublishRequest{
		Stream:    "platform-events",
		EventType: "message.sent",
		Payload:   json.RawMessage(`{"hello":"world"}`),
	}

	args := BuildXAddArgs(req, 10000)
	if args.Stream != "platform-events" {
		t.Fatalf("Stream = %q, want %q", args.Stream, "platform-events")
	}
	if args.MaxLen != 10000 {
		t.Fatalf("MaxLen = %d, want %d", args.MaxLen, 10000)
	}
	if !args.Approx {
		t.Fatalf("Approx = false, want true")
	}

	fields := valuesToMap(t, args.Values)
	if fields["event_type"] != "message.sent" {
		t.Fatalf("event_type = %v, want %q", fields["event_type"], "message.sent")
	}
	if fields["sender"] != "real-time-gateway" {
		t.Fatalf("sender = %v, want %q", fields["sender"], "real-time-gateway")
	}
	if fields["priority"] != "normal" {
		t.Fatalf("priority = %v, want %q", fields["priority"], "normal")
	}

	payload, ok := fields["payload"].(string)
	if !ok {
		t.Fatalf("payload type = %T, want string", fields["payload"])
	}
	if payload != `{"hello":"world"}` {
		t.Fatalf("payload = %q, want %q", payload, `{"hello":"world"}`)
	}

	ts, ok := fields["timestamp"].(string)
	if !ok {
		t.Fatalf("timestamp type = %T, want string", fields["timestamp"])
	}
	if _, err := time.Parse(time.RFC3339Nano, ts); err != nil {
		t.Fatalf("timestamp parse error = %v", err)
	}
}

func TestBuildXAddArgs_OptionalFields(t *testing.T) {
	req := PublishRequest{
		Stream:    "platform-events",
		EventType: "message.sent",
		Sender:    "realtime-gateway",
		Priority:  "high",
		Recipient: "user-123",
		Payload:   json.RawMessage(`{"x":1}`),
		TTLSec:    120,
	}

	args := BuildXAddArgs(req, 5000)
	fields := valuesToMap(t, args.Values)

	if fields["recipient"] != "user-123" {
		t.Fatalf("recipient = %v, want %q", fields["recipient"], "user-123")
	}
	if fields["ttl_sec"] != int32(120) {
		t.Fatalf("ttl_sec = %v, want %d", fields["ttl_sec"], int32(120))
	}
}

func valuesToMap(t *testing.T, valuesArg any) map[string]any {
	t.Helper()

	values, ok := valuesArg.([]any)
	if !ok {
		t.Fatalf("values type = %T, want []any", valuesArg)
	}
	if len(values)%2 != 0 {
		t.Fatalf("values length = %d, want even", len(values))
	}

	out := make(map[string]any, len(values)/2)
	for i := 0; i < len(values); i += 2 {
		key, ok := values[i].(string)
		if !ok {
			t.Fatalf("key[%d] type = %T, want string", i, values[i])
		}
		out[key] = values[i+1]
	}
	return out
}
