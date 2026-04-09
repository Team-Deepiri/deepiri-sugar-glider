package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	synapsev1 "github.com/Team-Deepiri/deepiri-platform/platform-services/backend/deepiri-realtime-gateway/synapse-sidecar/proto/synapse/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type options struct {
	addr      string
	stream    string
	group     string
	consumer  string
	attempts  int
	batchSize int
}

func parseFlags() options {
	now := time.Now().Unix()
	opts := options{}
	flag.StringVar(&opts.addr, "addr", "localhost:50051", "sugar glider gRPC address")
	flag.StringVar(&opts.stream, "stream", "platform-events", "stream to publish/read")
	flag.StringVar(&opts.group, "group", "sugar-glider-grpc-smoke", "consumer group")
	flag.StringVar(&opts.consumer, "consumer", fmt.Sprintf("grpc-smoke-%d", now), "consumer name")
	flag.IntVar(&opts.attempts, "attempts", 12, "max subscribe attempts")
	flag.IntVar(&opts.batchSize, "batch-size", 25, "subscribe batch size")
	flag.Parse()

	if strings.TrimSpace(opts.consumer) == "" {
		opts.consumer = fmt.Sprintf("grpc-smoke-%d", now)
	}
	return opts
}

func main() {
	opts := parseFlags()

	if opts.attempts <= 0 {
		exitf("attempts must be > 0")
	}
	if opts.batchSize <= 0 {
		exitf("batch-size must be > 0")
	}

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer dialCancel()

	conn, err := grpc.DialContext(
		dialCtx,
		opts.addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		exitf("dial %s failed: %v", opts.addr, err)
	}
	defer conn.Close()

	client := synapsev1.NewSynapseSidecarClient(conn)

	healthCtx, healthCancel := context.WithTimeout(context.Background(), 5*time.Second)
	health, err := client.Health(healthCtx, &synapsev1.HealthRequest{})
	healthCancel()
	if err != nil {
		exitf("health RPC failed: %v", err)
	}
	if !health.GetHealthy() {
		exitf("sugar glider reported unhealthy redis_status=%s", health.GetRedisStatus())
	}

	smokeID := fmt.Sprintf("grpc-smoke-%d", time.Now().UnixNano())
	payload, err := json.Marshal(map[string]any{
		"smoke_id":  smokeID,
		"source":    "sugar-glider-grpc-smoke",
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		exitf("payload marshal failed: %v", err)
	}

	pubCtx, pubCancel := context.WithTimeout(context.Background(), 5*time.Second)
	pubResp, err := client.Publish(pubCtx, &synapsev1.PublishRequest{
		Stream:    opts.stream,
		EventType: "smoke.test.grpc",
		Sender:    "sugar-glider-grpc-smoke",
		Priority:  "normal",
		Payload:   payload,
	})
	pubCancel()
	if err != nil {
		exitf("publish RPC failed: %v", err)
	}
	entryID := strings.TrimSpace(pubResp.GetEntryId())
	if entryID == "" {
		exitf("publish returned empty entry_id (publish likely queued due Redis outage)")
	}
	fmt.Printf("Published stream=%s entry_id=%s smoke_id=%s\n", opts.stream, entryID, smokeID)

	var matchedID string
	for attempt := 1; attempt <= opts.attempts; attempt++ {
		subCtx, subCancel := context.WithTimeout(context.Background(), 8*time.Second)
		sub, subErr := client.Subscribe(subCtx, &synapsev1.SubscribeRequest{
			Stream:        opts.stream,
			ConsumerGroup: opts.group,
			ConsumerName:  opts.consumer,
			BatchSize:     int32(opts.batchSize),
		})
		if subErr != nil {
			subCancel()
			exitf("subscribe RPC failed on attempt %d: %v", attempt, subErr)
		}

		for {
			ev, recvErr := sub.Recv()
			if errors.Is(recvErr, io.EOF) {
				break
			}
			if recvErr != nil {
				subCancel()
				exitf("stream receive failed on attempt %d: %v", attempt, recvErr)
			}
			if ev.GetEntryId() == entryID || bytes.Contains(ev.GetPayload(), []byte(smokeID)) {
				matchedID = ev.GetEntryId()
				break
			}
		}
		subCancel()

		if matchedID != "" {
			break
		}
		fmt.Printf("Subscribe attempt %d/%d: no match yet\n", attempt, opts.attempts)
		time.Sleep(500 * time.Millisecond)
	}

	if matchedID == "" {
		exitf("timed out waiting for published entry_id=%s", entryID)
	}

	ackCtx, ackCancel := context.WithTimeout(context.Background(), 5*time.Second)
	ackResp, err := client.Ack(ackCtx, &synapsev1.AckRequest{
		Stream:        opts.stream,
		ConsumerGroup: opts.group,
		EntryIds:      []string{matchedID},
	})
	ackCancel()
	if err != nil {
		exitf("ack RPC failed: %v", err)
	}
	if ackResp.GetAcked() < 1 {
		exitf("ack RPC returned acked=%d", ackResp.GetAcked())
	}

	fmt.Printf("PASS: gRPC smoke succeeded (entry_id=%s, acked=%d)\n", matchedID, ackResp.GetAcked())
}

func exitf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "FAIL: "+format+"\n", args...)
	os.Exit(1)
}
