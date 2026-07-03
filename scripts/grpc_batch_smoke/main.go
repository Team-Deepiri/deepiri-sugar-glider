// One-off smoke: gRPC PublishBatch (N events) + Subscribe + Ack.
// Usage: go run ./scripts/grpc_batch_smoke [--addr localhost:15051] [--n 10]
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	synapsev1 "github.com/Team-Deepiri/deepiri-sugar-glider/proto/synapse/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "localhost:15051", "gRPC address")
	stream := flag.String("stream", "platform-events", "stream name")
	n := flag.Int("n", 10, "batch size")
	flag.Parse()
	if *n < 1 {
		fail("n must be >= 1")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, *addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		fail("dial: %v", err)
	}
	defer conn.Close()

	client := synapsev1.NewSynapseSidecarClient(conn)
	group := fmt.Sprintf("batch-smoke-%d", time.Now().Unix())
	consumer := fmt.Sprintf("batch-consumer-%d", time.Now().Unix())

	events := make([]*synapsev1.PublishRequest, 0, *n)
	ids := make([]string, 0, *n)
	for i := 0; i < *n; i++ {
		smokeID := fmt.Sprintf("batch-smoke-%d-%d", time.Now().UnixNano(), i)
		ids = append(ids, smokeID)
		payload, _ := json.Marshal(map[string]any{"smoke_id": smokeID, "i": i})
		events = append(events, &synapsev1.PublishRequest{
			Stream: *stream, EventType: "smoke.test.batch", Sender: "batch-smoke",
			Priority: "normal", Payload: payload,
		})
	}

	pubCtx, pubCancel := context.WithTimeout(context.Background(), 10*time.Second)
	resp, err := client.PublishBatch(pubCtx, &synapsev1.PublishBatchRequest{Stream: *stream, Events: events})
	pubCancel()
	if err != nil {
		fail("PublishBatch: %v", err)
	}
	entryIDs := resp.GetEntryIds()
	if len(entryIDs) != *n {
		fail("entry_ids len=%d want %d", len(entryIDs), *n)
	}
	for i, id := range entryIDs {
		if strings.TrimSpace(id) == "" {
			fail("empty entry_id at index %d", i)
		}
	}

	found := make(map[string]bool)
	for attempt := 0; attempt < 15 && len(found) < *n; attempt++ {
		subCtx, subCancel := context.WithTimeout(context.Background(), 8*time.Second)
		sub, err := client.Subscribe(subCtx, &synapsev1.SubscribeRequest{
			Stream: *stream, ConsumerGroup: group, ConsumerName: consumer, BatchSize: 50,
		})
		if err != nil {
			subCancel()
			fail("Subscribe: %v", err)
		}
		for {
			ev, err := sub.Recv()
			if err != nil {
				break
			}
			for _, smokeID := range ids {
				if strings.Contains(string(ev.GetPayload()), smokeID) {
					found[smokeID] = true
				}
			}
			if len(found) == *n {
				break
			}
		}
		subCancel()
		time.Sleep(300 * time.Millisecond)
	}
	if len(found) != *n {
		fail("matched %d/%d events", len(found), *n)
	}

	ackCtx, ackCancel := context.WithTimeout(context.Background(), 5*time.Second)
	ackResp, err := client.Ack(ackCtx, &synapsev1.AckRequest{
		Stream: *stream, ConsumerGroup: group, EntryIds: entryIDs,
	})
	ackCancel()
	if err != nil {
		fail("Ack: %v", err)
	}
	if ackResp.GetAcked() < int32(*n) {
		fail("acked=%d want>=%d", ackResp.GetAcked(), *n)
	}

	fmt.Printf("PASS: PublishBatch smoke (%d events, acked=%d)\n", *n, ackResp.GetAcked())
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "FAIL: "+format+"\n", args...)
	os.Exit(1)
}
