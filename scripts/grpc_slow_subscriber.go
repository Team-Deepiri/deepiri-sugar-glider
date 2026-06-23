// Blocks a gRPC Subscribe stream without calling Recv (slow consumer).
// Usage: go run ./scripts/grpc_slow_subscriber.go [--addr localhost:15051]
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	synapsev1 "github.com/Team-Deepiri/deepiri-sugar-glider/proto/synapse/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "localhost:15051", "gRPC address")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, *addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := synapsev1.NewSynapseSidecarClient(conn)
	subCtx, subCancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer subCancel()

	sub, err := client.Subscribe(subCtx, &synapsev1.SubscribeRequest{
		Stream: "platform-events", ConsumerGroup: "evict-ft",
		ConsumerName: fmt.Sprintf("slow-%d", time.Now().Unix()), BatchSize: 1,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "subscribe: %v\n", err)
		os.Exit(1)
	}
	_ = sub
	<-subCtx.Done()
}
