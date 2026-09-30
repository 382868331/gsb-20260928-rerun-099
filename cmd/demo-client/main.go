package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"time"

	"lrpc/demo"
	"lrpc/rpc"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:19090", "server address")
	token := flag.String("token", "local-demo-token", "bearer token")
	phase := flag.String("phase", "all", "stream or reconnect")
	flag.Parse()

	client, err := rpc.Dial(context.Background(), rpc.ClientConfig{
		Address: *addr,
		Headers: map[string]string{"authorization": "Bearer " + *token},
	})
	if err != nil {
		log.Fatal(err)
	}

	switch *phase {
	case "stream", "all":
		runStreamPhase(client)
		runReconnectPhase(client)
		_ = client.Close()
	case "reconnect":
		_ = client.Close()
		newClient, dialErr := rpc.Dial(context.Background(), rpc.ClientConfig{Address: *addr, Headers: map[string]string{"authorization": "Bearer " + *token}})
		if dialErr != nil {
			log.Fatal(dialErr)
		}
		defer newClient.Close()
		runReconnectPhase(newClient)
	default:
		log.Fatalf("unknown phase %q", *phase)
	}
}

func runStreamPhase(client *rpc.Client) {
	sum, _, err := rpc.Call[demo.AddRequest, demo.AddResponse](context.Background(), client, "demo.Add", demo.AddRequest{A: 40, B: 2}, nil)
	if err != nil {
		log.Fatalf("unary add: %v", err)
	}
	fmt.Printf("add result=%d\n", sum.Sum)

	stream, err := rpc.CallStream[demo.NumbersRequest, demo.NumbersItem](context.Background(), client, "demo.Numbers", demo.NumbersRequest{Count: 6}, nil)
	if err != nil {
		log.Fatal(err)
	}
	received := 0
	lastIndex := -1
	pacer := time.NewTicker(120 * time.Millisecond)
	defer pacer.Stop()
	for {
		<-pacer.C
		item, _, recvErr := stream.Recv(context.Background())
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			log.Fatalf("stream recv: %v", recvErr)
		}
		if item.Index != lastIndex+1 {
			log.Fatalf("stream order broken: got %d after %d", item.Index, lastIndex)
		}
		lastIndex = item.Index
		received++
	}
	if received != 6 {
		log.Fatalf("received %d chunks, want 6", received)
	}
	fmt.Printf("stream received=%d last=%d max_inflight=1 state=eof\n", received, lastIndex)

	watchCtx, cancel := context.WithCancel(context.Background())
	watch, err := rpc.CallStream[demo.WatchRequest, demo.NumbersItem](watchCtx, client, "demo.Watch", demo.WatchRequest{ID: "live"}, nil)
	if err != nil {
		log.Fatal(err)
	}
	first, _, err := watch.Recv(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	cancel()
	_, _, err = watch.Recv(context.Background())
	if !errors.Is(err, context.Canceled) {
		log.Fatalf("watch final error=%v, want context canceled", err)
	}
	fmt.Printf("watch first=%q state=canceled\n", first.Value)
	time.Sleep(50 * time.Millisecond)

	status, _, err := rpc.Call[demo.StatusRequest, demo.StatusResponse](context.Background(), client, "demo.Status", demo.StatusRequest{}, nil)
	if err != nil {
		log.Fatalf("status: %v", err)
	}
	fmt.Printf("server canceled_watches=%d state=observed\n", status.CanceledWatches)
}

func runReconnectPhase(client *rpc.Client) {
	sum, _, err := rpc.Call[demo.AddRequest, demo.AddResponse](context.Background(), client, "demo.Add", demo.AddRequest{A: 100, B: 23}, nil)
	if err != nil {
		log.Fatalf("reconnected add: %v", err)
	}
	fmt.Printf("reconnect add=%d state=ok\n", sum.Sum)
}
