package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"strings"
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
		runBidiPhase(client)
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

func runBidiPhase(client *rpc.Client) {
	chat, err := rpc.CallBidi[demo.ChatRequest](context.Background(), client, "demo.Chat", demo.ChatRequest{ID: "room-1"}, nil)
	if err != nil {
		log.Fatal(err)
	}
	sizes := []int{4, 64, 1024, 16, 256}
	for i, size := range sizes {
		text := strings.Repeat("m", size)
		if err := rpc.BidiSend(context.Background(), rpc.JSONCodec{}, chat.Send, demo.ChatMessage{Seq: i + 1, Text: text}); err != nil {
			log.Fatalf("bidi send %d: %v", i+1, err)
		}
		echo, err := rpc.BidiRecv[demo.ChatMessage](context.Background(), rpc.JSONCodec{}, chat.Recv)
		if err != nil {
			log.Fatalf("bidi recv %d: %v", i+1, err)
		}
		if echo.Seq != i+1 || echo.Text != "echo:"+text {
			log.Fatalf("bidi echo %d mismatch", i+1)
		}
	}
	if err := chat.CloseSend(context.Background()); err != nil {
		log.Fatal(err)
	}
	sum, _, err := rpc.Call[demo.AddRequest, demo.AddResponse](context.Background(), client, "demo.Add", demo.AddRequest{A: 1, B: 2}, nil)
	if err != nil || sum.Sum != 3 {
		log.Fatalf("unary during half-closed bidi: %+v, %v", sum, err)
	}
	summary, err := rpc.BidiRecv[demo.ChatMessage](context.Background(), rpc.JSONCodec{}, chat.Recv)
	if err != nil {
		log.Fatalf("bidi summary: %v", err)
	}
	if summary.Text != "done:5" {
		log.Fatalf("bidi summary = %q", summary.Text)
	}
	if _, err := rpc.BidiRecv[demo.ChatMessage](context.Background(), rpc.JSONCodec{}, chat.Recv); err != io.EOF {
		log.Fatalf("bidi final recv = %v", err)
	}
	state := chat.State()
	if !state.Done || state.Err != nil || state.Sent != 5 || state.Acked != 5 || state.Received != 6 {
		log.Fatalf("bidi final state: %+v", state)
	}
	fmt.Printf("bidi sent=%d acked=%d received=%d state=eof\n", state.Sent, state.Acked, state.Received)
}

func runReconnectPhase(client *rpc.Client) {
	sum, _, err := rpc.Call[demo.AddRequest, demo.AddResponse](context.Background(), client, "demo.Add", demo.AddRequest{A: 100, B: 23}, nil)
	if err != nil {
		log.Fatalf("reconnected add: %v", err)
	}
	fmt.Printf("reconnect add=%d state=ok\n", sum.Sum)
}
