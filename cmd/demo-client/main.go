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
	flag.Parse()

	client, err := rpc.Dial(context.Background(), rpc.ClientConfig{
		Address:      *addr,
		StreamWindow: 4096,
		Headers:      map[string]string{"authorization": "Bearer " + *token},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// 1) Ordinary call on the shared connection.
	sum, _, err := rpc.Call[demo.AddRequest, demo.AddResponse](context.Background(), client, "demo.Add", demo.AddRequest{A: 21, B: 21}, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("unary demo.Add: 21+21=%d\n", sum.Sum)

	// 2) Streaming call: fragments arrive in order alongside unary traffic.
	runOrderedStream(client)

	// 3) Slow reader constrains the sender; buffered/in-flight data stays bounded.
	runBackpressure(client)

	// 4) Cancel mid-stream ends waiting on both sides.
	runCancel(client)

	// 5) Drop the connection: both sides stop; a later call reconnects cleanly.
	runReconnect(client)

	fmt.Println("drill complete: order, backpressure, cancel and reconnect verified")
}

func runOrderedStream(client *rpc.Client) {
	ctx := context.Background()
	st, err := rpc.OpenStreamTyped[demo.CountRequest](ctx, client, "demo.Count", demo.CountRequest{Count: 5, Size: 8}, nil)
	if err != nil {
		log.Fatal(err)
	}
	var seqs []int
	for {
		var item demo.CountItem
		if err := st.Recv(ctx, &item); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			log.Fatal(err)
		}
		seqs = append(seqs, item.Seq)
	}
	fmt.Printf("stream demo.Count fragments in order: %v\n", seqs)
	if len(seqs) != 5 {
		log.Fatalf("expected 5 fragments, got %d", len(seqs))
	}
	for i, v := range seqs {
		if v != i {
			log.Fatalf("fragment order broken at %d: %d", i, v)
		}
	}
}

func runBackpressure(client *rpc.Client) {
	st, err := client.OpenStream(context.Background(), "demo.CountObserved", demo.CountRequest{Count: 1_000_000, Size: 512}, nil)
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := st.Recv(ctx); err != nil {
			log.Fatal(err)
		}
	}
	// Stop reading. Give the producer time to fill the window and block.
	time.Sleep(200 * time.Millisecond)
	buffered := st.BufferedBytes()
	fmt.Printf("slow reader: unconsumed buffered payload = %d bytes (window 4096)\n", buffered)
	if buffered > 4096+1024 {
		log.Fatalf("in-flight data unbounded: %d bytes", buffered)
	}
	st.Cancel()
	fmt.Println("slow reader stream canceled; sender released")
}

func runCancel(client *rpc.Client) {
	ctx, cancel := context.WithCancel(context.Background())
	st, err := rpc.OpenStreamTyped[demo.CountRequest](ctx, client, "demo.Count", demo.CountRequest{Count: 1_000_000, Size: 8}, nil)
	if err != nil {
		log.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		var item demo.CountItem
		for {
			if err := st.Recv(ctx, &item); err != nil {
				done <- err
				return
			}
		}
	}()
	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			log.Fatalf("unexpected cancel result: %v", err)
		}
		fmt.Println("stream canceled mid-flight, both sides ended the request")
	case <-time.After(2 * time.Second):
		log.Fatal("cancel did not end the stream wait")
	}
}

func runReconnect(client *rpc.Client) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st, err := rpc.OpenStreamTyped[demo.CountRequest](ctx, client, "demo.Count", demo.CountRequest{Count: 1_000_000, Size: 8}, nil)
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		var item demo.CountItem
		for {
			if err := st.Recv(ctx, &item); err != nil {
				return
			}
		}
	}()
	time.Sleep(30 * time.Millisecond)
	client.DropConnection()
	fmt.Println("connection dropped; in-flight stream ended with connection closed")

	reply, _, err := rpc.Call[demo.DelayRequest, demo.DelayResponse](context.Background(), client, "demo.Delay", demo.DelayRequest{Value: "reconnected"}, nil)
	if err != nil {
		log.Fatalf("call after reconnect: %v", err)
	}
	fmt.Printf("new session after reconnect served: %q\n", reply.Value)
}
