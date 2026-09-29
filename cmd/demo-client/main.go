package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
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
		Address: *addr,
		Headers: map[string]string{"authorization": "Bearer " + *token},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	sum, _, err := rpc.Call[demo.AddRequest, demo.AddResponse](context.Background(), client, "demo.Add", demo.AddRequest{A: 21, B: 21}, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("add: %d\n", sum.Sum)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := rpc.Call[demo.JobRequest, demo.JobResponse](ctx, client, "demo.Job", demo.JobRequest{ID: "observe-cancel"}, nil)
		done <- err
	}()
	time.Sleep(30 * time.Millisecond)
	cancel()
	if err := <-done; errors.Is(err, context.Canceled) {
		fmt.Println("canceled long job on one TCP connection")
	} else {
		log.Fatalf("unexpected cancel result: %v", err)
	}
}
