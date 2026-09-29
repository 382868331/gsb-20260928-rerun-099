package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	localrpc "localrpc"
)

func main() {
	listener := newPipeListener()
	server := localrpc.NewServer()
	server.Handle("demo", "echo", localrpc.JSONEcho)
	server.Handle("demo", "delay", localrpc.JSONDelay)
	go func() {
		if err := server.Serve(listener); err != nil {
			log.Fatal(err)
		}
	}()

	ctx := context.Background()
	clientConn, err := listener.Dial()
	if err != nil {
		log.Fatal(err)
	}
	client := localrpc.NewClient(clientConn)
	defer client.Close()

	run(ctx, client, "echo", "hello-local-rpc", 0)
	run(ctx, client, "delay", "first", 80)

	cancelCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	_, err = call(cancelCtx, client, "delay", "canceled", 300)
	fmt.Printf("canceled call: %v\n", err)

	run(ctx, client, "delay", "second", 30)
}

func run(ctx context.Context, client *localrpc.Client, method, text string, delay int) {
	payload, err := call(ctx, client, method, text, delay)
	if err != nil {
		log.Fatal(err)
	}
	var response localrpc.EchoResponse
	if err := json.Unmarshal(payload, &response); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s result: %s\n", method, response.Text)
}

func call(ctx context.Context, client *localrpc.Client, method, text string, delay int) ([]byte, error) {
	request := localrpc.DelayRequest{Text: text, DelayMillis: delay}
	if method == "echo" {
		request.DelayMillis = 0
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	response, err := client.Call(ctx, "demo", method, payload, nil)
	if err != nil && method == "echo" && errors.Is(err, context.DeadlineExceeded) {
		return nil, err
	}
	return response, err
}

type pipeListener struct {
	connections chan net.Conn
	closed      chan struct{}
}

func newPipeListener() *pipeListener {
	return &pipeListener{connections: make(chan net.Conn), closed: make(chan struct{})}
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.connections:
		return conn, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *pipeListener) Close() error {
	select {
	case <-l.closed:
	default:
		close(l.closed)
	}
	return nil
}

func (l *pipeListener) Addr() net.Addr {
	return pipeAddr{}
}

func (l *pipeListener) Dial() (net.Conn, error) {
	clientConn, serverConn := net.Pipe()
	select {
	case l.connections <- serverConn:
		return clientConn, nil
	case <-l.closed:
		_ = clientConn.Close()
		_ = serverConn.Close()
		return nil, net.ErrClosed
	}
}

type pipeAddr struct{}

func (pipeAddr) Network() string { return "pipe" }
func (pipeAddr) String() string  { return "127.0.0.1:pipe" }
