package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net"
	"sync/atomic"

	"lrpc/demo"
	"lrpc/rpc"
)

var streamInFlight atomic.Int64

func main() {
	addr := flag.String("addr", "127.0.0.1:19090", "listen address")
	token := flag.String("token", "local-demo-token", "required bearer token")
	flag.Parse()

	server := rpc.NewServer(rpc.ServerConfig{
		Address:      *addr,
		Authenticate: authenticator(*token),
	})
	rpc.HandleTyped(server, "demo.Add", demo.Add)
	rpc.HandleTyped(server, "demo.Delay", demo.Delay)
	rpc.HandleTyped(server, "demo.Job", demo.Job)
	rpc.HandleStreamTyped(server, "demo.Count", demo.Count)
	server.HandleStream("demo.CountObserved", func(ctx context.Context, payload []byte, _ map[string]string, out rpc.StreamSender) error {
		var req demo.CountRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			return err
		}
		return demo.Count(ctx, &req, func(item demo.CountItem) error {
			data, err := json.Marshal(item)
			if err != nil {
				return err
			}
			if err := out.Send(data, nil); err != nil {
				return err
			}
			streamInFlight.Store(int64(out.Outstanding()))
			return nil
		})
	})

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("demo RPC server listening on %s", listener.Addr())
	log.Fatal(server.Serve(listener))
}

func authenticator(token string) rpc.Authenticator {
	return func(headers map[string]string) error {
		if headers["authorization"] != "Bearer "+token {
			return errors.New("unauthorized")
		}
		return nil
	}
}
