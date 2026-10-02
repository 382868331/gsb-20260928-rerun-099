package main

import (
	"errors"
	"flag"
	"log"
	"net"

	"lrpc/demo"
	"lrpc/rpc"
)

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
	rpc.HandleTyped(server, "demo.Status", demo.Status)
	rpc.HandleStream(server, "demo.Numbers", demo.Numbers)
	rpc.HandleStream(server, "demo.Watch", demo.Watch)
	rpc.HandleBidi(server, "demo.Chat", demo.Chat)

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
