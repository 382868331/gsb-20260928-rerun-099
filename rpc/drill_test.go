package rpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

// TestEndToEndDrill mirrors the two-process demo over a real full-duplex byte
// stream: ordering, slow-reader backpressure, mid-stream cancel, disconnect
// and reconnect, with unary calls sharing the connection throughout.
func TestEndToEndDrill(t *testing.T) {
	server := NewServer(ServerConfig{MaxHandlers: 32, StreamWindow: 4096})
	server.Handle("add", func(ctx context.Context, p []byte, _ map[string]string) ([]byte, map[string]string, error) {
		return []byte(`{"sum":42}`), nil, nil
	})
	server.HandleStream("count", func(ctx context.Context, _ []byte, _ map[string]string, out StreamSender) error {
		for i := 0; i < 1_000_000; i++ {
			if err := out.Send([]byte(fmt.Sprintf(`{"seq":%d}`, i)), nil); err != nil {
				return err
			}
		}
		return nil
	})
	t.Cleanup(func() { _ = server.Close() })

	clientConn, serverConn := net.Pipe()
	serveTestConn(server, serverConn)
	client := newClientWithConn(clientConn, ClientConfig{Address: "pipe", StreamWindow: 4096})
	t.Cleanup(func() { _ = client.Close() })

	// Unary call while later streams reuse the same connection.
	var reply struct {
		Sum int `json:"sum"`
	}
	if _, err := client.Call(context.Background(), "add", nil, &reply, nil); err != nil {
		t.Fatal(err)
	}
	if reply.Sum != 42 {
		t.Fatalf("sum = %d", reply.Sum)
	}

	// Ordered stream.
	st, err := client.OpenStream(context.Background(), "count", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for want := 0; want < 10; want++ {
		item, err := st.Recv(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if string(item.Payload) != fmt.Sprintf(`{"seq":%d}`, want) {
			t.Fatalf("fragment = %s", item.Payload)
		}
	}

	// Slow reader: drain a few frames, then stop. In-flight stays bounded.
	for i := 0; i < 3; i++ {
		if _, err := st.Recv(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(150 * time.Millisecond)
	if buffered := st.BufferedBytes(); buffered > 4096+1024 {
		t.Fatalf("unbounded in-flight data: %d", buffered)
	}

	// A unary call must still complete on the shared connection while a slow
	// stream is flow-blocked (priority write lane).
	doneUnary := make(chan error, 1)
	go func() {
		var r struct {
			Sum int `json:"sum"`
		}
		_, e := client.Call(context.Background(), "add", nil, &r, nil)
		doneUnary <- e
	}()
	select {
	case err := <-doneUnary:
		if err != nil {
			t.Fatalf("unary during blocked stream: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unary call was head-of-line blocked by slow stream")
	}

	st.Cancel()
	if _, err := st.Recv(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("after cancel = %v", err)
	}

	// Disconnect ends waiting; reconnect serves fresh calls, old session stale.
	st2, err := client.OpenStream(context.Background(), "count", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = st2.Recv(context.Background()) }()
	time.Sleep(20 * time.Millisecond)
	client.DropConnection()
	if _, err := st2.Recv(context.Background()); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("disconnect stream = %v", err)
	}

	clientConn2, serverConn2 := net.Pipe()
	serveTestConn(server, serverConn2)
	client.mu.Lock()
	client.session = client.newSession(clientConn2)
	client.mu.Unlock()
	var reply2 struct {
		Sum int `json:"sum"`
	}
	if _, err := client.Call(context.Background(), "add", nil, &reply2, nil); err != nil {
		t.Fatalf("after reconnect: %v", err)
	}
	if reply2.Sum != 42 {
		t.Fatalf("reconnect sum = %d", reply2.Sum)
	}

	var _ = io.EOF
}
