package rpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type chunkItem struct {
	Index int    `json:"index"`
	Text  string `json:"text"`
}

func newStreamTestServer(t *testing.T) *Server {
	t.Helper()
	server := NewServer(ServerConfig{Address: "pipe", MaxHandlers: 16, StreamWindow: 256})
	t.Cleanup(func() { _ = server.Close() })
	return server
}

func TestStreamDeliversFragmentsInOrder(t *testing.T) {
	server := newStreamTestServer(t)
	HandleStreamTyped(server, "seq", func(ctx context.Context, _ *testPayload, emit func(chunkItem) error) error {
		for i := 0; i < 20; i++ {
			if err := emit(chunkItem{Index: i, Text: fmt.Sprintf("fragment-%d", i)}); err != nil {
				return err
			}
		}
		return nil
	})
	client := dialTestClient(t, server, ClientConfig{StreamWindow: 256})

	st, err := OpenStreamTyped[testPayload](context.Background(), client, "seq", testPayload{Value: "go"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var indices []int
	for {
		var item chunkItem
		if err := st.Recv(context.Background(), &item); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		indices = append(indices, item.Index)
		if want := fmt.Sprintf("fragment-%d", item.Index); item.Text != want {
			t.Fatalf("text = %q, want %q", item.Text, want)
		}
	}
	if len(indices) != 20 {
		t.Fatalf("got %d fragments", len(indices))
	}
	for i, v := range indices {
		if v != i {
			t.Fatalf("out of order at %d: %d", i, v)
		}
	}
}

func TestStreamSlowReaderBackpressuresSender(t *testing.T) {
	const window = 128
	server := newStreamTestServer(t)
	var maxOutstanding atomic.Int64
	handlerStopped := make(chan struct{})
	server.HandleStream("burst", func(ctx context.Context, _ []byte, _ map[string]string, out StreamSender) error {
		defer close(handlerStopped)
		big := make([]byte, 512)
		for i := range big {
			big[i] = 'x'
		}
		for i := 0; i < 10000; i++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			cur := int64(out.Outstanding())
			for {
				m := maxOutstanding.Load()
				if cur <= m || maxOutstanding.CompareAndSwap(m, cur) {
					break
				}
			}
			if cur > int64(window)+int64(len(big))+256 {
				return fmt.Errorf("in-flight %d exceeded bound", cur)
			}
			if err := out.Send(big, nil); err != nil {
				return err
			}
		}
		return nil
	})
	client := dialTestClient(t, server, ClientConfig{StreamWindow: window})
	st, err := client.OpenStream(context.Background(), "burst", testPayload{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := st.Recv(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(200 * time.Millisecond)
	bound := window + 512
	if st.BufferedBytes() > bound {
		t.Fatalf("client buffered %d > bound %d", st.BufferedBytes(), bound)
	}
	if max := maxOutstanding.Load(); max < int64(window) {
		t.Fatalf("expected window to fill, max in-flight only %d", max)
	}
	st.Cancel()
	select {
	case <-handlerStopped:
	case <-time.After(2 * time.Second):
		t.Fatal("handler kept producing after cancel")
	}
}

func TestStreamCancelStopsBothSides(t *testing.T) {
	server := newStreamTestServer(t)
	handlerStopped := make(chan struct{})
	HandleStreamTyped(server, "forever", func(ctx context.Context, _ *testPayload, emit func(chunkItem) error) error {
		defer close(handlerStopped)
		for i := 0; ; i++ {
			if err := emit(chunkItem{Index: i}); err != nil {
				return err
			}
		}
	})
	client := dialTestClient(t, server, ClientConfig{StreamWindow: 64})
	st, err := OpenStreamTyped[testPayload](context.Background(), client, "forever", testPayload{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var got chunkItem
	if err := st.Recv(context.Background(), &got); err != nil {
		t.Fatal(err)
	}
	st.Cancel()
	select {
	case <-handlerStopped:
	case <-time.After(2 * time.Second):
		t.Fatal("server handler did not stop after cancel")
	}
	var again chunkItem
	if err := st.Recv(context.Background(), &again); !errors.Is(err, context.Canceled) {
		t.Fatalf("recv after cancel = %v, want context.Canceled", err)
	}
}

func TestStreamDisconnectEndsBothSidesAndReconnectWorks(t *testing.T) {
	server := newStreamTestServer(t)
	handlerStopped := make(chan struct{})
	HandleStreamTyped(server, "endless", func(ctx context.Context, _ *testPayload, emit func(chunkItem) error) error {
		defer close(handlerStopped)
		for i := 0; ; i++ {
			if err := emit(chunkItem{Index: i}); err != nil {
				return err
			}
		}
	})
	HandleTyped(server, "ping", func(ctx context.Context, req *testPayload) (testReply, error) {
		return testReply{Value: req.Value}, nil
	})

	clientConn, serverConn := net.Pipe()
	serveTestConn(server, serverConn)
	client := newClientWithConn(clientConn, ClientConfig{Address: "pipe", StreamWindow: 64})
	t.Cleanup(func() { _ = client.Close() })

	st, err := client.OpenStream(context.Background(), "endless", testPayload{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Recv(context.Background()); err != nil {
		t.Fatal(err)
	}

	client.DropConnection()
	select {
	case <-handlerStopped:
	case <-time.After(2 * time.Second):
		t.Fatal("server side did not stop on disconnect")
	}
	if _, err := st.Recv(context.Background()); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("stream after disconnect = %v", err)
	}

	clientConn2, serverConn2 := net.Pipe()
	serveTestConn(server, serverConn2)
	client.mu.Lock()
	client.session = client.newSession(clientConn2)
	client.mu.Unlock()
	reply, _, err := Call[testPayload, testReply](context.Background(), client, "ping", testPayload{Value: "after-reconnect"}, nil)
	if err != nil {
		t.Fatalf("call after reconnect: %v", err)
	}
	if reply.Value != "after-reconnect" {
		t.Fatalf("reply = %q", reply.Value)
	}
}

func TestUnaryAndStreamRemainCorrelatedOnSharedConnection(t *testing.T) {
	server := newStreamTestServer(t)
	HandleTyped(server, "slow", func(ctx context.Context, req *testPayload) (testReply, error) {
		time.Sleep(time.Duration(req.Wait))
		return testReply{Value: req.Value}, nil
	})
	HandleStreamTyped(server, "ticks", func(ctx context.Context, _ *testPayload, emit func(chunkItem) error) error {
		for i := 0; i < 5; i++ {
			time.Sleep(15 * time.Millisecond)
			if err := emit(chunkItem{Index: i}); err != nil {
				return err
			}
		}
		return nil
	})
	client := dialTestClient(t, server, ClientConfig{StreamWindow: 1024})

	type unaryResult struct {
		v   string
		err error
	}
	unaryDone := make(chan unaryResult, 3)
	for _, value := range []string{"a", "b", "c"} {
		go func(value string) {
			r, _, err := Call[testPayload, testReply](context.Background(), client, "slow", testPayload{Value: value, Wait: int64(60 * time.Millisecond)}, nil)
			unaryDone <- unaryResult{v: r.Value, err: err}
		}(value)
	}
	st, err := OpenStreamTyped[testPayload](context.Background(), client, "ticks", testPayload{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var indices []int
	for {
		var item chunkItem
		if err := st.Recv(context.Background(), &item); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		indices = append(indices, item.Index)
	}
	seen := map[string]bool{}
	for i := 0; i < 3; i++ {
		r := <-unaryDone
		if r.err != nil {
			t.Fatalf("unary failed: %v", r.err)
		}
		seen[r.v] = true
	}
	if !seen["a"] || !seen["b"] || !seen["c"] {
		t.Fatalf("unary results corrupted: %v", seen)
	}
	if len(indices) != 5 || indices[0] != 0 || indices[4] != 4 {
		t.Fatalf("stream indices = %v", indices)
	}
}

func TestLargeFragmentReassembledAcrossChunks(t *testing.T) {
	server := newStreamTestServer(t)
	payload := make([]byte, 5000)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}
	server.HandleStream("blob", func(ctx context.Context, _ []byte, _ map[string]string, out StreamSender) error {
		return out.Send(payload, nil)
	})
	client := dialTestClient(t, server, ClientConfig{StreamWindow: 128})
	st, err := client.OpenStream(context.Background(), "blob", testPayload{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	item, err := st.Recv(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if string(item.Payload) != string(payload) {
		t.Fatalf("reassembled length %d, want %d", len(item.Payload), len(payload))
	}
	if _, err := st.Recv(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("end = %v", err)
	}
}
