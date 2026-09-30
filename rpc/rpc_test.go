package rpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

type testPayload struct {
	Value string `json:"value"`
	Wait  int64  `json:"wait_nanos"`
}

type testReply struct {
	Value string `json:"value"`
}

func newTestServer(t *testing.T, handler RawHandler) *Server {
	t.Helper()
	server := NewServer(ServerConfig{Address: "pipe", MaxHandlers: 8})
	server.Handle("test", handler)
	t.Cleanup(func() { _ = server.Close() })
	return server
}

func dialTestClient(t *testing.T, server *Server, cfg ClientConfig) *Client {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	serveTestConn(server, serverConn)
	client := newClientWithConn(clientConn, cfg)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func serveTestConn(server *Server, conn net.Conn) {
	server.conns.Add(1)
	go func() {
		defer server.conns.Done()
		server.serveConn(conn)
	}()
}

func TestConcurrentInterleavedResponsesRemainCorrelated(t *testing.T) {
	handler := func(ctx context.Context, payload []byte, _ map[string]string) ([]byte, map[string]string, error) {
		var req testPayload
		codec := JSONCodec{}
		err := codec.Decode(payload, &req)
		if err != nil {
			return nil, nil, err
		}
		time.Sleep(time.Duration(req.Wait))
		return []byte(fmt.Sprintf(`{"value":%q}`, req.Value)), nil, nil
	}
	server := newTestServer(t, handler)
	client := dialTestClient(t, server, ClientConfig{})

	values := []string{"third", "first", "second"}
	delays := []time.Duration{80 * time.Millisecond, 10 * time.Millisecond, 40 * time.Millisecond}
	results := make([]string, len(values))
	errCh := make(chan error, len(values))
	var wg sync.WaitGroup
	for i := range values {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			reply, _, err := Call[testPayload, testReply](context.Background(), client, "test", testPayload{Value: values[i], Wait: int64(delays[i])}, nil)
			if err != nil {
				errCh <- err
				return
			}
			results[i] = reply.Value
		}(i)
	}
	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}
	for i, want := range values {
		if results[i] != want {
			t.Fatalf("result %d = %q, want %q", i, results[i], want)
		}
	}
}

func TestContextCancelSendsCancelAndStopsCall(t *testing.T) {
	started := make(chan struct{})
	finished := make(chan struct{}, 1)
	handler := func(ctx context.Context, _ []byte, _ map[string]string) ([]byte, map[string]string, error) {
		close(started)
		<-ctx.Done()
		finished <- struct{}{}
		return nil, nil, ctx.Err()
	}
	server := newTestServer(t, handler)
	client := dialTestClient(t, server, ClientConfig{})

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, _, err := Call[testPayload, testReply](ctx, client, "test", testPayload{}, nil)
		errCh <- err
	}()
	<-started
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("client error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("client call did not stop")
	}
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("server handler did not observe cancellation")
	}
}

func TestStreamOrderAndConcurrentCallsShareConnection(t *testing.T) {
	server := newTestServer(t, nil)
	HandleStream[testPayload, testReply](server, "stream", func(_ context.Context, req *testPayload, out *TypedServerStream[testReply]) error {
		for i := 0; i < 3; i++ {
			if _, err := out.Send(context.Background(), testReply{Value: fmt.Sprintf("%s-%d", req.Value, i)}); err != nil {
				return err
			}
		}
		return nil
	})
	server.Handle("test", func(_ context.Context, payload []byte, _ map[string]string) ([]byte, map[string]string, error) {
		var req testPayload
		if err := (JSONCodec{}).Decode(payload, &req); err != nil {
			return nil, nil, err
		}
		return []byte(fmt.Sprintf(`{"value":%q}`, "unary:"+req.Value)), nil, nil
	})
	client := dialTestClient(t, server, ClientConfig{})
	stream, err := CallStream[testPayload, testReply](context.Background(), client, "stream", testPayload{Value: "item"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	first, _, err := stream.Recv(context.Background())
	if err != nil || first.Value != "item-0" {
		t.Fatalf("first = %+v, %v", first, err)
	}
	got := []string{first.Value}
	for {
		item, _, err := stream.Recv(context.Background())
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, item.Value)
	}
	want := []string{"item-0", "item-1", "item-2"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("items = %v, want %v", got, want)
	}
	errCh := make(chan error, 2)
	for _, value := range []string{"one", "two"} {
		go func(value string) {
			_, _, err := Call[testPayload, testReply](context.Background(), client, "test", testPayload{Value: value}, nil)
			errCh <- err
		}(value)
	}
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
}

func TestStreamSlowReaderAppliesBackpressure(t *testing.T) {
	server := newTestServer(t, nil)
	firstSent := make(chan struct{})
	largeSent := make(chan error, 1)
	HandleStream[testPayload, testReply](server, "stream", func(ctx context.Context, _ *testPayload, out *TypedServerStream[testReply]) error {
		if _, err := out.Send(ctx, testReply{Value: "ready"}); err != nil {
			return err
		}
		close(firstSent)
		_, err := out.Send(ctx, testReply{Value: "large"})
		largeSent <- err
		return err
	})
	client := dialTestClient(t, server, ClientConfig{})
	stream, err := CallStream[testPayload, testReply](context.Background(), client, "stream", testPayload{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ready, _, err := stream.Recv(context.Background())
	if err != nil || ready.Value != "ready" {
		t.Fatalf("ready = %+v, %v", ready, err)
	}
	<-firstSent
	select {
	case err := <-largeSent:
		t.Fatalf("large send completed before reader consumed data: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	done := make(chan int, 1)
	go func() {
		item, _, err := stream.Recv(context.Background())
		if err != nil {
			done <- -1
			return
		}
		done <- len(item.Value)
	}()
	select {
	case n := <-done:
		if n != len("large") {
			t.Fatalf("large item size = %d", n)
		}
	case <-time.After(time.Second):
		t.Fatal("reader did not receive backpressured item")
	}
	select {
	case err := <-largeSent:
		if err != nil {
			t.Fatalf("server send: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server send did not finish after consumption")
	}
}

func TestStreamAckBypassesFullOrdinaryWriteQueue(t *testing.T) {
	server := newTestServer(t, nil)
	second := make(chan struct{})
	HandleStream[testPayload, testReply](server, "stream", func(ctx context.Context, _ *testPayload, out *TypedServerStream[testReply]) error {
		if _, err := out.Send(ctx, testReply{Value: "first"}); err != nil {
			return err
		}
		<-second
		if _, err := out.Send(ctx, testReply{Value: "second"}); err != nil {
			return err
		}
		return nil
	})
	client := dialTestClient(t, server, ClientConfig{WriteQueue: 1})
	stream, err := CallStream[testPayload, testReply](context.Background(), client, "stream", testPayload{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := stream.Recv(context.Background()); err != nil {
		t.Fatal(err)
	}
	close(second)
	for i := 0; i < 8; i++ {
		go func() { _, _ = client.Call(context.Background(), "missing", testPayload{}, nil, nil) }()
	}
	item, _, err := stream.Recv(context.Background())
	if err != nil {
		t.Fatalf("second item after prioritized ack: %v", err)
	}
	if item.Value != "second" {
		t.Fatalf("item = %q", item.Value)
	}
}

func TestStreamCancelStopsBothSidesWithoutBufferedResults(t *testing.T) {
	server := newTestServer(t, nil)
	started := make(chan struct{})
	serverStopped := make(chan struct{})
	HandleStream[testPayload, testReply](server, "stream", func(ctx context.Context, _ *testPayload, out *TypedServerStream[testReply]) error {
		defer close(serverStopped)
		if _, err := out.Send(ctx, testReply{Value: "start"}); err != nil {
			return err
		}
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	client := dialTestClient(t, server, ClientConfig{})
	stream, err := CallStream[testPayload, testReply](context.Background(), client, "stream", testPayload{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := stream.Recv(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-serverStopped:
	case <-time.After(time.Second):
		t.Fatal("server stream remained after cancel")
	}
}

func TestTimeout(t *testing.T) {
	handler := func(ctx context.Context, _ []byte, _ map[string]string) ([]byte, map[string]string, error) {
		<-ctx.Done()
		return nil, nil, ctx.Err()
	}
	server := newTestServer(t, handler)
	client := dialTestClient(t, server, ClientConfig{})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, _, err := Call[testPayload, testReply](ctx, client, "test", testPayload{}, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v", err)
	}
}

func TestDisconnectFailsAffectedCallsAndReconnectSucceeds(t *testing.T) {
	server := newTestServer(t, func(ctx context.Context, _ []byte, _ map[string]string) ([]byte, map[string]string, error) {
		<-ctx.Done()
		return nil, nil, ctx.Err()
	})
	clientConn, serverConn := net.Pipe()
	serveTestConn(server, serverConn)
	cfg := withDefaults(ClientConfig{})
	client := newClientWithConn(clientConn, cfg)
	oldConn := client.session
	errCh := make(chan error, 1)
	go func() {
		_, err := client.Call(context.Background(), "test", testPayload{}, nil, nil)
		errCh <- err
	}()
	time.Sleep(20 * time.Millisecond)
	oldConn.close(ErrConnectionClosed)
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrConnectionClosed) {
			t.Fatalf("error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending call survived disconnect")
	}

	newServer := NewServer(ServerConfig{MaxHandlers: 1})
	newServer.Handle("test", func(context.Context, []byte, map[string]string) ([]byte, map[string]string, error) {
		return []byte(`{"value":"new"}`), nil, nil
	})
	t.Cleanup(func() { _ = newServer.Close() })
	clientConn2, serverConn2 := net.Pipe()
	serveTestConn(newServer, serverConn2)
	c := client
	c.mu.Lock()
	newSession := c.newSession(clientConn2)
	c.session = newSession
	c.mu.Unlock()

	var reply testReply
	_, err := client.Call(context.Background(), "test", testPayload{Value: "new"}, &reply, nil)
	if err != nil {
		t.Fatalf("call after reconnect: %v", err)
	}
	if reply.Value != "new" {
		t.Fatalf("reply = %q", reply.Value)
	}
}

func TestLateResponseFromOldConnectionCannotMatchNewCall(t *testing.T) {
	oldClientConn, oldServerConn := net.Pipe()
	newClientConn, newServerConn := net.Pipe()
	client := newClientWithConn(oldClientConn, ClientConfig{})
	oldConn := client.session
	if oldConn == nil {
		t.Fatal("missing session")
	}
	oldID := oldConn.nextID.Load() + 1
	oldConn.close(ErrConnectionClosed)
	_ = oldServerConn.Close()
	newSession := client.newSession(newClientConn)
	client.mu.Lock()
	client.session = newSession
	client.mu.Unlock()
	newSession.nextID.Store(oldID - 1)

	newDone := make(chan *Envelope, 1)
	newSession.mu.Lock()
	newSession.pending[oldID] = &pendingCall{id: oldID, ch: newDone, done: make(chan struct{})}
	newSession.mu.Unlock()
	oldDone := make(chan *Envelope, 1)
	var oldCall *pendingCall
	oldConn.mu.Lock()
	oldCall = &pendingCall{id: oldID, ch: oldDone, done: make(chan struct{})}
	oldConn.pending[oldID] = oldCall
	oldConn.mu.Unlock()

	env := &Envelope{Type: TypeResponse, ID: oldID, Payload: []byte(`{"value":"stale"}`), Serializer: serializerJSON}
	oldConn.remove(oldID)
	select {
	case <-oldCall.done:
	default:
		t.Fatal("old session should not deliver after close")
	}
	select {
	case <-newDone:
		t.Fatal("late response from old session matched a new call")
	default:
	}

	var reply testReply
	resultCh := make(chan error, 1)
	go func() {
		_, err := client.Call(context.Background(), "test", testPayload{}, &reply, nil)
		resultCh <- err
	}()
	time.Sleep(20 * time.Millisecond)
	frame, err := EncodeEnvelope(env, defaultMaxFrameSize)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteFrame(newServerConn, defaultMaxFrameSize, frame); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-resultCh:
		if err != nil {
			t.Fatalf("new call failed: %v", err)
		}
		if reply.Value != "stale" {
			t.Fatalf("reply = %q", reply.Value)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("new call did not receive response")
	}
}

func TestBadFrameClosesOnlyConnection(t *testing.T) {
	server := newTestServer(t, func(context.Context, []byte, map[string]string) ([]byte, map[string]string, error) {
		return []byte(`{"value":"ok"}`), nil, nil
	})
	badClient, badServer := net.Pipe()
	serveTestConn(server, badServer)
	errCh := make(chan error, 1)
	go func() {
		_, err := badClient.Write([]byte{0xff, 0xff, 0xff, 0xff})
		errCh <- err
	}()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("bad frame write blocked")
	}
	_ = badClient.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	buf := make([]byte, 1)
	_, _ = badClient.Read(buf)
	_ = badClient.Close()

	client := dialTestClient(t, server, ClientConfig{})
	var reply testReply
	_, err := client.Call(context.Background(), "test", testPayload{Value: "ok"}, &reply, nil)
	if err != nil {
		t.Fatalf("server stopped after bad frame: %v", err)
	}
}

func TestLimits(t *testing.T) {
	t.Run("oversized request", func(t *testing.T) {
		client, serverConn := net.Pipe()
		go func() { _ = serverConn.Close() }()
		c := newClientWithConn(client, ClientConfig{MaxFrameSize: 32})
		_, err := c.Call(context.Background(), "test", testPayload{Value: "too large"}, nil, nil)
		_ = c.Close()
		if !errors.Is(err, ErrMessageTooLarge) {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("pending queue", func(t *testing.T) {
		server := newTestServer(t, func(ctx context.Context, _ []byte, _ map[string]string) ([]byte, map[string]string, error) {
			<-ctx.Done()
			return nil, nil, ctx.Err()
		})
		client := dialTestClient(t, server, ClientConfig{MaxPending: 1})
		started := make(chan struct{})
		go func() {
			client.Call(context.Background(), "test", testPayload{}, nil, nil)
			close(started)
		}()
		time.Sleep(20 * time.Millisecond)
		_, err := client.Call(context.Background(), "test", testPayload{}, nil, nil)
		if !errors.Is(err, ErrBusy) {
			t.Fatalf("error = %v", err)
		}
	})
}
