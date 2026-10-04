package rpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func bidiSegment(tag string, seq, size int) []byte {
	head := fmt.Sprintf("%s-%d-", tag, seq)
	if size < len(head) {
		size = len(head)
	}
	return []byte(head + strings.Repeat(string(rune('a'+seq%26)), size-len(head)))
}

func checkBidiSegment(t *testing.T, tag string, seq, size int, payload []byte) {
	t.Helper()
	want := bidiSegment(tag, seq, size)
	if string(payload) != string(want) {
		t.Fatalf("segment %d content mismatch: got %d bytes, want %d", seq, len(payload), len(want))
	}
}

func echoBidiHandler(ctx context.Context, stream *ServerBidiStream) error {
	for {
		payload, _, err := stream.Recv(ctx)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(ctx, payload, nil); err != nil {
			return err
		}
	}
}

func unaryTestHandler(_ context.Context, payload []byte, _ map[string]string) ([]byte, map[string]string, error) {
	var req testPayload
	if err := (JSONCodec{}).Decode(payload, &req); err != nil {
		return nil, nil, err
	}
	return []byte(fmt.Sprintf("{\"value\":%q}", "unary:"+req.Value)), nil, nil
}

func newBidiTestServer(t *testing.T) *Server {
	t.Helper()
	server := NewServer(ServerConfig{Address: "pipe", MaxHandlers: 16})
	server.HandleBidi("echo", func(ctx context.Context, _ []byte, _ map[string]string, stream *ServerBidiStream) error {
		return echoBidiHandler(ctx, stream)
	})
	server.Handle("test", unaryTestHandler)
	t.Cleanup(func() { _ = server.Close() })
	return server
}

func TestBidiParallelStreamsAndCallsShareConnection(t *testing.T) {
	server := newBidiTestServer(t)
	client := dialTestClient(t, server, ClientConfig{})

	sizes := [][]int{
		{1, 4096, 17, 65536},
		{100000, 3, 1024},
		{512, 512, 512, 512, 512},
	}
	var wg sync.WaitGroup
	errCh := make(chan error, 16)
	for s := range sizes {
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			tag := fmt.Sprintf("stream%d", s)
			stream, err := client.BidiStream(context.Background(), "echo", testPayload{Value: tag}, nil)
			if err != nil {
				errCh <- err
				return
			}
			for i, size := range sizes[s] {
				seq := i + 1
				if err := stream.Send(context.Background(), bidiSegment(tag, seq, size), nil); err != nil {
					errCh <- fmt.Errorf("%s send %d: %w", tag, seq, err)
					return
				}
				echo, _, err := stream.Recv(context.Background())
				if err != nil {
					errCh <- fmt.Errorf("%s recv %d: %w", tag, seq, err)
					return
				}
				checkBidiSegment(t, tag, seq, size, echo)
			}
			if err := stream.CloseSend(context.Background()); err != nil {
				errCh <- fmt.Errorf("%s close send: %w", tag, err)
				return
			}
			if _, _, err := stream.Recv(context.Background()); err != io.EOF {
				errCh <- fmt.Errorf("%s final recv: %v", tag, err)
				return
			}
			state := stream.State()
			want := uint64(len(sizes[s]))
			if !state.Done || state.Err != nil || !state.SendClosed || !state.RecvClosed {
				errCh <- fmt.Errorf("%s final state: %+v", tag, state)
				return
			}
			if state.Sent != want || state.Acked != want || state.Received != want {
				errCh <- fmt.Errorf("%s counters: %+v want %d", tag, state, want)
			}
		}(s)
	}
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			reply, _, err := Call[testPayload, testReply](context.Background(), client, "test", testPayload{Value: fmt.Sprintf("call-%d", i)}, nil)
			if err != nil {
				errCh <- err
				return
			}
			if reply.Value != fmt.Sprintf("unary:call-%d", i) {
				errCh <- fmt.Errorf("unary reply = %q", reply.Value)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

func TestBidiHalfCloseKeepsOtherDirectionAlive(t *testing.T) {
	server := newBidiTestServer(t)
	serverState := make(chan BidiState, 1)
	server.HandleBidi("drain-then-push", func(ctx context.Context, _ []byte, _ map[string]string, stream *ServerBidiStream) error {
		defer func() { serverState <- stream.State() }()
		for i := 1; i <= 2; i++ {
			payload, _, err := stream.Recv(ctx)
			if err != nil {
				return fmt.Errorf("recv %d: %w", i, err)
			}
			checkBidiSegment(t, "up", i, 64*(1<<(i-1)), payload)
		}
		if _, _, err := stream.Recv(ctx); err != io.EOF {
			return fmt.Errorf("expected client half-close, got %v", err)
		}
		for i := 1; i <= 3; i++ {
			if err := stream.Send(ctx, bidiSegment("down", i, 32*i), nil); err != nil {
				return err
			}
		}
		return stream.CloseSend(ctx)
	})
	client := dialTestClient(t, server, ClientConfig{})

	stream, err := client.BidiStream(context.Background(), "drain-then-push", testPayload{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 2; i++ {
		if err := stream.Send(context.Background(), bidiSegment("up", i, 64*(1<<(i-1))), nil); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	if err := stream.CloseSend(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(context.Background(), []byte("x"), nil); !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("send after half-close = %v", err)
	}
	reply, _, err := Call[testPayload, testReply](context.Background(), client, "test", testPayload{Value: "during-half-close"}, nil)
	if err != nil || reply.Value != "unary:during-half-close" {
		t.Fatalf("unary during half-close: %+v, %v", reply, err)
	}
	for i := 1; i <= 3; i++ {
		payload, _, err := stream.Recv(context.Background())
		if err != nil {
			t.Fatalf("recv %d: %v", i, err)
		}
		checkBidiSegment(t, "down", i, 32*i, payload)
	}
	if _, _, err := stream.Recv(context.Background()); err != io.EOF {
		t.Fatalf("final recv = %v", err)
	}
	state := stream.State()
	if !state.Done || state.Err != nil || !state.SendClosed || !state.RecvClosed {
		t.Fatalf("client final state: %+v", state)
	}
	if state.Sent != 2 || state.Acked != 2 || state.Received != 3 {
		t.Fatalf("client counters: %+v", state)
	}
	select {
	case st := <-serverState:
		if st.Sent != 3 || st.Acked != 3 || st.Received != 2 || !st.SendClosed || !st.RecvClosed {
			t.Fatalf("server final state: %+v", st)
		}
	case <-time.After(time.Second):
		t.Fatal("server handler did not finish")
	}
}

func TestBidiCancelLeavesOtherStreamsAndCallsRunning(t *testing.T) {
	server := newBidiTestServer(t)
	canceled := make(chan BidiState, 1)
	server.HandleBidi("watch", func(ctx context.Context, _ []byte, _ map[string]string, stream *ServerBidiStream) error {
		defer func() { canceled <- stream.State() }()
		if err := stream.Send(ctx, []byte("start"), nil); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	})
	client := dialTestClient(t, server, ClientConfig{})

	victim, err := client.BidiStream(context.Background(), "watch", testPayload{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := victim.Recv(context.Background()); err != nil {
		t.Fatal(err)
	}
	survivor, err := client.BidiStream(context.Background(), "echo", testPayload{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	victim.Cancel()
	if _, _, err := victim.Recv(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled stream recv = %v", err)
	}
	if state := victim.State(); !state.Done || !errors.Is(state.Err, context.Canceled) {
		t.Fatalf("canceled state: %+v", state)
	}
	select {
	case st := <-canceled:
		if st.Sent != 1 {
			t.Fatalf("server canceled stream state: %+v", st)
		}
	case <-time.After(time.Second):
		t.Fatal("server handler did not observe cancel")
	}

	if err := survivor.Send(context.Background(), bidiSegment("keep", 1, 128), nil); err != nil {
		t.Fatal(err)
	}
	echo, _, err := survivor.Recv(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	checkBidiSegment(t, "keep", 1, 128, echo)
	if err := survivor.CloseSend(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := survivor.Recv(context.Background()); err != io.EOF {
		t.Fatalf("survivor final recv = %v", err)
	}
	reply, _, err := Call[testPayload, testReply](context.Background(), client, "test", testPayload{Value: "after-cancel"}, nil)
	if err != nil || reply.Value != "unary:after-cancel" {
		t.Fatalf("unary after cancel: %+v, %v", reply, err)
	}
}

func TestBidiSlowReceiverBoundsOnlyItsOwnStream(t *testing.T) {
	server := newBidiTestServer(t)
	sendDone := make(chan int, 8)
	server.HandleBidi("push", func(ctx context.Context, _ []byte, _ map[string]string, stream *ServerBidiStream) error {
		for i := 1; i <= 4; i++ {
			if err := stream.Send(ctx, bidiSegment("push", i, 256), nil); err != nil {
				return err
			}
			sendDone <- i
		}
		return nil
	})
	client := dialTestClient(t, server, ClientConfig{})

	stream, err := client.BidiStream(context.Background(), "push", testPayload{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseSend(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The reader does not consume: with a window of one, no send can
	// complete and at most one segment stays in flight.
	select {
	case n := <-sendDone:
		t.Fatalf("segment %d completed while reader stalled", n)
	case <-time.After(80 * time.Millisecond):
	}
	deadline := time.After(2 * time.Second)
	for i := 0; i < 10; i++ {
		result := make(chan error, 1)
		go func(i int) {
			_, _, err := Call[testPayload, testReply](context.Background(), client, "test", testPayload{Value: fmt.Sprintf("fast-%d", i)}, nil)
			result <- err
		}(i)
		select {
		case err := <-result:
			if err != nil {
				t.Fatal(err)
			}
		case <-deadline:
			t.Fatal("short request starved by slow stream")
		}
	}
	fast, err := client.BidiStream(context.Background(), "echo", testPayload{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := fast.Send(context.Background(), bidiSegment("fast", 1, 64), nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := fast.Recv(context.Background()); err != nil {
		t.Fatalf("second stream starved: %v", err)
	}
	if err := fast.CloseSend(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := fast.Recv(context.Background()); err != io.EOF {
		t.Fatalf("fast stream final recv = %v", err)
	}
	for i := 1; i <= 4; i++ {
		payload, _, err := stream.Recv(context.Background())
		if err != nil {
			t.Fatalf("drain %d: %v", i, err)
		}
		checkBidiSegment(t, "push", i, 256, payload)
		select {
		case n := <-sendDone:
			if n != i {
				t.Fatalf("server completed segment %d, want %d", n, i)
			}
		case <-time.After(time.Second):
			t.Fatalf("server send %d did not complete after ack", i)
		}
	}
	if _, _, err := stream.Recv(context.Background()); err != io.EOF {
		t.Fatalf("push final recv = %v", err)
	}
	if state := stream.State(); state.Received != 4 || state.Err != nil {
		t.Fatalf("push final state: %+v", state)
	}
}

func TestBidiDisconnectReportsAckedAndUndetermined(t *testing.T) {
	var unaryCalls atomic.Int64
	serverState := make(chan BidiState, 1)
	server := NewServer(ServerConfig{Address: "pipe", MaxHandlers: 8})
	server.HandleBidi("sink", func(ctx context.Context, _ []byte, _ map[string]string, stream *ServerBidiStream) error {
		defer func() { serverState <- stream.State() }()
		if _, _, err := stream.Recv(ctx); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	})
	server.Handle("test", func(ctx context.Context, _ []byte, _ map[string]string) ([]byte, map[string]string, error) {
		unaryCalls.Add(1)
		<-ctx.Done()
		return nil, nil, ctx.Err()
	})
	t.Cleanup(func() { _ = server.Close() })
	client := dialTestClient(t, server, ClientConfig{})

	stream, err := client.BidiStream(context.Background(), "sink", testPayload{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(context.Background(), bidiSegment("lost", 1, 64), nil); err != nil {
		t.Fatal(err)
	}
	sendErr := make(chan error, 1)
	go func() {
		sendErr <- stream.Send(context.Background(), bidiSegment("lost", 2, 64), nil)
	}()
	callErr := make(chan error, 1)
	go func() {
		_, err := client.Call(context.Background(), "test", testPayload{}, nil, nil)
		callErr <- err
	}()
	time.Sleep(100 * time.Millisecond)

	old := client.session
	old.close(ErrConnectionClosed)

	select {
	case err := <-sendErr:
		if !errors.Is(err, ErrConnectionClosed) {
			t.Fatalf("stream send after break = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked send survived disconnect")
	}
	select {
	case err := <-callErr:
		if !errors.Is(err, ErrConnectionClosed) {
			t.Fatalf("unary call after break = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending call survived disconnect")
	}
	state := stream.State()
	if !state.Done || !errors.Is(state.Err, ErrConnectionClosed) {
		t.Fatalf("stream state after break: %+v", state)
	}
	if state.Acked != 1 || state.Sent != 2 {
		t.Fatalf("acked/sent = %d/%d, want 1/2 (segment 2 undetermined)", state.Acked, state.Sent)
	}
	select {
	case st := <-serverState:
		if st.Received != 1 {
			t.Fatalf("server state after break: %+v", st)
		}
	case <-time.After(time.Second):
		t.Fatal("server handler did not end after break")
	}
	// The interrupted unary call must not be replayed silently.
	if got := unaryCalls.Load(); got != 1 {
		t.Fatalf("unary handler ran %d times, want exactly 1 (no silent replay)", got)
	}

	// A new session only serves new calls.
	newServer := newBidiTestServer(t)
	clientConn2, serverConn2 := net.Pipe()
	serveTestConn(newServer, serverConn2)
	client.mu.Lock()
	client.session = client.newSession(clientConn2)
	client.mu.Unlock()
	reply, _, err := Call[testPayload, testReply](context.Background(), client, "test", testPayload{Value: "fresh"}, nil)
	if err != nil || reply.Value != "unary:fresh" {
		t.Fatalf("call on new session: %+v, %v", reply, err)
	}
	if got := unaryCalls.Load(); got != 1 {
		t.Fatalf("broken call replayed on new session: handler ran %d times", got)
	}
}

func TestBidiServerEarlyReturnResetsInboundDirection(t *testing.T) {
	server := newBidiTestServer(t)
	server.HandleBidi("quit-early", func(ctx context.Context, _ []byte, _ map[string]string, stream *ServerBidiStream) error {
		if err := stream.Send(ctx, []byte("only"), nil); err != nil {
			return err
		}
		return nil
	})
	client := dialTestClient(t, server, ClientConfig{})

	stream, err := client.BidiStream(context.Background(), "quit-early", testPayload{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	payload, _, err := stream.Recv(context.Background())
	if err != nil || string(payload) != "only" {
		t.Fatalf("first recv: %q, %v", payload, err)
	}
	if _, _, err := stream.Recv(context.Background()); err != io.EOF {
		t.Fatalf("recv after handler return = %v, want io.EOF", err)
	}
	if err := stream.Send(context.Background(), []byte("too-late"), nil); !errors.Is(err, ErrStreamReset) {
		t.Fatalf("send to abandoned stream = %v, want ErrStreamReset", err)
	}
	state := stream.State()
	if !state.Done || !errors.Is(state.Err, ErrStreamReset) {
		t.Fatalf("reset stream state: %+v", state)
	}
	// The connection itself stays healthy for other work.
	reply, _, err := Call[testPayload, testReply](context.Background(), client, "test", testPayload{Value: "alive"}, nil)
	if err != nil || reply.Value != "unary:alive" {
		t.Fatalf("unary after reset: %+v, %v", reply, err)
	}
}

func waitForBufferUsed(t *testing.T, client *Client, want int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := client.BufferState().Used; got == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("buffer used = %d, want %d", client.BufferState().Used, want)
}

func TestBidiSharedBufferCreditBoundsAndReuses(t *testing.T) {
	server := NewServer(ServerConfig{Address: "pipe", MaxHandlers: 16})
	gate := make(chan struct{})
	server.HandleBidi("gated", func(ctx context.Context, _ []byte, _ map[string]string, stream *ServerBidiStream) error {
		<-gate
		return echoBidiHandler(ctx, stream)
	})
	server.Handle("test", unaryTestHandler)
	t.Cleanup(func() { _ = server.Close() })
	client := dialTestClient(t, server, ClientConfig{StreamBuffer: 128 * 1024})

	if state := client.BufferState(); state.Limit != 128*1024 || state.Used != 0 {
		t.Fatalf("initial buffer state: %+v", state)
	}
	big := bidiSegment("big", 1, 100_000)

	first, err := client.BidiStream(context.Background(), "gated", testPayload{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	firstErr := make(chan error, 1)
	go func() { firstErr <- first.Send(context.Background(), big, nil) }()
	// The server is gated, so the segment stays unacknowledged and holds
	// 100KB of the 128KB connection budget.
	waitForBufferUsed(t, client, 100_000)

	second, err := client.BidiStream(context.Background(), "gated", testPayload{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	secondErr := make(chan error, 1)
	go func() { secondErr <- second.Send(context.Background(), big, nil) }()
	select {
	case err := <-secondErr:
		t.Fatalf("second big send finished while credit exhausted: %v", err)
	case <-time.After(80 * time.Millisecond):
	}
	// Short requests do not draw stream credit and must not starve.
	reply, _, err := Call[testPayload, testReply](context.Background(), client, "test", testPayload{Value: "small"}, nil)
	if err != nil || reply.Value != "unary:small" {
		t.Fatalf("unary during credit pressure: %+v, %v", reply, err)
	}
	// Cancellation is a control frame: it bypasses the credit wait.
	second.Cancel()
	select {
	case err := <-secondErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("credited send after cancel = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancel did not unblock a send waiting for credit")
	}
	// Let the server drain: the first send completes and its credit is
	// released for other streams.
	close(gate)
	echo, _, err := first.Recv(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	checkBidiSegment(t, "big", 1, 100_000, echo)
	select {
	case err := <-firstErr:
		if err != nil {
			t.Fatalf("first send: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first send did not complete after gate opened")
	}
	if err := first.CloseSend(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := first.Recv(context.Background()); err != io.EOF {
		t.Fatalf("first final recv = %v", err)
	}
	waitForBufferUsed(t, client, 0)

	// The released budget serves a new large stream end to end.
	third, err := client.BidiStream(context.Background(), "gated", testPayload{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := third.Send(context.Background(), big, nil); err != nil {
		t.Fatalf("reused credit send: %v", err)
	}
	echo, _, err = third.Recv(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	checkBidiSegment(t, "big", 1, 100_000, echo)
	if err := third.CloseSend(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := third.Recv(context.Background()); err != io.EOF {
		t.Fatalf("third final recv = %v", err)
	}
	waitForBufferUsed(t, client, 0)
	if state := third.State(); !state.Done || state.Err != nil || state.Acked != 1 || state.Pending != 0 {
		t.Fatalf("third final state: %+v", state)
	}
}

func TestBidiMultipleSlowReceiversStayIsolated(t *testing.T) {
	server := NewServer(ServerConfig{Address: "pipe", MaxHandlers: 16})
	sendDone := make(chan string, 8)
	server.HandleBidi("push", func(ctx context.Context, payload []byte, _ map[string]string, stream *ServerBidiStream) error {
		var req testPayload
		if err := (JSONCodec{}).Decode(payload, &req); err != nil {
			return err
		}
		for i := 1; i <= 2; i++ {
			if err := stream.Send(ctx, bidiSegment(req.Value, i, 128), nil); err != nil {
				return err
			}
			sendDone <- fmt.Sprintf("%s-%d", req.Value, i)
		}
		return nil
	})
	server.Handle("test", unaryTestHandler)
	t.Cleanup(func() { _ = server.Close() })
	client := dialTestClient(t, server, ClientConfig{})

	open := func(tag string) *BidiStream {
		stream, err := client.BidiStream(context.Background(), "push", testPayload{Value: tag}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.CloseSend(context.Background()); err != nil {
			t.Fatal(err)
		}
		return stream
	}
	slow1 := open("slow1")
	slow2 := open("slow2")

	// Neither reader consumes: no send may complete and short requests
	// must still flow.
	select {
	case done := <-sendDone:
		t.Fatalf("segment %s completed while both readers stalled", done)
	case <-time.After(80 * time.Millisecond):
	}
	for i := 0; i < 4; i++ {
		if _, _, err := Call[testPayload, testReply](context.Background(), client, "test", testPayload{Value: "fast"}, nil); err != nil {
			t.Fatal(err)
		}
	}
	// Draining the first stream releases exactly its own segments; the
	// second stream keeps its own content and sequence.
	for i := 1; i <= 2; i++ {
		payload, _, err := slow1.Recv(context.Background())
		if err != nil {
			t.Fatalf("slow1 drain %d: %v", i, err)
		}
		checkBidiSegment(t, "slow1", i, 128, payload)
		select {
		case done := <-sendDone:
			if done != fmt.Sprintf("slow1-%d", i) {
				t.Fatalf("server completed %s, want slow1-%d", done, i)
			}
		case <-time.After(time.Second):
			t.Fatalf("slow1 segment %d not completed after ack", i)
		}
	}
	if _, _, err := slow1.Recv(context.Background()); err != io.EOF {
		t.Fatalf("slow1 final recv = %v", err)
	}
	for i := 1; i <= 2; i++ {
		payload, _, err := slow2.Recv(context.Background())
		if err != nil {
			t.Fatalf("slow2 drain %d: %v", i, err)
		}
		checkBidiSegment(t, "slow2", i, 128, payload)
	}
	if _, _, err := slow2.Recv(context.Background()); err != io.EOF {
		t.Fatalf("slow2 final recv = %v", err)
	}
	for tag, stream := range map[string]*BidiStream{"slow1": slow1, "slow2": slow2} {
		state := stream.State()
		if state.Received != 2 || state.Err != nil {
			t.Fatalf("%s final state: %+v", tag, state)
		}
	}
}

func TestBidiSimultaneousHalfClose(t *testing.T) {
	server := newBidiTestServer(t)
	serverState := make(chan BidiState, 1)
	server.HandleBidi("ping-pong", func(ctx context.Context, _ []byte, _ map[string]string, stream *ServerBidiStream) error {
		defer func() { serverState <- stream.State() }()
		for i := 1; i <= 3; i++ {
			payload, _, err := stream.Recv(ctx)
			if err != nil {
				return fmt.Errorf("recv %d: %w", i, err)
			}
			checkBidiSegment(t, "up", i, 16*i, payload)
			if err := stream.Send(ctx, bidiSegment("down", i, 8*i), nil); err != nil {
				return err
			}
		}
		if _, _, err := stream.Recv(ctx); err != io.EOF {
			return fmt.Errorf("expected client half-close, got %v", err)
		}
		// Half-close immediately without waiting for the client to read:
		// both directions close concurrently.
		return stream.CloseSend(ctx)
	})
	client := dialTestClient(t, server, ClientConfig{})

	stream, err := client.BidiStream(context.Background(), "ping-pong", testPayload{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		if err := stream.Send(context.Background(), bidiSegment("up", i, 16*i), nil); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
		payload, _, err := stream.Recv(context.Background())
		if err != nil {
			t.Fatalf("recv %d: %v", i, err)
		}
		checkBidiSegment(t, "down", i, 8*i, payload)
	}
	if err := stream.CloseSend(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := stream.Recv(context.Background()); err != io.EOF {
		t.Fatalf("final recv = %v", err)
	}
	state := stream.State()
	if !state.Done || state.Err != nil || !state.SendClosed || !state.RecvClosed {
		t.Fatalf("client final state: %+v", state)
	}
	if state.Sent != 3 || state.Acked != 3 || state.Received != 3 || state.Pending != 0 {
		t.Fatalf("client counters: %+v", state)
	}
	select {
	case st := <-serverState:
		if st.Sent != 3 || st.Acked != 3 || st.Received != 3 || !st.SendClosed || !st.RecvClosed || st.Pending != 0 {
			t.Fatalf("server final state: %+v", st)
		}
	case <-time.After(time.Second):
		t.Fatal("server handler did not finish")
	}
}

func TestBidiCancelInterleavedWithTailDelivery(t *testing.T) {
	server := newBidiTestServer(t)
	serverDone := make(chan BidiState, 1)
	server.HandleBidi("burst", func(ctx context.Context, _ []byte, _ map[string]string, stream *ServerBidiStream) error {
		defer func() { serverDone <- stream.State() }()
		for i := 1; i <= 3; i++ {
			if err := stream.Send(ctx, bidiSegment("tail", i, 96), nil); err != nil {
				return err
			}
		}
		return stream.CloseSend(ctx)
	})
	client := dialTestClient(t, server, ClientConfig{})

	stream, err := client.BidiStream(context.Background(), "burst", testPayload{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Read the whole tail, then cancel while the server's half-close is
	// still in flight or buffered.
	for i := 1; i <= 3; i++ {
		payload, _, err := stream.Recv(context.Background())
		if err != nil {
			t.Fatalf("recv %d: %v", i, err)
		}
		checkBidiSegment(t, "tail", i, 96, payload)
	}
	// The server finished its side; wait until its half-close has
	// actually arrived and is buffered on the client.
	select {
	case st := <-serverDone:
		if st.Sent != 3 || st.Acked != 3 {
			t.Fatalf("server final state: %+v", st)
		}
	case <-time.After(time.Second):
		t.Fatal("server handler did not finish")
	}
	deadline := time.Now().Add(2 * time.Second)
	for !stream.State().RecvEnd {
		if time.Now().After(deadline) {
			t.Fatal("server half-close never arrived")
		}
		time.Sleep(5 * time.Millisecond)
	}
	stream.Cancel()
	if state := stream.State(); !state.Done || !errors.Is(state.Err, context.Canceled) || state.Received != 3 {
		t.Fatalf("canceled state: %+v", state)
	}
	// The buffered end-of-stream tail must still be delivered, not lost
	// behind the cancellation.
	if _, _, err := stream.Recv(context.Background()); err != io.EOF {
		t.Fatalf("buffered tail end = %v, want io.EOF", err)
	}
	// The cancel stays scoped to its own stream.
	reply, _, err := Call[testPayload, testReply](context.Background(), client, "test", testPayload{Value: "after"}, nil)
	if err != nil || reply.Value != "unary:after" {
		t.Fatalf("unary after cancel: %+v, %v", reply, err)
	}
	other, err := client.BidiStream(context.Background(), "echo", testPayload{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := other.Send(context.Background(), bidiSegment("ok", 1, 32), nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := other.Recv(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := other.CloseSend(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := other.Recv(context.Background()); err != io.EOF {
		t.Fatalf("other stream final recv = %v", err)
	}
}
