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

func TestBidiSnapshotsCompareConfirmationsAndSurviveDisconnect(t *testing.T) {
	const streamCount = 3
	firstSizes := []int{1, 4096, 100000}
	secondSizes := []int{512, 17, 8192}
	thirdSizes := []int{3, 64, 256}
	firstReceived := make(chan struct{}, streamCount)
	releaseSecond := make(chan struct{})
	secondReceived := make(chan struct{}, streamCount)
	serverSnapshots := make(chan StreamSnapshot, streamCount)

	server := NewServer(ServerConfig{Address: "pipe", MaxHandlers: 16, StreamCredits: 4})
	server.HandleBidi("snapshot", func(ctx context.Context, _ []byte, _ map[string]string, stream *ServerBidiStream) error {
		if _, _, err := stream.Recv(ctx); err != nil {
			return err
		}
		firstReceived <- struct{}{}
		<-releaseSecond
		if _, _, err := stream.Recv(ctx); err != nil {
			return err
		}
		serverSnapshots <- stream.Snapshot()
		secondReceived <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	})
	t.Cleanup(func() { _ = server.Close() })
	client := dialTestClient(t, server, ClientConfig{StreamCredits: 4})

	streams := make([]*BidiStream, streamCount)
	before := make([]StreamSnapshot, streamCount)
	for i := range streams {
		stream, err := client.BidiStream(context.Background(), "snapshot", testPayload{Value: fmt.Sprintf("snapshot-%d", i)}, nil)
		if err != nil {
			t.Fatal(err)
		}
		streams[i] = stream
		if err := stream.Send(context.Background(), bidiSegment("snapshot", 1, firstSizes[i]), nil); err != nil {
			t.Fatalf("stream %d first send: %v", i, err)
		}
		<-firstReceived
		before[i] = stream.Snapshot()
		if before[i].Sent != 1 || before[i].Acked != 1 || before[i].SegmentSizes[1] != len(bidiSegment("snapshot", 1, firstSizes[i])) {
			t.Fatalf("stream %d first snapshot: %+v", i, before[i])
		}
	}

	sendErr := make(chan error, streamCount*2)
	for i, stream := range streams {
		go func(i int, stream *BidiStream) {
			sendErr <- stream.Send(context.Background(), bidiSegment("snapshot", 2, secondSizes[i]), nil)
		}(i, stream)
	}
	deadline := time.After(time.Second)
	for {
		held := streams[0].Snapshot()
		for _, stream := range streams {
			held = stream.Snapshot()
		}
		if held.Connection.Credits.Used == streamCount && held.Connection.Credits.Available == 1 {
			break
		}
		select {
		case <-deadline:
			snap := streams[0].Snapshot()
			t.Fatalf("shared credits = %+v, want used=%d available=1", snap.Connection.Credits, streamCount)
		default:
			time.Sleep(time.Millisecond)
		}
	}
	for i, stream := range streams {
		held := stream.Snapshot()
		pending, ok := held.PendingRange()
		if !ok || pending != (SeqRange{Start: 2, End: 2}) || held.Connection.Credits.Limit != 4 || held.Connection.Credits.Used != streamCount {
			t.Fatalf("stream %d held snapshot: %+v", i, held)
		}
	}

	close(releaseSecond)
	for i := 0; i < streamCount; i++ {
		if err := <-sendErr; err != nil {
			t.Fatalf("second send: %v", err)
		}
		<-secondReceived
	}
	for i, stream := range streams {
		after := stream.Snapshot()
		diff := CompareStreamSnapshots(before[i], after)
		if !diff.HasChange || diff.NewlyConfirmed != (SeqRange{Start: 2, End: 2}) || after.Acked != 2 {
			t.Fatalf("stream %d confirmation diff: %+v", i, diff)
		}
		if after.SegmentSizes[2] != len(bidiSegment("snapshot", 2, secondSizes[i])) {
			t.Fatalf("stream %d second size: %+v", i, after.SegmentSizes)
		}
	}
	for i := 0; i < streamCount; i++ {
		serverSnap := <-serverSnapshots
		if serverSnap.Method != "snapshot" || serverSnap.StreamID == 0 || serverSnap.Received != 2 {
			t.Fatalf("server snapshot not correlated: %+v", serverSnap)
		}
	}

	for i, stream := range streams {
		go func(i int, stream *BidiStream) {
			sendErr <- stream.Send(context.Background(), bidiSegment("snapshot", 3, thirdSizes[i]), nil)
		}(i, stream)
	}
	time.Sleep(50 * time.Millisecond)
	old := client.session
	old.close(ErrConnectionClosed)
	for i, stream := range streams {
		if err := <-sendErr; err != nil && !errors.Is(err, ErrConnectionClosed) {
			t.Fatalf("third send: %v", err)
		}
		finalSnap := stream.Snapshot()
		if !finalSnap.Done || !errors.Is(finalSnap.CloseReason, ErrConnectionClosed) || !finalSnap.Connection.Closed {
			t.Fatalf("stream %d final close snapshot: %+v", i, finalSnap)
		}
		pending, ok := finalSnap.PendingRange()
		if !ok || pending != (SeqRange{Start: 3, End: 3}) || finalSnap.Acked != 2 || finalSnap.Sent != 3 {
			t.Fatalf("stream %d final ranges: %+v", i, finalSnap)
		}
		if CompareStreamSnapshots(before[i], finalSnap).NewlyConfirmed != (SeqRange{Start: 2, End: 2}) {
			t.Fatalf("final snapshot lost previously confirmed range: %+v", finalSnap)
		}
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
