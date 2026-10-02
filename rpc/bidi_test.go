package rpc

import (
	"bytes"
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

// echoBidi 逐段回显，并在 payload 内校验对方序号单调递增。
func echoBidi(ctx context.Context, _ []byte, _ map[string]string, stream *ServerBidiStream) error {
	for {
		data, err := stream.Recv(ctx)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(ctx, append([]byte("echo:"), data...), nil); err != nil {
			return err
		}
	}
}

func newBidiTestServer(t *testing.T) *Server {
	t.Helper()
	server := NewServer(ServerConfig{Address: "pipe", MaxHandlers: 16})
	server.HandleBidi("echo", echoBidi)
	server.Handle("test", func(_ context.Context, payload []byte, _ map[string]string) ([]byte, map[string]string, error) {
		return append([]byte("ok:"), payload...), nil, nil
	})
	t.Cleanup(func() { _ = server.Close() })
	return server
}

func segment(seed, size int) []byte {
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = byte(seed + i)
	}
	return buf
}

func TestBidiParallelStreamsVaryingSegmentsAndCalls(t *testing.T) {
	server := newBidiTestServer(t)
	client := dialTestClient(t, server, ClientConfig{})
	ctx := context.Background()

	const streamCount = 3
	const segCount = 12
	sizes := []int{1, 7, 64, 1023, 4096, 65536, 3, 32768, 100, 8192, 2, 16384}

	var wg sync.WaitGroup
	errCh := make(chan error, streamCount*2+8)
	for sIdx := 0; sIdx < streamCount; sIdx++ {
		stream, err := client.Bidi(ctx, "echo", nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func(sIdx int, stream *BidiStream) {
			defer wg.Done()
			for i := 0; i < segCount; i++ {
				payload := segment(sIdx*1000+i, sizes[(sIdx+i)%len(sizes)])
				if err := stream.Send(ctx, payload); err != nil {
					errCh <- fmt.Errorf("stream %d send %d: %w", sIdx, i, err)
					return
				}
				got, err := stream.Recv(ctx)
				if err != nil {
					errCh <- fmt.Errorf("stream %d recv %d: %w", sIdx, i, err)
					return
				}
				want := append([]byte("echo:"), payload...)
				if !bytes.Equal(got, want) {
					errCh <- fmt.Errorf("stream %d segment %d content mismatch (got %d bytes, want %d)", sIdx, i, len(got), len(want))
					return
				}
			}
			if err := stream.CloseSend(); err != nil {
				errCh <- fmt.Errorf("stream %d close send: %w", sIdx, err)
				return
			}
			if _, err := stream.Recv(ctx); err != io.EOF {
				errCh <- fmt.Errorf("stream %d final recv = %v, want EOF", sIdx, err)
			}
		}(sIdx, stream)
	}
	// 与流并行的普通短请求。
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payload := []byte(fmt.Sprintf("req-%d", i))
			got, err := client.Call(ctx, "test", payload, nil, nil)
			_ = got
			if err != nil {
				errCh <- fmt.Errorf("call %d: %w", i, err)
			}
		}(i)
	}
	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}
	// 所有流结束后，同一条连接上的普通请求仍然可用。
	if _, err := client.Call(ctx, "test", []byte("after"), nil, nil); err != nil {
		t.Fatalf("call after streams ended: %v", err)
	}
}

func TestBidiHalfCloseBothDirections(t *testing.T) {
	server := newBidiTestServer(t)
	serverFinished := make(chan struct{}, 1)
	server.HandleBidi("half", func(ctx context.Context, _ []byte, _ map[string]string, stream *ServerBidiStream) error {
		defer func() { serverFinished <- struct{}{} }()
		var got [][]byte
		for {
			data, err := stream.Recv(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			got = append(got, data)
		}
		if len(got) != 3 {
			return fmt.Errorf("server got %d segments, want 3", len(got))
		}
		// 客户端已半关闭，服务端继续发送两个分段再结束。
		if err := stream.Send(ctx, []byte("s0"), nil); err != nil {
			return err
		}
		return stream.Send(ctx, []byte("s1"), nil)
	})
	client := dialTestClient(t, server, ClientConfig{})
	ctx := context.Background()

	stream, err := client.Bidi(ctx, "half", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := stream.Send(ctx, []byte{byte('a' + i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(ctx, []byte("x")); !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("send after CloseSend = %v, want ErrStreamClosed", err)
	}
	// 接收方向不受影响：先收到 s0、s1，再收到 EOF。
	var got []string
	for {
		data, err := stream.Recv(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, string(data))
	}
	if strings.Join(got, ",") != "s0,s1" {
		t.Fatalf("got %v, want [s0 s1]", got)
	}
	// 再次 Recv 仍是 EOF；再次 CloseSend 是幂等的。
	if _, err := stream.Recv(ctx); err != io.EOF {
		t.Fatalf("recv after EOF = %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("second CloseSend = %v", err)
	}
	select {
	case <-serverFinished:
	case <-time.After(2 * time.Second):
		t.Fatal("server handler did not finish")
	}
	// 流全部收尾后连接仍可用于普通请求。
	if _, err := client.Call(ctx, "test", []byte("ping"), nil, nil); err != nil {
		t.Fatalf("call after half-close: %v", err)
	}
}

func TestBidiInterleavedCancelKeepsOthersAlive(t *testing.T) {
	server := newBidiTestServer(t)
	var canceled atomic.Int64
	server.HandleBidi("watch", func(ctx context.Context, _ []byte, _ map[string]string, stream *ServerBidiStream) error {
		if err := stream.Send(ctx, []byte("ready"), nil); err != nil {
			return err
		}
		<-ctx.Done()
		canceled.Add(1)
		return ctx.Err()
	})
	client := dialTestClient(t, server, ClientConfig{})
	ctx := context.Background()

	victim, err := client.Bidi(ctx, "watch", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := victim.Recv(ctx); err != nil {
		t.Fatal(err)
	}
	survivor, err := client.Bidi(ctx, "echo", nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	victim.Cancel()
	if _, err := victim.Recv(ctx); !errors.Is(err, ErrCanceled) {
		t.Fatalf("canceled recv = %v, want ErrCanceled", err)
	}
	if err := victim.Send(ctx, []byte("x")); !errors.Is(err, ErrCanceled) {
		t.Fatalf("canceled send = %v, want ErrCanceled", err)
	}

	// 另一条流与普通请求照常进行。
	if err := survivor.Send(ctx, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	got, err := survivor.Recv(ctx)
	if err != nil || string(got) != "echo:hello" {
		t.Fatalf("survivor echo = %q, %v", got, err)
	}
	if _, err := client.Call(ctx, "test", []byte("ping"), nil, nil); err != nil {
		t.Fatalf("call during cancel: %v", err)
	}
	if err := survivor.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := survivor.Recv(ctx); err != io.EOF {
		t.Fatalf("survivor final = %v, want EOF", err)
	}
	deadline := time.After(2 * time.Second)
	for canceled.Load() != 1 {
		select {
		case <-deadline:
			t.Fatal("server did not observe cancel")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func TestBidiSlowReceiverBackpressureAndNoStarvation(t *testing.T) {
	server := NewServer(ServerConfig{Address: "pipe", MaxHandlers: 8})
	t.Cleanup(func() { _ = server.Close() })
	release := make(chan struct{})
	server.HandleBidi("sink", func(ctx context.Context, _ []byte, _ map[string]string, stream *ServerBidiStream) error {
		<-release // 在放行前不接收，客户端窗口必然被占满
		for {
			if _, err := stream.Recv(ctx); err != nil {
				return nil
			}
		}
	})
	server.Handle("test", func(_ context.Context, payload []byte, _ map[string]string) ([]byte, map[string]string, error) {
		return payload, nil, nil
	})
	client := dialTestClient(t, server, ClientConfig{})
	ctx := context.Background()

	stream, err := client.Bidi(ctx, "sink", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	// 窗口内的分段立即返回。
	for i := 0; i < bidiWindow; i++ {
		if err := stream.Send(ctx, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	// 窗口已满：下一段必须阻塞，而不是无限积压。
	blocked := make(chan error, 1)
	go func() { blocked <- stream.Send(ctx, []byte("overflow")) }()
	select {
	case err := <-blocked:
		t.Fatalf("send beyond window returned %v, want blocked", err)
	case <-time.After(100 * time.Millisecond):
	}
	// 阻塞期间短请求不被饿死。
	callDone := make(chan error, 1)
	go func() {
		_, err := client.Call(ctx, "test", []byte("fast"), nil, nil)
		callDone <- err
	}()
	select {
	case err := <-callDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("short call starved by blocked stream")
	}
	// 接收方放行后，阻塞的发送得以完成。
	close(release)
	select {
	case err := <-blocked:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("send still blocked after receiver resumed")
	}
	stream.Cancel()
}

func TestBidiDisconnectReportsAckedAndUndetermined(t *testing.T) {
	server := NewServer(ServerConfig{Address: "pipe", MaxHandlers: 8})
	t.Cleanup(func() { _ = server.Close() })
	var calls atomic.Int64
	server.HandleBidi("sink", func(ctx context.Context, _ []byte, _ map[string]string, stream *ServerBidiStream) error {
		calls.Add(1)
		// 只确认前两个分段，之后不再接收。
		for i := 0; i < 2; i++ {
			if _, err := stream.Recv(ctx); err != nil {
				return err
			}
		}
		<-ctx.Done()
		return ctx.Err()
	})
	server.Handle("test", func(ctx context.Context, payload []byte, _ map[string]string) ([]byte, map[string]string, error) {
		calls.Add(1)
		<-ctx.Done()
		return nil, nil, ctx.Err()
	})
	clientConn, serverConn := net.Pipe()
	serveTestConn(server, serverConn)
	client := newClientWithConn(clientConn, ClientConfig{})
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()

	stream, err := client.Bidi(ctx, "sink", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := stream.Send(ctx, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	// 等前两个分段的确认到达客户端。
	deadline := time.Now().Add(2 * time.Second)
	for stream.Outcome().AckedUpTo != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("outcome = %+v, want AckedUpTo 1", stream.Outcome())
		}
		time.Sleep(5 * time.Millisecond)
	}
	// 再发两个分段：进入服务端缓冲但不会被确认。
	if err := stream.Send(ctx, []byte("seg2")); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(ctx, []byte("seg3")); err != nil {
		t.Fatal(err)
	}
	// 一个在途普通请求，连接断开时必须失败而不是被静默重放。
	callErr := make(chan error, 1)
	go func() {
		_, err := client.Call(ctx, "test", []byte("side-effect"), nil, nil)
		callErr <- err
	}()
	time.Sleep(50 * time.Millisecond)

	_ = serverConn.Close()
	select {
	case err := <-callErr:
		if !errors.Is(err, ErrConnectionClosed) {
			t.Fatalf("in-flight call = %v, want ErrConnectionClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight call did not fail after disconnect")
	}
	select {
	case err := <-func() chan error {
		ch := make(chan error, 1)
		go func() { _, err := stream.Recv(ctx); ch <- err }()
		return ch
	}():
		if !errors.Is(err, ErrConnectionClosed) {
			t.Fatalf("stream recv = %v, want ErrConnectionClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stream recv did not fail after disconnect")
	}
	outcome := stream.Outcome()
	if outcome.AckedUpTo != 1 {
		t.Fatalf("AckedUpTo = %d, want 1", outcome.AckedUpTo)
	}
	if len(outcome.Undetermined) != 2 || outcome.Undetermined[0] != 2 || outcome.Undetermined[1] != 3 {
		t.Fatalf("Undetermined = %v, want [2 3]", outcome.Undetermined)
	}
	// 断开不会触发任何静默重放：每个处理器只被调用一次。
	if got := calls.Load(); got != 2 {
		t.Fatalf("handler invocations = %d, want 2 (no replay)", got)
	}
}
