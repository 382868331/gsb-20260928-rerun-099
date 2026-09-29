package localrpc

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"
)

func startTestServer(t *testing.T, options ...ServerOption) (*Server, net.Listener) {
	t.Helper()
	listener := newPipeListener()
	server := NewServer(options...)
	server.Handle("test", "echo", func(_ context.Context, payload []byte) ([]byte, error) {
		return payload, nil
	})
	server.Handle("test", "delay", func(ctx context.Context, payload []byte) ([]byte, error) {
		duration, err := time.ParseDuration(string(payload))
		if err != nil {
			return nil, err
		}
		timer := time.NewTimer(duration)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return []byte("done:" + duration.String()), nil
		}
	})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		_ = listener.Close()
	})
	return server, listener
}

func dialTestClient(t *testing.T, listener net.Listener, options ...ClientOption) *Client {
	t.Helper()
	clientConn, err := listener.(*pipeListener).Dial()
	if err != nil {
		t.Fatal(err)
	}
	client := NewClient(clientConn, options...)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestInterleavedRequestsDoNotCrossResults(t *testing.T) {
	_, listener := startTestServer(t)
	client := dialTestClient(t, listener)

	count := 50
	results := make(chan string, count)
	var wg sync.WaitGroup
	for i := range count {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payload := []byte(strconv.Itoa(i))
			result, err := client.Call(context.Background(), "test", "echo", payload, nil)
			if err != nil {
				t.Errorf("request %d: %v", i, err)
				return
			}
			results <- string(result)
		}(i)
	}
	wg.Wait()
	close(results)

	seen := make(map[string]bool)
	for result := range results {
		if seen[result] {
			t.Fatalf("duplicate result %q", result)
		}
		seen[result] = true
	}
	if len(seen) != count {
		t.Fatalf("got %d results, want %d", len(seen), count)
	}
}

func TestCancelStopsClientAndServerWait(t *testing.T) {
	blocked := make(chan struct{})
	canceled := make(chan struct{})
	server, listener := startTestServer(t)
	server.Handle("test", "block", func(ctx context.Context, _ []byte) ([]byte, error) {
		close(blocked)
		<-ctx.Done()
		close(canceled)
		return nil, ctx.Err()
	})
	client := dialTestClient(t, listener)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := client.Call(ctx, "test", "block", nil, nil)
		errCh <- err
	}()
	waitForSignal(t, blocked, "handler started")
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("client error = %v, want context.Canceled", err)
	}
	waitForSignal(t, canceled, "handler canceled")

	result, err := client.Call(context.Background(), "test", "echo", []byte("still-usable"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != "still-usable" {
		t.Fatalf("result = %q", result)
	}
}

func TestTimeoutAndConnectionReuse(t *testing.T) {
	_, listener := startTestServer(t)
	client := dialTestClient(t, listener)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := client.Call(ctx, "test", "delay", []byte("200ms"), nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want deadline exceeded", err)
	}

	result, err := client.Call(context.Background(), "test", "echo", []byte("reused"), nil)
	if err != nil {
		t.Fatalf("connection not reused after timeout: %v", err)
	}
	if string(result) != "reused" {
		t.Fatalf("result = %q", result)
	}
}

func TestConnectionBreakFailsInflightCalls(t *testing.T) {
	server, listener := startTestServer(t)
	server.Handle("test", "block2", func(ctx context.Context, _ []byte) ([]byte, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	client := dialTestClient(t, listener)

	errCh := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := client.Call(context.Background(), "test", "block2", nil, nil)
			errCh <- err
		}()
	}
	time.Sleep(20 * time.Millisecond)
	client.mu.Lock()
	conn := client.conn
	client.mu.Unlock()
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}

	for range 2 {
		select {
		case err := <-errCh:
			if err == nil {
				t.Fatal("broken call returned nil")
			}
		case <-time.After(time.Second):
			t.Fatal("broken call did not return")
		}
	}

	_, err := client.Call(context.Background(), "test", "echo", nil, nil)
	if !errors.Is(err, ErrConnClosed) {
		t.Fatalf("new call error = %v, want ErrConnClosed", err)
	}
}

func TestLateResponseFromOldConnectionCannotMatchNewCall(t *testing.T) {
	oldClientConn, oldServerConn := net.Pipe()
	newClientConn, newServerConn := net.Pipe()
	client := NewClient(oldClientConn)
	oldConnID := client.connID
	newConnID := client.attachConn(newClientConn)
	newDone := make(chan Message, 1)
	client.mu.Lock()
	client.pending[1] = pendingCall{connID: newConnID, done: newDone}
	client.mu.Unlock()

	_ = WriteFrame(oldServerConn, Message{ID: 1, Payload: []byte("old-late")}, FrameResponse, nil, DefaultMaxMessageSize)
	time.Sleep(50 * time.Millisecond)
	if client.Pending() != 1 {
		t.Fatalf("old connection response matched a new call; pending=%d", client.Pending())
	}
	_ = WriteFrame(newServerConn, Message{ID: 1, Payload: []byte("new-result")}, FrameResponse, nil, DefaultMaxMessageSize)

	select {
	case response := <-newDone:
		if string(response.Payload) != "new-result" {
			t.Fatalf("new call got %q", response.Payload)
		}
	case <-time.After(time.Second):
		t.Fatal("new call did not receive response")
	}
	if newConnID == oldConnID {
		t.Fatal("old connection id reused")
	}
	_ = oldServerConn.Close()
	_ = newServerConn.Close()
}

type pipeListener struct {
	connections chan net.Conn
	closed      chan struct{}
}

func newPipeListener() *pipeListener {
	return &pipeListener{
		connections: make(chan net.Conn),
		closed:      make(chan struct{}),
	}
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
func (pipeAddr) String() string  { return "pipe" }

func TestMessageSizeLimit(t *testing.T) {
	maxSize := 64
	_, listener := startTestServer(t, WithServerMaxMessageSize(maxSize))
	client := dialTestClient(t, listener, WithClientMaxMessageSize(maxSize))
	_, err := client.Call(context.Background(), "test", "echo", bytes.Repeat([]byte("x"), maxSize), nil)
	if !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("client error = %v, want message too large", err)
	}
}

func TestServerQueueLimit(t *testing.T) {
	_, listener := startTestServer(t, WithServerMaxQueue(1))
	client := dialTestClient(t, listener, WithClientMaxPending(10))

	errs := make(chan error, 3)
	for range 3 {
		go func() {
			_, err := client.Call(context.Background(), "test", "delay", []byte("2s"), nil)
			errs <- err
		}()
	}
	queueFull := 0
	for range 3 {
		err := <-errs
		var remoteErr *RemoteError
		if errors.As(err, &remoteErr) && remoteErr.Code == CodeQueueFull {
			queueFull++
		}
	}
	if queueFull == 0 {
		t.Fatal("expected at least one queue full response")
	}
}

func TestBadFrameOnlyClosesAffectedConnection(t *testing.T) {
	_, listener := startTestServer(t)
	client := dialTestClient(t, listener)
	client.mu.Lock()
	conn := client.conn
	client.mu.Unlock()
	if _, err := conn.Write([]byte{0, 1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)

	client2 := dialTestClient(t, listener)
	result, err := client2.Call(context.Background(), "test", "echo", []byte("alive"), nil)
	if err != nil {
		t.Fatalf("server process did not survive bad frame: %v", err)
	}
	if string(result) != "alive" {
		t.Fatalf("result = %q", result)
	}
}

func TestClientRejectsOversizedFrame(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	client := NewClient(clientConn, WithClientMaxMessageSize(32))
	go func() {
		header := make([]byte, 16)
		header[0], header[1], header[2], header[3] = frameMagic0, frameMagic1, frameVersion, FrameResponse
		header[4] = 0
		header[7] = 33
		_, _ = serverConn.Write(header)
		_, _ = io.Copy(io.Discard, serverConn)
	}()
	errCh := make(chan error, 1)
	go func() {
		_, err := client.Call(context.Background(), "test", "echo", []byte("x"), nil)
		errCh <- err
	}()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected oversized frame error")
		}
	case <-time.After(time.Second):
		t.Fatal("call did not fail")
	}
}

func waitForSignal(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}
