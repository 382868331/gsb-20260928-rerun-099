package localrpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
)

type Client struct {
	codec          Codec
	maxMessageSize int
	maxPending     int

	mu      sync.Mutex
	conn    net.Conn
	connID  uint64
	closed  bool
	nextID  uint64
	pending map[uint64]pendingCall
	writeMu sync.Mutex
}

type pendingCall struct {
	connID uint64
	done   chan Message
}

type ClientOption func(*Client)

func WithClientCodec(codec Codec) ClientOption {
	return func(c *Client) { c.codec = codec }
}

func WithClientMaxMessageSize(size int) ClientOption {
	return func(c *Client) { c.maxMessageSize = size }
}

func WithClientMaxPending(size int) ClientOption {
	return func(c *Client) { c.maxPending = size }
}

func Dial(ctx context.Context, address string, options ...ClientOption) (*Client, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	return NewClient(conn, options...), nil
}

func NewClient(conn net.Conn, options ...ClientOption) *Client {
	c := &Client{
		codec:          JSONCodec{},
		maxMessageSize: DefaultMaxMessageSize,
		maxPending:     DefaultMaxPending,
		nextID:         1,
		pending:        make(map[uint64]pendingCall),
	}
	for _, option := range options {
		option(c)
	}
	c.attachConn(conn)
	return c
}

func (c *Client) attachConn(conn net.Conn) uint64 {
	c.mu.Lock()
	c.connID++
	id := c.connID
	c.conn = conn
	c.closed = false
	c.mu.Unlock()
	go c.readLoop(conn, id)
	return id
}

func (c *Client) Call(ctx context.Context, service, method string, payload []byte, headers map[string]string) ([]byte, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrConnClosed
	}
	if len(c.pending) >= c.maxPending {
		c.mu.Unlock()
		return nil, ErrTooManyPending
	}
	id := c.nextID
	c.nextID++
	connID := c.connID
	conn := c.conn
	ch := make(chan Message, 1)
	c.pending[id] = pendingCall{connID: connID, done: ch}
	c.mu.Unlock()

	cleanup := func() {
		c.mu.Lock()
		if call, ok := c.pending[id]; ok && call.connID == connID {
			delete(c.pending, id)
		}
		c.mu.Unlock()
	}

	request := Message{ID: id, Service: service, Method: method, Headers: headers, Payload: payload}
	c.writeMu.Lock()
	err := WriteFrame(conn, request, FrameRequest, c.codec, c.maxMessageSize)
	c.writeMu.Unlock()
	if err != nil {
		cleanup()
		c.failConn(connID, conn, err)
		return nil, err
	}

	select {
	case <-ctx.Done():
		cancel := Message{ID: id}
		c.writeMu.Lock()
		_ = WriteFrame(conn, cancel, FrameCancel, c.codec, c.maxMessageSize)
		c.writeMu.Unlock()
		cleanup()
		return nil, ctx.Err()
	case response := <-ch:
		if response.Error != nil {
			return nil, response.Error
		}
		return response.Payload, nil
	}
}

func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	conn := c.conn
	id := c.connID
	c.closed = true
	c.mu.Unlock()
	c.failConn(id, conn, ErrConnClosed)
	if conn == nil {
		return nil
	}
	return conn.Close()
}

func (c *Client) readLoop(conn net.Conn, connID uint64) {
	for {
		msg, frameType, err := ReadFrame(conn, c.codec, c.maxMessageSize)
		if err != nil {
			c.failConn(connID, conn, err)
			return
		}
		if frameType != FrameResponse {
			c.failConn(connID, conn, fmt.Errorf("%w: unexpected frame type %d", ErrBadFrame, frameType))
			return
		}
		c.deliver(connID, msg)
	}
}

func (c *Client) deliver(connID uint64, msg Message) {
	c.mu.Lock()
	call, ok := c.pending[msg.ID]
	matched := ok && call.connID == connID
	if matched {
		delete(c.pending, msg.ID)
	}
	c.mu.Unlock()
	if matched {
		select {
		case call.done <- msg:
		default:
		}
	}
}

func (c *Client) failConn(connID uint64, conn net.Conn, cause error) {
	if errors.Is(cause, ErrBadFrame) || errors.Is(cause, ErrFrameTooLarge) {
		cause = fmt.Errorf("%w: %v", ErrConnClosed, cause)
	}
	c.mu.Lock()
	if c.conn == conn && c.connID == connID {
		c.closed = true
	}
	var calls []pendingCall
	for id, call := range c.pending {
		if call.connID == connID {
			delete(c.pending, id)
			calls = append(calls, call)
		}
	}
	c.mu.Unlock()

	for _, call := range calls {
		response := Message{Error: &RemoteError{Code: CodeUnavailable, Message: cause.Error()}}
		select {
		case call.done <- response:
		default:
		}
	}
	_ = conn.Close()
}

func (c *Client) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending)
}
