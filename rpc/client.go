package rpc

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
)

type ClientConfig struct {
	Address      string
	MaxFrameSize int
	MaxPending   int
	WriteQueue   int
	Headers      map[string]string
	Codec        PayloadCodec
}

type Client struct {
	cfg     ClientConfig
	mu      sync.Mutex
	session *clientSession
	closed  bool
}

type CallResult struct {
	Payload []byte
	Headers map[string]string
}

type clientSession struct {
	cfg       ClientConfig
	conn      net.Conn
	reader    *bufio.Reader
	writeCh   chan []byte
	cancelCh  chan []byte
	pending   map[uint64]*pendingCall
	nextID    atomic.Uint64
	mu        sync.Mutex
	closed    chan struct{}
	closeOnce sync.Once
}

type pendingCall struct {
	id        uint64
	method    string
	ch        chan *Envelope
	done      chan struct{}
	stream    bool
	closeOnce sync.Once
}

type Stream struct {
	session *clientSession
	call    *pendingCall
}

func (s *clientSession) cfgRef() ClientConfig { return s.cfg }

func newClientWithConn(conn net.Conn, cfg ClientConfig) *Client {
	cfg = withDefaults(cfg)
	c := &Client{cfg: cfg}
	c.session = c.newSession(conn)
	return c
}

func Dial(ctx context.Context, cfg ClientConfig) (*Client, error) {
	cfg = withDefaults(cfg)
	c := &Client{cfg: cfg}
	if _, err := c.connectLocked(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

func NewLazyClient(cfg ClientConfig) *Client {
	return &Client{cfg: withDefaults(cfg)}
}

func withDefaults(cfg ClientConfig) ClientConfig {
	if cfg.MaxFrameSize <= 0 {
		cfg.MaxFrameSize = defaultMaxFrameSize
	}
	if cfg.MaxPending <= 0 {
		cfg.MaxPending = 1024
	}
	if cfg.WriteQueue <= 0 {
		cfg.WriteQueue = 1024
	}
	if cfg.Codec == nil {
		cfg.Codec = JSONCodec{}
	}
	return cfg
}

func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	s := c.session
	c.session = nil
	c.mu.Unlock()
	if s != nil {
		s.close(ErrConnectionClosed)
	}
	return nil
}

func (c *Client) Call(ctx context.Context, method string, arg any, reply any, headers map[string]string) (map[string]string, error) {
	payload, err := c.cfg.Codec.Encode(arg)
	if err != nil {
		return nil, err
	}
	if len(payload) > c.cfg.MaxFrameSize {
		return nil, ErrMessageTooLarge
	}
	merged := mergeHeaders(c.cfg.Headers, headers)
	env := &Envelope{Type: TypeRequest, Method: method, Payload: payload, Headers: merged, Serializer: c.cfg.Codec.ID()}
	if deadline, ok := ctx.Deadline(); ok {
		if env.Headers == nil {
			env.Headers = map[string]string{}
		}
		env.Headers["x-lrpc-deadline-unix-nano"] = formatDeadline(deadline)
	}

	s, err := c.sessionForCall(ctx)
	if err != nil {
		return nil, err
	}
	result, err := s.send(ctx, env, c.cfg)
	if err != nil {
		return nil, err
	}
	if reply != nil {
		if err := c.cfg.Codec.Decode(result.Payload, reply); err != nil {
			return result.Headers, err
		}
	}
	return result.Headers, nil
}

func (c *Client) Stream(ctx context.Context, method string, arg any, headers map[string]string) (*Stream, error) {
	payload, err := c.cfg.Codec.Encode(arg)
	if err != nil {
		return nil, err
	}
	if len(payload) > c.cfg.MaxFrameSize {
		return nil, ErrMessageTooLarge
	}
	merged := mergeHeaders(c.cfg.Headers, headers)
	env := &Envelope{Type: TypeRequest, Flags: FlagStream, Method: method, Payload: payload, Headers: merged, Serializer: c.cfg.Codec.ID()}
	if deadline, ok := ctx.Deadline(); ok {
		if env.Headers == nil {
			env.Headers = map[string]string{}
		}
		env.Headers["x-lrpc-deadline-unix-nano"] = formatDeadline(deadline)
	}
	s, err := c.sessionForCall(ctx)
	if err != nil {
		return nil, err
	}
	call, err := s.begin(ctx, env, c.cfg, true)
	if err != nil {
		return nil, err
	}
	return &Stream{session: s, call: call}, nil
}

func (s *Stream) Recv(ctx context.Context) ([]byte, map[string]string, error) {
	select {
	case <-ctx.Done():
		s.session.cancel(s.call.id, s.call.method, s.session.cfgRef())
		s.session.remove(s.call.id)
		return nil, nil, ctx.Err()
	case <-s.session.closed:
		return nil, nil, ErrConnectionClosed
	case <-s.call.done:
		return nil, nil, ErrConnectionClosed
	case env := <-s.call.ch:
		if env == nil {
			return nil, nil, ErrConnectionClosed
		}
		switch env.Type {
		case TypeStream:
			if env.Flags&FlagStream != 0 {
				s.session.remove(s.call.id)
				return env.Payload, env.Headers, io.EOF
			}
			s.session.ack(s.call.id, s.call.method, s.session.cfg)
			return env.Payload, env.Headers, nil
		case TypeResponse:
			s.session.remove(s.call.id)
			return env.Payload, env.Headers, io.EOF
		case TypeError:
			s.session.remove(s.call.id)
			return nil, env.Headers, &CallError{Message: env.Error}
		}
	}
	return nil, nil, ErrBadFrame
}

func (s *Stream) Close() error {
	s.session.cancel(s.call.id, s.call.method, s.session.cfgRef())
	s.session.remove(s.call.id)
	return nil
}

func (c *Client) sessionForCall(ctx context.Context) (*clientSession, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrConnectionClosed
	}
	if c.session != nil {
		s := c.session
		c.mu.Unlock()
		select {
		case <-s.closed:
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return s, nil
		}
	} else {
		c.mu.Unlock()
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrConnectionClosed
	}
	if c.session != nil {
		select {
		case <-c.session.closed:
		default:
			return c.session, nil
		}
	}
	return c.connectLocked(ctx)
}

func (c *Client) connectLocked(ctx context.Context) (*clientSession, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", c.cfg.Address)
	if err != nil {
		return nil, err
	}
	s := c.newSession(conn)
	c.session = s
	return s, nil
}

func (c *Client) newSession(conn net.Conn) *clientSession {
	s := &clientSession{
		cfg:      c.cfg,
		conn:     conn,
		reader:   bufio.NewReader(conn),
		writeCh:  make(chan []byte, c.cfg.WriteQueue),
		cancelCh: make(chan []byte, 8),
		pending:  make(map[uint64]*pendingCall),
		closed:   make(chan struct{}),
	}
	s.nextID.Store(fastrandID())
	go s.writeLoop(c.cfg.MaxFrameSize)
	go s.readLoop(c.cfg.MaxFrameSize)
	return s
}

func (s *clientSession) send(ctx context.Context, env *Envelope, cfg ClientConfig) (*Envelope, error) {
	call, err := s.begin(ctx, env, cfg, false)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		s.cancel(call.id, env.Method, cfg)
		s.remove(call.id)
		return nil, ctx.Err()
	case <-call.done:
		return nil, ErrConnectionClosed
	case response := <-call.ch:
		if response == nil {
			return nil, ErrConnectionClosed
		}
		if response.Type == TypeError {
			return nil, &CallError{Message: response.Error}
		}
		return response, nil
	}
}

func (s *clientSession) begin(ctx context.Context, env *Envelope, cfg ClientConfig, stream bool) (*pendingCall, error) {
	s.mu.Lock()
	select {
	case <-s.closed:
		s.mu.Unlock()
		return nil, ErrConnectionClosed
	default:
	}
	if len(s.pending) >= cfg.MaxPending {
		s.mu.Unlock()
		return nil, ErrBusy
	}
	id := s.nextID.Add(1)
	env.ID = id
	chSize := 1
	if stream {
		chSize = 0
	}
	call := &pendingCall{id: id, method: env.Method, ch: make(chan *Envelope, chSize), done: make(chan struct{}), stream: stream}
	s.pending[id] = call
	s.mu.Unlock()

	frame, err := EncodeEnvelope(env, cfg.MaxFrameSize)
	if err != nil {
		s.remove(id)
		return nil, err
	}
	if err := s.enqueue(frame); err != nil {
		s.remove(id)
		return nil, err
	}
	return call, nil
}

func (s *clientSession) enqueue(frame []byte) error {
	select {
	case <-s.closed:
		return ErrConnectionClosed
	default:
	}
	select {
	case s.writeCh <- frame:
		return nil
	case <-s.closed:
		return ErrConnectionClosed
	}
}

func (s *clientSession) cancel(id uint64, method string, cfg ClientConfig) {
	frame, err := EncodeEnvelope(&Envelope{Type: TypeCancel, ID: id, Method: method, Serializer: cfg.Codec.ID()}, cfg.MaxFrameSize)
	if err == nil {
		select {
		case s.cancelCh <- frame:
		default:
		}
	}
}

func (s *clientSession) ack(id uint64, method string, cfg ClientConfig) {
	frame, err := EncodeEnvelope(&Envelope{Type: TypeAck, ID: id, Method: method, Serializer: cfg.Codec.ID()}, cfg.MaxFrameSize)
	if err == nil {
		select {
		case s.cancelCh <- frame:
		default:
		}
	}
}

func (s *clientSession) remove(id uint64) {
	s.mu.Lock()
	if call, ok := s.pending[id]; ok {
		delete(s.pending, id)
		call.closeOnce.Do(func() { close(call.done) })
	}
	s.mu.Unlock()
}

func (s *clientSession) readLoop(maxSize int) {
	defer s.close(ErrConnectionClosed)
	for {
		frame, err := ReadFrame(s.reader, maxSize, nil)
		if err != nil {
			return
		}
		env, err := DecodeEnvelope(frame)
		if err != nil {
			if errors.Is(err, ErrBadFrame) || errors.Is(err, ErrMessageTooLarge) {
				return
			}
			return
		}
		if env.Type != TypeResponse && env.Type != TypeError && env.Type != TypeStream {
			continue
		}
		s.mu.Lock()
		call := s.pending[env.ID]
		s.mu.Unlock()
		if call == nil {
			continue
		}
		select {
		case call.ch <- env:
		case <-call.done:
		case <-s.closed:
			return
		}
	}
}

func (s *clientSession) writeLoop(maxSize int) {
	defer s.close(ErrConnectionClosed)
	for {
		writeCh := s.writeCh
		select {
		case frame := <-s.cancelCh:
			if err := WriteFrame(s.conn, maxSize, frame); err != nil {
				return
			}
			continue
		default:
		}
		select {
		case frame := <-s.cancelCh:
			if err := WriteFrame(s.conn, maxSize, frame); err != nil {
				return
			}
		case frame := <-writeCh:
			if err := WriteFrame(s.conn, maxSize, frame); err != nil {
				return
			}
		case <-s.closed:
			return
		}
	}
}

func (s *clientSession) close(err error) {
	s.closeOnce.Do(func() {
		close(s.closed)
		_ = s.conn.Close()
		s.mu.Lock()
		for id, call := range s.pending {
			delete(s.pending, id)
			call.closeOnce.Do(func() { close(call.done) })
		}
		s.mu.Unlock()
	})
}
