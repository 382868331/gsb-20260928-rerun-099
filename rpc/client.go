package rpc

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
)

type ClientConfig struct {
	Address      string
	MaxFrameSize int
	MaxPending   int
	WriteQueue   int
	StreamWindow int
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
	conn      net.Conn
	writeCh   chan []byte
	pending   map[uint64]chan *Envelope
	streams   map[uint64]*streamMailbox
	nextID    atomic.Uint64
	mu        sync.Mutex
	closed    chan struct{}
	closeOnce sync.Once
	writeMu   sync.Mutex
}

type pendingCall struct {
	done   chan *Envelope
	method string
}

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
	if cfg.StreamWindow <= 0 {
		cfg.StreamWindow = defaultStreamWindow
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

// DropConnection forcibly closes the current session, exactly like a network
// failure. Pending calls and streams end with ErrConnectionClosed; the next
// call opens a fresh session. It is useful for disconnect/reconnect drills.
func (c *Client) DropConnection() {
	c.mu.Lock()
	s := c.session
	c.session = nil
	c.mu.Unlock()
	if s != nil {
		s.close(ErrConnectionClosed)
	}
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

// OpenStream starts a server-streaming call. Fragments are received via the
// returned Stream. Canceling ctx or calling Stream.Cancel ends the call on
// both ends and releases unconsumed fragments.
func (c *Client) OpenStream(ctx context.Context, method string, arg any, headers map[string]string) (*Stream, error) {
	payload, err := c.cfg.Codec.Encode(arg)
	if err != nil {
		return nil, err
	}
	if len(payload) > c.cfg.MaxFrameSize {
		return nil, ErrMessageTooLarge
	}
	merged := mergeHeaders(c.cfg.Headers, headers)
	if merged == nil {
		merged = map[string]string{}
	}
	merged[HeaderStream] = strconv.Itoa(c.cfg.StreamWindow)
	env := &Envelope{Type: TypeRequest, Method: method, Payload: payload, Headers: merged, Serializer: c.cfg.Codec.ID()}
	if deadline, ok := ctx.Deadline(); ok {
		env.Headers["x-lrpc-deadline-unix-nano"] = formatDeadline(deadline)
	}

	s, err := c.sessionForCall(ctx)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	select {
	case <-s.closed:
		s.mu.Unlock()
		return nil, ErrConnectionClosed
	default:
	}
	if len(s.pending)+len(s.streams) >= c.cfg.MaxPending {
		s.mu.Unlock()
		return nil, ErrBusy
	}
	id := s.nextID.Add(1)
	env.ID = id
	mbox := newStreamMailbox()
	s.streams[id] = mbox
	s.mu.Unlock()

	frame, err := EncodeEnvelope(env, c.cfg.MaxFrameSize)
	if err != nil {
		s.removeStream(id)
		return nil, err
	}
	if err := s.enqueue(frame); err != nil {
		s.removeStream(id)
		return nil, err
	}

	stream := &Stream{s: s, id: id, method: method, cfg: c.cfg, mbox: mbox, codec: c.cfg.Codec, doneCh: make(chan struct{})}
	go func() {
		select {
		case <-ctx.Done():
			stream.Cancel()
		case <-s.closed:
			stream.fail(ErrConnectionClosed)
		case <-stream.doneCh:
		}
	}()
	return stream, nil
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
		conn:    conn,
		writeCh: make(chan []byte, c.cfg.WriteQueue),
		pending: make(map[uint64]chan *Envelope),
		streams: make(map[uint64]*streamMailbox),
		closed:  make(chan struct{}),
	}
	s.nextID.Store(fastrandID())
	go s.writeLoop(c.cfg.MaxFrameSize)
	go s.readLoop(c.cfg.MaxFrameSize)
	return s
}

func (s *clientSession) send(ctx context.Context, env *Envelope, cfg ClientConfig) (*Envelope, error) {
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
	// Streaming openings share the pending slot budget so call volume stays
	// bounded even before the first fragment arrives.
	id := s.nextID.Add(1)
	env.ID = id
	done := make(chan *Envelope, 1)
	s.pending[id] = done
	s.mu.Unlock()

	frame, err := EncodeEnvelope(env, cfg.MaxFrameSize)
	if err != nil {
		s.remove(id, nil)
		return nil, err
	}
	if err := s.enqueue(frame); err != nil {
		s.remove(id, nil)
		return nil, err
	}

	select {
	case <-ctx.Done():
		s.cancel(id, env.Method, cfg)
		s.remove(id, nil)
		return nil, ctx.Err()
	case env := <-done:
		if env == nil {
			return nil, ErrConnectionClosed
		}
		if env.Type == TypeError {
			return nil, &CallError{Message: env.Error}
		}
		return env, nil
	}
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
	if err != nil {
		return
	}
	// Write bypassing the queued data path: a cancel must leave even while a
	// slow-stream backlog still occupies the shared connection.
	s.writeControl(frame, cfg.MaxFrameSize)
}

func (s *clientSession) writeControl(frame []byte, maxSize int) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	select {
	case <-s.closed:
	default:
		_ = WriteFrame(s.conn, maxSize, frame)
	}
}

func (s *clientSession) remove(id uint64, env *Envelope) {
	s.mu.Lock()
	if ch, ok := s.pending[id]; ok {
		delete(s.pending, id)
		if env != nil {
			select {
			case ch <- env:
			default:
			}
		}
	}
	s.mu.Unlock()
}

func (s *clientSession) readLoop(maxSize int) {
	defer s.close(ErrConnectionClosed)
	var buf []byte
	for {
		frame, err := ReadFrame(s.conn, maxSize, buf)
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
		if env.Type == TypeStream {
			s.routeStream(env)
			continue
		}
		if env.Type != TypeResponse && env.Type != TypeError {
			continue
		}
		s.remove(env.ID, env)
	}
}

func (s *clientSession) routeStream(env *Envelope) {
	s.mu.Lock()
	mbox := s.streams[env.ID]
	s.mu.Unlock()
	if mbox == nil {
		return
	}
	terminal := env.Error != "" || (env.Headers != nil && (env.Headers[HeaderStreamEnd] != "" || env.Headers[HeaderStreamError] != ""))
	// Acks from the reader arrive here too.
	if env.Headers != nil && env.Headers[HeaderStreamAck] != "" {
		return
	}
	if terminal {
		mbox.offer(env)
	} else {
		mbox.offer(env)
	}
}

func (s *clientSession) removeStream(id uint64) {
	s.mu.Lock()
	delete(s.streams, id)
	s.mu.Unlock()
}

func (s *clientSession) writeLoop(maxSize int) {
	defer s.close(ErrConnectionClosed)
	for {
		select {
		case frame := <-s.writeCh:
			s.writeMu.Lock()
			err := WriteFrame(s.conn, maxSize, frame)
			s.writeMu.Unlock()
			if err != nil {
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
		for id, ch := range s.pending {
			delete(s.pending, id)
			select {
			case ch <- nil:
			default:
			}
		}
		for id, mbox := range s.streams {
			delete(s.streams, id)
			mbox.fail()
		}
		s.mu.Unlock()
	})
}
