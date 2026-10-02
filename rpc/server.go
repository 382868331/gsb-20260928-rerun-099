package rpc

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

type RawHandler func(ctx context.Context, payload []byte, headers map[string]string) ([]byte, map[string]string, error)
type RawStreamHandler func(ctx context.Context, payload []byte, headers map[string]string, stream *ServerStream) error
type RawBidiHandler func(ctx context.Context, payload []byte, headers map[string]string, stream *ServerBidiStream) error

type Authenticator func(headers map[string]string) error

type ServerConfig struct {
	Address       string
	MaxFrameSize  int
	WriteQueue    int
	MaxHandlers   int
	Codec         PayloadCodec
	Authenticate  Authenticator
	HeaderTimeout time.Duration
}

type Server struct {
	cfg            ServerConfig
	mu             sync.RWMutex
	handlers       map[string]RawHandler
	streamHandlers map[string]RawStreamHandler
	bidiHandlers   map[string]RawBidiHandler
	listener       net.Listener
	closed         chan struct{}
	closeMu        sync.Mutex
	conns          sync.WaitGroup
	activeMu       sync.Mutex
	active         map[net.Conn]struct{}
}

func NewServer(cfg ServerConfig) *Server {
	if cfg.MaxFrameSize <= 0 {
		cfg.MaxFrameSize = defaultMaxFrameSize
	}
	if cfg.WriteQueue <= 0 {
		cfg.WriteQueue = 1024
	}
	if cfg.MaxHandlers <= 0 {
		cfg.MaxHandlers = 1024
	}
	if cfg.Codec == nil {
		cfg.Codec = JSONCodec{}
	}
	if cfg.HeaderTimeout <= 0 {
		cfg.HeaderTimeout = time.Minute
	}
	return &Server{cfg: cfg, handlers: make(map[string]RawHandler), streamHandlers: make(map[string]RawStreamHandler), bidiHandlers: make(map[string]RawBidiHandler), closed: make(chan struct{}), active: make(map[net.Conn]struct{})}
}

func (s *Server) Handle(method string, handler RawHandler) {
	s.mu.Lock()
	s.handlers[method] = handler
	s.mu.Unlock()
}

func (s *Server) HandleStream(method string, handler RawStreamHandler) {
	s.mu.Lock()
	s.streamHandlers[method] = handler
	s.mu.Unlock()
}

func (s *Server) HandleBidi(method string, handler RawBidiHandler) {
	s.mu.Lock()
	s.bidiHandlers[method] = handler
	s.mu.Unlock()
}

// serverBidi 是服务端一条双向流的连接级状态。
type serverBidi struct {
	id       uint64
	sender   bidiSender
	recvCh   chan *Envelope
	sendAck  func(seq uint64)
	draining bool // 处理器已返回：继续确认并丢弃对方剩余分段，直到对方半关闭
}

type serverWriteReq struct {
	env   *Envelope
	errCh chan error
}

type ServerStream struct {
	id         uint64
	send       func(context.Context, *Envelope) error
	serializer byte
	waitAck    func(context.Context) error
}

func (s *ServerStream) Send(ctx context.Context, payload []byte, headers map[string]string) (map[string]string, error) {
	err := s.send(ctx, &Envelope{Type: TypeStream, ID: s.id, Payload: payload, Headers: headers, Serializer: s.serializer})
	if err == nil && s.waitAck != nil {
		err = s.waitAck(ctx)
	}
	return headers, err
}

func HandleTyped[Arg any, Reply any](s *Server, method string, fn TypedHandler[Arg, Reply]) {
	s.Handle(method, func(ctx context.Context, payload []byte, headers map[string]string) ([]byte, map[string]string, error) {
		var arg Arg
		codec := s.cfg.Codec
		if err := codec.Decode(payload, &arg); err != nil {
			return nil, nil, err
		}
		reply, err := fn(ctx, &arg)
		if err != nil {
			return nil, nil, err
		}
		data, err := codec.Encode(reply)
		return data, nil, err
	})
}

func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.cfg.Address)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

func (s *Server) Serve(ln net.Listener) error {
	s.closeMu.Lock()
	if s.listener == nil {
		s.listener = ln
	} else {
		s.closeMu.Unlock()
		return errors.New("lrpc: server already serving")
	}
	s.closeMu.Unlock()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.closed:
				return nil
			default:
				return err
			}
		}
		s.conns.Add(1)
		go s.runConn(conn)
	}
}

func (s *Server) Addr() net.Addr {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

func (s *Server) Close() error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	select {
	case <-s.closed:
	default:
		close(s.closed)
	}
	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.activeMu.Lock()
	for conn := range s.active {
		_ = conn.Close()
	}
	s.activeMu.Unlock()
	s.conns.Wait()
	return nil
}

func (s *Server) runConn(conn net.Conn) {
	defer s.conns.Done()
	s.serveConn(conn)
}

func (s *Server) serveConn(conn net.Conn) {
	s.activeMu.Lock()
	s.active[conn] = struct{}{}
	s.activeMu.Unlock()
	defer func() {
		s.activeMu.Lock()
		delete(s.active, conn)
		s.activeMu.Unlock()
	}()
	defer conn.Close()

	connDone := make(chan struct{})
	writeCh := make(chan serverWriteReq, s.cfg.WriteQueue)
	requests := make(map[uint64]context.CancelFunc)
	streamAcks := make(map[uint64]chan *Envelope)
	bidiStreams := make(map[uint64]*serverBidi)
	var requestsMu sync.Mutex
	semaphore := make(chan struct{}, s.cfg.MaxHandlers)
	var active sync.WaitGroup

	var writeWG sync.WaitGroup
	writeWG.Add(1)
	go func() {
		defer writeWG.Done()
		for {
			select {
			case req := <-writeCh:
				env := req.env
				frame, err := EncodeEnvelope(env, s.cfg.MaxFrameSize)
				if err != nil {
					req.errCh <- err
					return
				}
				_ = conn.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
				err = WriteFrame(conn, s.cfg.MaxFrameSize, frame)
				_ = conn.SetWriteDeadline(time.Time{})
				if err != nil {
					req.errCh <- err
					return
				}
				req.errCh <- nil
			case <-connDone:
				return
			}
		}
	}()

	writeEnv := func(outCtx context.Context, out *Envelope) error {
		req := serverWriteReq{env: out, errCh: make(chan error, 1)}
		select {
		case writeCh <- req:
			select {
			case err := <-req.errCh:
				return err
			case <-outCtx.Done():
				return outCtx.Err()
			case <-connDone:
				return ErrConnectionClosed
			}
		case <-outCtx.Done():
			return outCtx.Err()
		case <-connDone:
			return ErrConnectionClosed
		}
	}

	reader := bufio.NewReader(conn)
	readErr := error(nil)
	shutdownRead := false
	for {
		if shutdownRead {
			break
		}
		_ = conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		frame, err := ReadFrame(reader, s.cfg.MaxFrameSize, nil)
		_ = conn.SetReadDeadline(time.Time{})
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				select {
				case <-s.closed:
					return
				default:
					continue
				}
			}
			readErr = err
			break
		}
		env, err := DecodeEnvelope(frame)
		if err != nil {
			readErr = err
			break
		}
		switch env.Type {
		case TypeBidiData:
			requestsMu.Lock()
			st := bidiStreams[env.ID]
			if st != nil && st.draining {
				// 处理器已结束：确认并丢弃，对方半关闭后移除流状态。
				st.sendAck(env.Seq)
				if env.Flags&FlagEnd != 0 {
					delete(bidiStreams, env.ID)
				}
				requestsMu.Unlock()
				continue
			}
			requestsMu.Unlock()
			if st != nil {
				select {
				case st.recvCh <- env:
				default:
					// 对方违反发送窗口，协议错误，只关闭这条连接。
					readErr = ErrBadFrame
					shutdownRead = true
				}
			}
		case TypeBidiAck:
			requestsMu.Lock()
			st := bidiStreams[env.ID]
			requestsMu.Unlock()
			if st != nil {
				st.sender.ack(env.Seq)
			}
		case TypeAck:
			requestsMu.Lock()
			ack := streamAcks[env.ID]
			requestsMu.Unlock()
			if ack != nil {
				select {
				case ack <- env:
				default:
				}
			}
		case TypeCancel:
			requestsMu.Lock()
			cancel := requests[env.ID]
			requestsMu.Unlock()
			if cancel != nil {
				cancel()
			}
		case TypeRequest:
			if s.cfg.Authenticate != nil {
				if err := s.cfg.Authenticate(env.Headers); err != nil {
					s.sendError(writeCh, connDone, env, err)
					continue
				}
			}
			select {
			case semaphore <- struct{}{}:
			default:
				s.sendError(writeCh, connDone, env, ErrBusy)
				continue
			}
			ctx, cancel := context.WithCancel(context.Background())
			if deadlineText := env.Headers["x-lrpc-deadline-unix-nano"]; deadlineText != "" {
				if unixNano, err := strconv.ParseInt(deadlineText, 10, 64); err == nil {
					deadline := time.Unix(0, unixNano)
					if time.Until(deadline) > s.cfg.HeaderTimeout {
						deadline = time.Now().Add(s.cfg.HeaderTimeout)
					}
					ctx, cancel = context.WithDeadline(context.Background(), deadline)
				}
			}
			requestsMu.Lock()
			requests[env.ID] = cancel
			var ackCh chan *Envelope
			if env.Flags&FlagStream != 0 {
				ackCh = make(chan *Envelope, 1)
				streamAcks[env.ID] = ackCh
			}
			bidiCall := env.Flags&FlagBidi != 0
			var st *serverBidi
			if bidiCall {
				st = &serverBidi{
					id:     env.ID,
					sender: newBidiSender(),
					recvCh: make(chan *Envelope, bidiWindow+1),
				}
				st.sendAck = func(seq uint64) {
					_ = writeEnv(context.Background(), &Envelope{Type: TypeBidiAck, ID: env.ID, Seq: seq, Serializer: s.cfg.Codec.ID()})
				}
				bidiStreams[env.ID] = st
			}
			requestsMu.Unlock()
			streamCall := env.Flags&FlagStream != 0
			active.Add(1)
			go func(env *Envelope) {
				defer active.Done()
				defer func() {
					<-semaphore
					cancel()
					requestsMu.Lock()
					delete(requests, env.ID)
					delete(streamAcks, env.ID)
					if bidiCall {
						// 流状态保留到对方半关闭或连接断开，避免对方发送方永久阻塞。
						st.draining = true
					}
					requestsMu.Unlock()
				}()
				if bidiCall {
					stream := &ServerBidiStream{
						id:         env.ID,
						serializer: s.cfg.Codec.ID(),
						st:         st,
						write:      writeEnv,
						connDone:   connDone,
					}
					callErr := s.dispatchBidiSafe(ctx, env, stream)
					if callErr != nil {
						if ctx.Err() == nil {
							_ = writeEnv(context.Background(), &Envelope{Type: TypeError, ID: env.ID, Error: callErr.Error(), Serializer: s.cfg.Codec.ID()})
						}
						return
					}
					// 处理器返回即服务端发送方向结束，自动半关闭。
					_ = stream.CloseSend(context.Background())
					return
				}
				if streamCall {
					stream := &ServerStream{
						id:         env.ID,
						serializer: s.cfg.Codec.ID(),
						send:       writeEnv,
						waitAck: func(waitCtx context.Context) error {
							select {
							case <-ackCh:
								return nil
							case <-waitCtx.Done():
								return waitCtx.Err()
							case <-connDone:
								return ErrConnectionClosed
							}
						},
					}
					callErr := s.dispatchStreamSafe(ctx, env, stream)
					if ctx.Err() != nil {
						return
					}
					out := &Envelope{ID: env.ID, Serializer: s.cfg.Codec.ID(), Headers: map[string]string{}}
					if callErr != nil {
						out.Type = TypeError
						out.Error = callErr.Error()
					} else {
						out.Type = TypeStream
						out.Flags = FlagStream
					}
					_ = writeEnv(context.Background(), out)
					return
				}
				payload, headers, callErr := s.dispatchSafe(ctx, env)
				if ctx.Err() != nil {
					return
				}
				if callErr != nil {
					_ = writeEnv(context.Background(), &Envelope{Type: TypeError, ID: env.ID, Error: callErr.Error(), Serializer: s.cfg.Codec.ID()})
					return
				}
				_ = writeEnv(context.Background(), &Envelope{Type: TypeResponse, ID: env.ID, Payload: payload, Headers: headers, Serializer: s.cfg.Codec.ID()})
			}(env)
		default:
			readErr = ErrBadFrame
			shutdownRead = true
		}
	}

	conn.Close()
	close(connDone)
	requestsMu.Lock()
	for _, cancel := range requests {
		cancel()
	}
	requestsMu.Unlock()
	active.Wait()
	writeWG.Wait()
	_ = readErr
}

func (s *Server) dispatchSafe(ctx context.Context, env *Envelope) (payload []byte, headers map[string]string, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			payload = nil
			headers = nil
			err = &CallError{Message: fmt.Sprintf("lrpc: handler panic: %v", recovered)}
		}
	}()
	return s.dispatch(ctx, env)
}

func (s *Server) dispatch(ctx context.Context, env *Envelope) ([]byte, map[string]string, error) {
	s.mu.RLock()
	handler := s.handlers[env.Method]
	s.mu.RUnlock()
	if handler == nil {
		return nil, nil, &CallError{Message: "lrpc: unknown method: " + env.Method}
	}
	return handler(ctx, env.Payload, env.Headers)
}

func (s *Server) dispatchStreamSafe(ctx context.Context, env *Envelope, stream *ServerStream) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &CallError{Message: fmt.Sprintf("lrpc: stream handler panic: %v", recovered)}
		}
	}()
	s.mu.RLock()
	handler := s.streamHandlers[env.Method]
	s.mu.RUnlock()
	if handler == nil {
		return &CallError{Message: "lrpc: unknown stream method: " + env.Method}
	}
	return handler(ctx, env.Payload, env.Headers, stream)
}

func (s *Server) dispatchBidiSafe(ctx context.Context, env *Envelope, stream *ServerBidiStream) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &CallError{Message: fmt.Sprintf("lrpc: bidi handler panic: %v", recovered)}
		}
	}()
	s.mu.RLock()
	handler := s.bidiHandlers[env.Method]
	s.mu.RUnlock()
	if handler == nil {
		return &CallError{Message: "lrpc: unknown bidi method: " + env.Method}
	}
	return handler(ctx, env.Payload, env.Headers, stream)
}

func (s *Server) sendError(writeCh chan<- serverWriteReq, connDone <-chan struct{}, request *Envelope, err error) {
	go func() {
		req := serverWriteReq{env: &Envelope{Type: TypeError, ID: request.ID, Error: err.Error(), Serializer: s.cfg.Codec.ID()}, errCh: make(chan error, 1)}
		select {
		case writeCh <- req:
			select {
			case <-req.errCh:
			case <-connDone:
			}
		case <-connDone:
		}
	}()
}
