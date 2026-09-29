package rpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"
)

type RawHandler func(ctx context.Context, payload []byte, headers map[string]string) ([]byte, map[string]string, error)

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
	cfg      ServerConfig
	mu       sync.RWMutex
	handlers map[string]RawHandler
	listener net.Listener
	closed   chan struct{}
	closeMu  sync.Mutex
	conns    sync.WaitGroup
	activeMu sync.Mutex
	active   map[net.Conn]struct{}
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
	return &Server{cfg: cfg, handlers: make(map[string]RawHandler), closed: make(chan struct{}), active: make(map[net.Conn]struct{})}
}

func (s *Server) Handle(method string, handler RawHandler) {
	s.mu.Lock()
	s.handlers[method] = handler
	s.mu.Unlock()
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
	writeCh := make(chan *Envelope, s.cfg.WriteQueue)
	requests := make(map[uint64]context.CancelFunc)
	var requestsMu sync.Mutex
	semaphore := make(chan struct{}, s.cfg.MaxHandlers)
	var active sync.WaitGroup

	var writeWG sync.WaitGroup
	writeWG.Add(1)
	go func() {
		defer writeWG.Done()
		for {
			select {
			case env := <-writeCh:
				frame, err := EncodeEnvelope(env, s.cfg.MaxFrameSize)
				if err != nil {
					return
				}
				if err := WriteFrame(conn, s.cfg.MaxFrameSize, frame); err != nil {
					return
				}
			case <-connDone:
				return
			}
		}
	}()

	var readBuf []byte
	readErr := error(nil)
	shutdownRead := false
	for {
		if shutdownRead {
			break
		}
		frame, err := ReadFrame(conn, s.cfg.MaxFrameSize, readBuf)
		if err != nil {
			readErr = err
			break
		}
		env, err := DecodeEnvelope(frame)
		if err != nil {
			readErr = err
			break
		}
		switch env.Type {
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
					s.sendError(writeCh, env, err)
					continue
				}
			}
			select {
			case semaphore <- struct{}{}:
			default:
				s.sendError(writeCh, env, ErrBusy)
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
			requestsMu.Unlock()
			active.Add(1)
			go func(env *Envelope) {
				defer active.Done()
				defer func() {
					<-semaphore
					cancel()
					requestsMu.Lock()
					delete(requests, env.ID)
					requestsMu.Unlock()
				}()
				payload, headers, callErr := s.dispatchSafe(ctx, env)
				if ctx.Err() != nil {
					return
				}
				if callErr != nil {
					s.sendError(writeCh, env, callErr)
					return
				}
				select {
				case writeCh <- &Envelope{Type: TypeResponse, ID: env.ID, Payload: payload, Headers: headers, Serializer: s.cfg.Codec.ID()}:
				case <-connDone:
				}
			}(env)
		default:
			readErr = ErrBadFrame
			shutdownRead = true
		}
	}

	close(connDone)
	conn.Close()
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

func (s *Server) sendError(writeCh chan<- *Envelope, request *Envelope, err error) {
	select {
	case writeCh <- &Envelope{Type: TypeError, ID: request.ID, Error: err.Error(), Serializer: s.cfg.Codec.ID()}:
	default:
	}
}
