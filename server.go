package localrpc

import (
	"context"
	"errors"
	"net"
	"sync"
)

type HandlerFunc func(context.Context, []byte) ([]byte, error)

type Server struct {
	codec          Codec
	maxMessageSize int
	maxQueue       int
	mu             sync.RWMutex
	services       map[string]map[string]HandlerFunc
}

type serverCall struct {
	ctx    context.Context
	cancel context.CancelFunc
}

type ServerOption func(*Server)

func WithServerCodec(codec Codec) ServerOption {
	return func(s *Server) { s.codec = codec }
}

func WithServerMaxMessageSize(size int) ServerOption {
	return func(s *Server) { s.maxMessageSize = size }
}

func WithServerMaxQueue(size int) ServerOption {
	return func(s *Server) { s.maxQueue = size }
}

func NewServer(options ...ServerOption) *Server {
	s := &Server{
		codec:          JSONCodec{},
		maxMessageSize: DefaultMaxMessageSize,
		maxQueue:       DefaultMaxQueue,
		services:       make(map[string]map[string]HandlerFunc),
	}
	for _, option := range options {
		option(s)
	}
	return s
}

func (s *Server) Handle(service, method string, handler HandlerFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.services[service] == nil {
		s.services[service] = make(map[string]HandlerFunc)
	}
	s.services[service][method] = handler
}

func (s *Server) Serve(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go s.serveConn(conn)
	}
}

func (s *Server) serveConn(conn net.Conn) {
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var inflightMu sync.Mutex
	var writeMu sync.Mutex
	active := make(map[uint64]serverCall)
	jobs := make(chan Message, s.maxQueue)
	tokens := make(chan struct{}, s.maxQueue)

	go func() {
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-jobs:
				go func(msg Message) {
					defer func() { <-tokens }()
					defer func() {
						inflightMu.Lock()
						delete(active, msg.ID)
						inflightMu.Unlock()
					}()
					s.callHandler(ctx, conn, &writeMu, msg, active, &inflightMu)
				}(msg)
			}
		}
	}()

	for {
		msg, frameType, err := ReadFrame(conn, s.codec, s.maxMessageSize)
		if err != nil {
			cancel()
			return
		}
		switch frameType {
		case FrameRequest:
			requestCtx, requestCancel := context.WithCancel(ctx)
			inflightMu.Lock()
			active[msg.ID] = serverCall{ctx: requestCtx, cancel: requestCancel}
			inflightMu.Unlock()

			select {
			case tokens <- struct{}{}:
				jobs <- msg
			default:
				requestCancel()
				inflightMu.Lock()
				delete(active, msg.ID)
				inflightMu.Unlock()
				s.writeResponse(conn, &writeMu, Message{ID: msg.ID, Error: &RemoteError{Code: CodeQueueFull, Message: ErrQueueFull.Error()}})
			}
		case FrameCancel:
			inflightMu.Lock()
			call := active[msg.ID]
			inflightMu.Unlock()
			if call.cancel != nil {
				call.cancel()
			}
		default:
			cancel()
			return
		}
	}
}

func (s *Server) callHandler(parent context.Context, conn net.Conn, writeMu *sync.Mutex, msg Message, active map[uint64]serverCall, inflightMu *sync.Mutex) {
	inflightMu.Lock()
	call := active[msg.ID]
	inflightMu.Unlock()
	if call.cancel == nil {
		return
	}

	response := Message{ID: msg.ID}
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				response.Payload = nil
				response.Error = &RemoteError{Code: CodeInternal, Message: "handler panic"}
			}
		}()
		handler := s.handler(msg.Service, msg.Method)
		if handler == nil {
			response.Error = &RemoteError{Code: CodeUnknownMethod, Message: "unknown method"}
			return
		}
		result, err := handler(call.ctx, msg.Payload)
		if err != nil {
			response.Error = remoteError(err)
			return
		}
		response.Payload = result
	}()

	select {
	case <-parent.Done():
	default:
		s.writeResponse(conn, writeMu, response)
	}
}

func (s *Server) writeResponse(conn net.Conn, writeMu *sync.Mutex, response Message) {
	writeMu.Lock()
	defer writeMu.Unlock()
	_ = WriteFrame(conn, response, FrameResponse, s.codec, s.maxMessageSize)
}

func (s *Server) handler(service, method string) HandlerFunc {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if methods := s.services[service]; methods != nil {
		return methods[method]
	}
	return nil
}
