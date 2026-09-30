package rpc

import (
	"context"
	"sync"
)

// StreamSender lets a server handler emit consecutive fragments. Send blocks
// while the reader has not granted enough window, so a slow reader slows the
// handler. Outstanding reports payload bytes sent but not yet acknowledged.
type StreamSender interface {
	Send(payload []byte, headers map[string]string) error
	Outstanding() int
}

// StreamHandler is a raw server-streaming handler.
type StreamHandler func(ctx context.Context, payload []byte, headers map[string]string, out StreamSender) error

type serverStream struct {
	id       uint64
	writeCh  chan<- *Envelope
	connDone <-chan struct{}
	codecID  byte
	maxFrame int
	ctx      context.Context

	mu          sync.Mutex
	cond        *sync.Cond
	credit      int
	outstanding int
	closed      bool
	done        chan struct{}
}

func newServerStream(ctx context.Context, id uint64, writeCh chan<- *Envelope, connDone <-chan struct{}, codecID byte, maxFrame, initialCredit int) *serverStream {
	s := &serverStream{
		id:       id,
		writeCh:  writeCh,
		connDone: connDone,
		codecID:  codecID,
		maxFrame: maxFrame,
		credit:   initialCredit,
		done:     make(chan struct{}),
		ctx:      ctx,
	}
	s.cond = sync.NewCond(&s.mu)
	go s.wakeOnContext(ctx)
	return s
}

func (s *serverStream) wakeOnContext(ctx context.Context) {
	select {
	case <-ctx.Done():
		s.abort()
	case <-s.connDone:
		s.abort()
	case <-s.done:
	}
}

func (s *serverStream) abort() {
	s.mu.Lock()
	s.closed = true
	s.cond.Broadcast()
	select {
	case <-s.done:
	default:
		close(s.done)
	}
	s.mu.Unlock()
}

func (s *serverStream) Outstanding() int {
	s.mu.Lock()
	n := s.outstanding
	s.mu.Unlock()
	return n
}

// Send transmits payload, splitting when it exceeds available credit or the
// frame size. It blocks once reader credit is exhausted (the backpressure
// point). Every consumed frame makes the reader grant matching credit.
func (s *serverStream) Send(payload []byte, headers map[string]string) error {
	remaining := append([]byte(nil), payload...)
	for len(remaining) > 0 {
		s.mu.Lock()
		for !s.closed && s.credit <= 0 {
			s.cond.Wait()
		}
		closed := s.closed
		avail := s.credit
		s.mu.Unlock()
		if closed {
			if s.ctx.Err() != nil {
				return s.ctx.Err()
			}
			return ErrConnectionClosed
		}
		n := len(remaining)
		if n > avail {
			n = avail
		}
		if limit := s.maxFrame - 64; n > limit {
			n = limit
		}
		s.mu.Lock()
		s.credit -= n
		s.outstanding += n
		s.mu.Unlock()

		h := map[string]string{}
		for key, value := range headers {
			h[key] = value
		}
		if len(remaining[n:]) > 0 {
			h[HeaderStreamMore] = "1"
		}
		env := &Envelope{Type: TypeStream, ID: s.id, Payload: remaining[:n], Headers: h, Serializer: s.codecID}
		select {
		case s.writeCh <- env:
		case <-s.connDone:
			if s.ctx.Err() != nil {
				return s.ctx.Err()
			}
			return ErrConnectionClosed
		}
		remaining = remaining[n:]
	}
	return nil
}

// acked grants additional bytes of reader credit.
func (s *serverStream) acked(n int64) {
	s.mu.Lock()
	s.outstanding -= int(n)
	if s.outstanding < 0 {
		s.outstanding = 0
	}
	s.credit += int(n)
	s.cond.Broadcast()
	s.mu.Unlock()
}

func (s *serverStream) sendTerminal(errorText string) {
	s.finish()
	env := &Envelope{Type: TypeStream, ID: s.id, Serializer: s.codecID, Headers: map[string]string{}}
	if errorText == "" {
		env.Headers[HeaderStreamEnd] = "1"
	} else {
		env.Error = errorText
		env.Headers[HeaderStreamError] = errorText
	}
	select {
	case s.writeCh <- env:
	case <-s.connDone:
	}
}

func (s *serverStream) finish() {
	s.mu.Lock()
	if !s.closed {
		close(s.done)
	}
	s.mu.Unlock()
}
