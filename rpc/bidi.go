package rpc

import (
	"context"
	"errors"
	"io"
	"sync"
)

// BidiState reports the final or current accounting of a bidirectional
// stream. After a connection break it tells exactly which segments were
// confirmed by the peer (Acked) and which were handed to the transport but
// cannot be confirmed (Sent > Acked): those results are undetermined and
// are never replayed silently.
type BidiState struct {
	Sent       uint64
	Acked      uint64
	Received   uint64
	Pending    int64
	SendClosed bool
	RecvEnd    bool
	RecvClosed bool
	Done       bool
	Err        error
}

// BidiStream is the client side of a bidirectional stream. Each direction
// is flow controlled independently: a sender may have at most one
// unacknowledged data segment in flight, so a slow receiver only stalls its
// own stream and memory stays bounded. Send is serialized; Recv, CloseSend
// and Cancel may be called concurrently with Send.
type BidiStream struct {
	session *clientSession
	call    *pendingCall
	ackCh   chan struct{}

	sendMu sync.Mutex
	mu     sync.Mutex

	sent       uint64
	acked      uint64
	received   uint64
	pending    map[uint64]int64
	pendingSum int64
	sendClosed bool
	recvEnd    bool
	recvClosed bool
	completed  bool
	termErr    error
}

func (c *Client) BidiStream(ctx context.Context, method string, arg any, headers map[string]string) (*BidiStream, error) {
	payload, err := c.cfg.Codec.Encode(arg)
	if err != nil {
		return nil, err
	}
	if len(payload) > c.cfg.MaxFrameSize {
		return nil, ErrMessageTooLarge
	}
	merged := mergeHeaders(c.cfg.Headers, headers)
	env := &Envelope{Type: TypeRequest, Flags: FlagBidi, Method: method, Payload: payload, Headers: merged, Serializer: c.cfg.Codec.ID()}
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
	stream := &BidiStream{ackCh: make(chan struct{}, 1), pending: make(map[uint64]int64)}
	call, err := s.beginBidi(env, c.cfg, stream)
	if err != nil {
		return nil, err
	}
	stream.session = s
	stream.call = call
	return stream, nil
}

func (s *BidiStream) Send(ctx context.Context, payload []byte, headers map[string]string) error {
	if len(payload) > s.session.cfg.MaxFrameSize {
		return ErrMessageTooLarge
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	s.mu.Lock()
	if s.termErr != nil {
		err := s.termErr
		s.mu.Unlock()
		return err
	}
	if s.completed || s.sendClosed {
		s.mu.Unlock()
		return ErrStreamClosed
	}
	s.mu.Unlock()

	// Data segments draw from the connection-wide buffer credit so one
	// large stream cannot exhaust memory and starve other streams. The
	// wait wakes if this stream finishes in the meantime.
	size := int64(len(payload))
	if err := s.session.buf.acquire(ctx, s.session.closed, s.call.done, size); err != nil {
		if errors.Is(err, errStreamDone) {
			return s.terminalErr()
		}
		return err
	}
	s.mu.Lock()
	if s.termErr != nil {
		err := s.termErr
		s.mu.Unlock()
		s.session.buf.release(size)
		return err
	}
	if s.completed || s.sendClosed {
		s.mu.Unlock()
		s.session.buf.release(size)
		return ErrStreamClosed
	}
	seq := s.sent + 1
	s.sent = seq
	s.pending[seq] = size
	s.pendingSum += size
	s.mu.Unlock()

	env := &Envelope{Type: TypeStream, ID: s.call.id, Seq: seq, Payload: payload, Headers: headers, Serializer: s.session.cfg.Codec.ID()}
	frame, err := EncodeEnvelope(env, s.session.cfg.MaxFrameSize)
	if err != nil {
		s.rollbackSend(seq, size)
		return err
	}
	if err := s.session.enqueueCtx(ctx, frame); err != nil {
		s.rollbackSend(seq, size)
		return err
	}
	for {
		s.mu.Lock()
		acked := s.acked >= seq
		s.mu.Unlock()
		if acked {
			return nil
		}
		select {
		case <-s.ackCh:
		case <-ctx.Done():
			return ctx.Err()
		case <-s.call.done:
			return s.terminalErr()
		case <-s.session.closed:
			return ErrConnectionClosed
		}
	}
}

func (s *BidiStream) rollbackSend(seq uint64, size int64) {
	s.mu.Lock()
	if s.sent == seq {
		s.sent--
	}
	delete(s.pending, seq)
	s.pendingSum -= size
	s.mu.Unlock()
	s.session.buf.release(size)
}

// releaseThrough marks every segment up to seq acknowledged and returns how
// much connection buffer credit they held.
func (s *BidiStream) releaseThrough(seq uint64) int64 {
	var released int64
	s.mu.Lock()
	if seq > s.acked {
		s.acked = seq
	}
	for pendingSeq, size := range s.pending {
		if pendingSeq <= seq {
			released += size
			delete(s.pending, pendingSeq)
		}
	}
	s.pendingSum -= released
	if s.pendingSum < 0 {
		s.pendingSum = 0
	}
	s.mu.Unlock()
	return released
}

func (s *BidiStream) releaseAll() {
	s.mu.Lock()
	released := s.pendingSum
	s.pending = make(map[uint64]int64)
	s.pendingSum = 0
	s.mu.Unlock()
	s.session.buf.release(released)
}

func (s *BidiStream) Recv(ctx context.Context) ([]byte, map[string]string, error) {
	s.mu.Lock()
	if s.recvClosed {
		s.mu.Unlock()
		return nil, nil, io.EOF
	}
	s.mu.Unlock()
	// Deliver anything already buffered before considering terminal
	// states: a tail segment or end-of-stream that already arrived must
	// not be lost just because the stream finished concurrently.
	select {
	case env := <-s.call.ch:
		return s.handleRecv(env)
	default:
	}
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-s.session.closed:
		return nil, nil, ErrConnectionClosed
	case <-s.call.done:
		return nil, nil, s.recvTerminal()
	case env := <-s.call.ch:
		return s.handleRecv(env)
	}
}

func (s *BidiStream) handleRecv(env *Envelope) ([]byte, map[string]string, error) {
	if env == nil {
		return nil, nil, ErrConnectionClosed
	}
	if env.Type == TypeStream && env.Flags&FlagEnd != 0 {
		s.mu.Lock()
		s.recvClosed = true
		both := s.sendClosed
		s.mu.Unlock()
		if both {
			s.finish()
		}
		return nil, env.Headers, io.EOF
	}
	s.mu.Lock()
	s.received = env.Seq
	s.mu.Unlock()
	s.session.ackBidi(s.call.id, s.call.method, s.session.cfg, env.Seq)
	return env.Payload, env.Headers, nil
}

func (s *BidiStream) CloseSend(ctx context.Context) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	s.mu.Lock()
	if s.termErr != nil {
		err := s.termErr
		s.mu.Unlock()
		return err
	}
	if s.completed {
		s.mu.Unlock()
		return nil
	}
	if s.sendClosed {
		s.mu.Unlock()
		return nil
	}
	s.sendClosed = true
	seq := s.sent
	recvClosed := s.recvClosed
	s.mu.Unlock()

	env := &Envelope{Type: TypeStream, Flags: FlagEnd, ID: s.call.id, Seq: seq, Serializer: s.session.cfg.Codec.ID()}
	frame, err := EncodeEnvelope(env, s.session.cfg.MaxFrameSize)
	if err != nil {
		return err
	}
	if err := s.session.enqueueCtx(ctx, frame); err != nil {
		return err
	}
	if recvClosed {
		s.finish()
	}
	return nil
}

func (s *BidiStream) Cancel() {
	s.session.cancel(s.call.id, s.call.method, s.session.cfg)
	s.fail(context.Canceled)
}

func (s *BidiStream) State() BidiState {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := BidiState{
		Sent:       s.sent,
		Acked:      s.acked,
		Received:   s.received,
		Pending:    s.pendingSum,
		SendClosed: s.sendClosed,
		RecvEnd:    s.recvEnd || s.recvClosed,
		RecvClosed: s.recvClosed,
	}
	select {
	case <-s.call.done:
		state.Done = true
		state.Err = s.termErr
		if state.Err == nil && !s.completed {
			state.Err = ErrConnectionClosed
		}
	default:
	}
	return state
}

func (s *BidiStream) terminalErr() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.termErr != nil {
		return s.termErr
	}
	if s.completed {
		return ErrStreamClosed
	}
	return ErrConnectionClosed
}

func (s *BidiStream) recvTerminal() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.termErr != nil {
		return s.termErr
	}
	return io.EOF
}

func (s *BidiStream) fail(err error) {
	s.mu.Lock()
	if s.termErr == nil && !s.completed {
		s.termErr = err
	}
	s.mu.Unlock()
	s.releaseAll()
	s.session.remove(s.call.id)
}

func (s *BidiStream) finish() {
	s.mu.Lock()
	if s.termErr == nil {
		s.completed = true
	}
	s.mu.Unlock()
	s.releaseAll()
	s.session.remove(s.call.id)
}

// onEnvelope routes an inbound envelope for this stream. It reports false
// when the peer violated the protocol (unknown sequencing or more data than
// the flow-control window allows); the session then closes the connection.
func (s *BidiStream) onEnvelope(env *Envelope) bool {
	switch env.Type {
	case TypeAck:
		released := s.releaseThrough(env.Seq)
		s.session.buf.release(released)
		select {
		case s.ackCh <- struct{}{}:
		case <-s.call.done:
		default:
		}
		return true
	case TypeCancel:
		// If the peer already half-closed its send direction and we
		// already closed ours, the stream is complete: a late reset
		// must not clobber a cleanly finished stream or drop its tail.
		s.mu.Lock()
		settled := s.recvEnd && s.sendClosed
		s.mu.Unlock()
		if settled {
			s.finish()
			return true
		}
		s.fail(ErrStreamReset)
		return true
	case TypeError:
		s.fail(&CallError{Message: env.Error})
		return true
	case TypeStream:
		if env.Flags&FlagEnd == 0 {
			s.mu.Lock()
			ok := env.Seq == s.received+1
			s.mu.Unlock()
			if !ok {
				return false
			}
		} else {
			s.mu.Lock()
			s.recvEnd = true
			s.mu.Unlock()
		}
		select {
		case s.call.ch <- env:
			return true
		case <-s.call.done:
			return true
		default:
			return false
		}
	}
	return false
}

// ServerBidiStream is the server side of a bidirectional stream. The
// handler must consume the inbound direction until io.EOF (or Cancel);
// returning while the peer can still send resets the stream so the peer is
// never left waiting without a signal.
type ServerBidiStream struct {
	id         uint64
	serializer byte
	write      func(context.Context, *Envelope) error
	ackCh      chan struct{}
	dataCh     chan *Envelope
	connDone   <-chan struct{}
	buf        *bufferCredit

	sendMu sync.Mutex
	mu     sync.Mutex

	sent         uint64
	acked        uint64
	received     uint64
	pending      map[uint64]int64
	pendingSum   int64
	sendClosed   bool
	remoteClosed bool
}

func (s *ServerBidiStream) Send(ctx context.Context, payload []byte, headers map[string]string) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	s.mu.Lock()
	if s.sendClosed {
		s.mu.Unlock()
		return ErrStreamClosed
	}
	s.mu.Unlock()
	size := int64(len(payload))
	if err := s.buf.acquire(ctx, s.connDone, nil, size); err != nil {
		return err
	}
	s.mu.Lock()
	seq := s.sent + 1
	s.sent = seq
	s.pending[seq] = size
	s.pendingSum += size
	s.mu.Unlock()
	env := &Envelope{Type: TypeStream, ID: s.id, Seq: seq, Payload: payload, Headers: headers, Serializer: s.serializer}
	if err := s.write(ctx, env); err != nil {
		s.mu.Lock()
		if s.sent == seq {
			s.sent--
		}
		delete(s.pending, seq)
		s.pendingSum -= size
		s.mu.Unlock()
		s.buf.release(size)
		return err
	}
	for {
		s.mu.Lock()
		acked := s.acked >= seq
		s.mu.Unlock()
		if acked {
			return nil
		}
		select {
		case <-s.ackCh:
		case <-ctx.Done():
			return ctx.Err()
		case <-s.connDone:
			return ErrConnectionClosed
		}
	}
}

// onAck records a peer acknowledgement, releases the connection buffer
// credit held by the acknowledged segments and wakes a waiting Send.
func (s *ServerBidiStream) onAck(seq uint64) {
	var released int64
	s.mu.Lock()
	if seq > s.acked {
		s.acked = seq
	}
	for pendingSeq, size := range s.pending {
		if pendingSeq <= seq {
			released += size
			delete(s.pending, pendingSeq)
		}
	}
	s.pendingSum -= released
	if s.pendingSum < 0 {
		s.pendingSum = 0
	}
	s.mu.Unlock()
	s.buf.release(released)
	select {
	case s.ackCh <- struct{}{}:
	default:
	}
}

// observeBuffered accounts for inbound segments the handler never consumed
// before returning, including a buffered half-close. It lets the framework
// distinguish "peer still wants to send" (reset needed) from "peer already
// finished" (no reset, the stream ended cleanly).
func (s *ServerBidiStream) observeBuffered() {
	for {
		select {
		case env := <-s.dataCh:
			if env.Flags&FlagEnd != 0 {
				s.mu.Lock()
				s.remoteClosed = true
				s.mu.Unlock()
				return
			}
			s.mu.Lock()
			if env.Seq > s.received {
				s.received = env.Seq
			}
			s.mu.Unlock()
		default:
			return
		}
	}
}

func (s *ServerBidiStream) Recv(ctx context.Context) ([]byte, map[string]string, error) {
	s.mu.Lock()
	if s.remoteClosed {
		s.mu.Unlock()
		return nil, nil, io.EOF
	}
	s.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-s.connDone:
		return nil, nil, ErrConnectionClosed
	case env := <-s.dataCh:
		if env.Flags&FlagEnd != 0 {
			s.mu.Lock()
			s.remoteClosed = true
			s.mu.Unlock()
			return nil, env.Headers, io.EOF
		}
		s.mu.Lock()
		if env.Seq != s.received+1 {
			s.mu.Unlock()
			_ = s.write(context.Background(), &Envelope{Type: TypeCancel, ID: s.id, Serializer: s.serializer})
			return nil, nil, ErrBadFrame
		}
		s.received = env.Seq
		s.mu.Unlock()
		ack := &Envelope{Type: TypeAck, ID: s.id, Seq: env.Seq, Serializer: s.serializer}
		if err := s.write(ctx, ack); err != nil {
			return nil, nil, err
		}
		return env.Payload, env.Headers, nil
	}
}

func (s *ServerBidiStream) CloseSend(ctx context.Context) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	s.mu.Lock()
	if s.sendClosed {
		s.mu.Unlock()
		return nil
	}
	s.sendClosed = true
	seq := s.sent
	s.mu.Unlock()
	return s.write(ctx, &Envelope{Type: TypeStream, Flags: FlagEnd, ID: s.id, Seq: seq, Serializer: s.serializer})
}

func (s *ServerBidiStream) Cancel() {
	_ = s.write(context.Background(), &Envelope{Type: TypeCancel, ID: s.id, Serializer: s.serializer})
}

func (s *ServerBidiStream) State() BidiState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return BidiState{
		Sent:       s.sent,
		Acked:      s.acked,
		Received:   s.received,
		Pending:    s.pendingSum,
		SendClosed: s.sendClosed,
		RecvEnd:    s.remoteClosed,
		RecvClosed: s.remoteClosed,
	}
}

func (s *ServerBidiStream) RemoteClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.remoteClosed
}

func (s *ServerBidiStream) SendClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sendClosed
}
