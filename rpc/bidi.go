package rpc

import (
	"context"
	"io"
	"sync"
)

// DefaultStreamCredits bounds how many bidi stream segments a single
// connection may buffer in total. Every stream is granted exactly one
// window credit at a time (so at most one segment per direction per stream
// is unacknowledged), and the credit pool is shared: many stalled streams
// together cannot grow beyond this bound, while free credits keep flowing
// to other streams, short requests and control frames.
const DefaultStreamCredits = 32

// creditPool is a connection-shared pool of flow-control grants for data
// segments travelling in one direction. Acquire blocks until a grant is
// available; Release returns it, typically when the segment has been
// delivered to the application and acknowledged.
type creditPool struct {
	ch chan struct{}
}

func newCreditPool(n int) *creditPool {
	if n <= 0 {
		n = DefaultStreamCredits
	}
	p := &creditPool{ch: make(chan struct{}, n)}
	for i := 0; i < n; i++ {
		p.ch <- struct{}{}
	}
	return p
}

func (p *creditPool) acquire(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-p.ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return ErrConnectionClosed
	}
}

func (p *creditPool) release() {
	select {
	case p.ch <- struct{}{}:
	default:
	}
}

// BidiState reports the accounting of a bidirectional stream at any point.
// Each direction is numbered independently from 1.
//
// Sent segments in [1, Acked] were confirmed by the peer, while those in
// (Acked, Sent] were handed to the transport and cannot be confirmed after
// a connection break; such effects are undetermined and are never silently
// replayed. Received counts segments handed to the local application.
type BidiState struct {
	Sent       uint64
	Acked      uint64
	Received   uint64
	SendClosed bool
	RecvClosed bool
	Done       bool
	Err        error
}

// Inflight reports how many sent segments have not been confirmed yet.
// Those segments consume one grant each from the connection-shared
// credit pool; their fate is undetermined after a connection break.
func (st BidiState) Inflight() uint64 {
	if st.Sent > st.Acked {
		return st.Sent - st.Acked
	}
	return 0
}

// BidiStream is the client side of a bidirectional stream. Each direction
// is flow controlled independently. Send is serialized; Recv, CloseSend
// and Cancel may run concurrently with Send.
type BidiStream struct {
	session *clientSession
	call    *pendingCall
	ackCh   chan uint64

	sendMu sync.Mutex
	mu     sync.Mutex

	sent       uint64
	acked      uint64
	received   uint64
	sendClosed bool
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
	stream := &BidiStream{ackCh: make(chan uint64, 4)}
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
	seq := s.sent + 1
	credits := s.session.bidiCredits
	s.mu.Unlock()

	// Acquire a shared connection grant before taking a transport slot, so a
	// slow peer bounds total buffering across all streams and cannot starve
	// cancellations, end frames or other streams indefinitely.
	if err := credits.acquire(ctx, s.call.done); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return s.terminalErr()
	}
	env := &Envelope{Type: TypeStream, ID: s.call.id, Seq: seq, Payload: payload, Headers: headers, Serializer: s.session.cfg.Codec.ID()}
	frame, err := EncodeEnvelope(env, s.session.cfg.MaxFrameSize)
	if err != nil {
		credits.release()
		return err
	}
	if err := s.session.enqueueCtx(ctx, frame); err != nil {
		credits.release()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	s.mu.Lock()
	s.sent = seq
	s.mu.Unlock()
	waitReleased := false
	release := func() {
		if !waitReleased {
			waitReleased = true
			credits.release()
		}
	}
	defer release()
	for {
		// Check for a terminal signal only when no unconsumed ack remains:
		// a segment already confirmed by the peer must complete normally
		// even if cancellation races with the acknowledgment.
		select {
		case acked := <-s.ackCh:
			s.mu.Lock()
			if acked > s.acked {
				s.acked = acked
			}
			s.mu.Unlock()
			if acked >= seq {
				return nil
			}
			continue
		default:
		}
		select {
		case acked := <-s.ackCh:
			s.mu.Lock()
			if acked > s.acked {
				s.acked = acked
			}
			s.mu.Unlock()
			if acked >= seq {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-s.call.done:
			return s.terminalErr()
		case <-s.session.closed:
			return ErrConnectionClosed
		}
	}
}

func (s *BidiStream) Recv(ctx context.Context) ([]byte, map[string]string, error) {
	s.mu.Lock()
	if s.termErr != nil {
		err := s.termErr
		s.mu.Unlock()
		return nil, nil, err
	}
	if s.recvClosed {
		s.mu.Unlock()
		return nil, nil, io.EOF
	}
	s.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-s.session.closed:
		return nil, nil, ErrConnectionClosed
	case <-s.call.done:
		return nil, nil, s.recvTerminal()
	case env := <-s.call.ch:
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
		SendClosed: s.sendClosed,
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
	s.session.remove(s.call.id)
}

func (s *BidiStream) finish() {
	s.mu.Lock()
	if s.termErr == nil {
		s.completed = true
	}
	s.mu.Unlock()
	s.session.remove(s.call.id)
}

// onEnvelope routes an inbound envelope for this stream. Control frames are
// delivered without flow control so cancellation never gets stuck behind a
// stalled data reader; data frames are delivered in order with backpressure
// (the read loop acquired the shared connection grant first).
func (s *BidiStream) onEnvelope(env *Envelope) bool {
	switch env.Type {
	case TypeAck:
		select {
		case s.ackCh <- env.Seq:
			return true
		case <-s.call.done:
			return true
		}
	case TypeCancel:
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
		}
		select {
		case s.call.ch <- env:
			return true
		case <-s.call.done:
			return true
		}
	}
	return false
}

// ServerBidiStream is the server side of a bidirectional stream. The
// handler should consume the inbound direction until io.EOF; returning
// while the peer can still send causes the framework to half-close the
// outbound direction and then reset the still-open inbound direction, so
// the peer is never left waiting without a signal.
type ServerBidiStream struct {
	id         uint64
	serializer byte
	write      func(context.Context, *Envelope) error
	ackCh      chan *Envelope
	dataCh     chan *Envelope
	resetCh    chan struct{}
	connDone   <-chan struct{}
	credits    *creditPool
	cancelCtx  context.CancelFunc
	pendingAck uint64

	sendMu sync.Mutex
	mu     sync.Mutex

	sent         uint64
	acked        uint64
	received     uint64
	sendClosed   bool
	remoteClosed bool
	resetClosed  bool
}

func (s *ServerBidiStream) Send(ctx context.Context, payload []byte, headers map[string]string) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	s.mu.Lock()
	if s.sendClosed {
		s.mu.Unlock()
		return ErrStreamClosed
	}
	seq := s.sent + 1
	s.mu.Unlock()

	if err := s.credits.acquire(ctx, s.connDone); err != nil {
		return err
	}
	env := &Envelope{Type: TypeStream, ID: s.id, Seq: seq, Payload: payload, Headers: headers, Serializer: s.serializer}
	if err := s.write(ctx, env); err != nil {
		s.credits.release()
		return err
	}
	s.mu.Lock()
	s.sent = seq
	s.mu.Unlock()
	released := false
	release := func() {
		if !released {
			released = true
			s.credits.release()
		}
	}
	defer release()
	for {
		// Prefer an already delivered acknowledgment over cancellation:
		// a peer-confirmed segment is finished and must not surface as an
		// error just because a cancel landed in the same wakeup.
		select {
		case ack := <-s.ackCh:
			s.mu.Lock()
			if ack.Seq > s.acked {
				s.acked = ack.Seq
			}
			s.mu.Unlock()
			if ack.Seq >= seq {
				return nil
			}
			continue
		default:
		}
		select {
		case ack := <-s.ackCh:
			s.mu.Lock()
			if ack.Seq > s.acked {
				s.acked = ack.Seq
			}
			s.mu.Unlock()
			if ack.Seq >= seq {
				return nil
			}
		case <-ctx.Done():
			// The read loop processes frames in TCP order, so any ack that
			// preceded this cancel was already recorded as lastAcked: a
			// peer-confirmed segment finishes even though the context is
			// now canceled.
			s.mu.Lock()
			confirmed := s.pendingAck >= seq
			if s.pendingAck > s.acked {
				s.acked = s.pendingAck
			}
			s.mu.Unlock()
			if confirmed {
				return nil
			}
			return ctx.Err()
		case <-s.resetCh:
			s.mu.Lock()
			confirmed := s.pendingAck >= seq
			if s.pendingAck > s.acked {
				s.acked = s.pendingAck
			}
			s.mu.Unlock()
			if confirmed {
				return nil
			}
			return ErrStreamReset
		case <-s.connDone:
			return ErrConnectionClosed
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
	case <-s.resetCh:
		return nil, nil, ErrStreamReset
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
	s.mu.Unlock()
	return s.write(ctx, &Envelope{Type: TypeStream, Flags: FlagEnd, ID: s.id, Seq: s.sent, Serializer: s.serializer})
}

func (s *ServerBidiStream) Cancel() {
	_ = s.write(context.Background(), &Envelope{Type: TypeCancel, ID: s.id, Serializer: s.serializer})
}

// deliverAck is called by the connection read loop for every inbound
// acknowledgment, in frame order. pendingAck always reflects the latest
// ack observed on the wire, so a cancel arriving immediately after it
// cannot erase confirmation of the tail segment. The channel wake is
// best effort; Send consults pendingAck when cancellation wins the wake.
func (s *ServerBidiStream) deliverAck(env *Envelope) {
	s.mu.Lock()
	if env.Seq > s.pendingAck {
		s.pendingAck = env.Seq
	}
	s.mu.Unlock()
	select {
	case s.ackCh <- env:
	case <-s.connDone:
	default:
	}
}

// postReset is invoked from the read loop when the peer cancels. The
// handler context is canceled and blocked Send/Recv wake with
// ErrStreamReset, but Send prefers a parked or delivered ack, so a
// segment confirmed by the peer at the moment of cancellation is not
// lost. Cancellation is scoped to this stream only.
func (s *ServerBidiStream) postReset() {
	s.mu.Lock()
	already := s.resetClosed
	s.resetClosed = true
	s.mu.Unlock()
	if !already {
		close(s.resetCh)
	}
	if s.cancelCtx != nil {
		s.cancelCtx()
	}
}

func (s *ServerBidiStream) State() BidiState {
	s.mu.Lock()
	acked := s.acked
	if s.pendingAck > acked {
		acked = s.pendingAck
	}
	s.mu.Unlock()
	return BidiState{
		Sent:       s.sent,
		Acked:      acked,
		Received:   s.received,
		SendClosed: s.sendClosed,
		RecvClosed: s.remoteClosed,
	}
}

// markRemoteClosed records a peer half-close observed by the connection
// read loop without requiring the handler to drain the inbox first.
func (s *ServerBidiStream) markRemoteClosed(env *Envelope) {
	s.mu.Lock()
	s.remoteClosed = true
	s.mu.Unlock()
	select {
	case s.dataCh <- env:
	case <-s.connDone:
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
