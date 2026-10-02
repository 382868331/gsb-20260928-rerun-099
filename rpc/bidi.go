package rpc

import (
	"context"
	"io"
	"sort"
	"sync"
)

// bidiWindow 是每条双向流每个方向上允许未确认分段的最大数量。
// 达到窗口上限后 Send 会阻塞，慢接收方因此只限制自己这条流。
const bidiWindow = 4

// StreamOutcome 描述连接中断或流结束时发送方向的确切状态：
// 哪些分段已被对方确认，哪些分段的结果不能确定。
type StreamOutcome struct {
	AckedUpTo    int64    // 已确认的最大连续序号，-1 表示没有任何确认
	Undetermined []uint64 // 已发送但未收到确认的分段序号，结果不能确定
}

// bidiSender 跟踪一个发送方向的序号、窗口与确认状态。
type bidiSender struct {
	mu           sync.Mutex
	nextSeq      uint64
	unacked      map[uint64]struct{}
	ackedUpTo    int64
	sendClosed   bool
	remoteClosed bool
	canceled     bool
	ackNotify    chan struct{}
}

func newBidiSender() bidiSender {
	return bidiSender{unacked: make(map[uint64]struct{}), ackedUpTo: -1, ackNotify: make(chan struct{}, 1)}
}

// ack 记录对方确认的序号并唤醒等待窗口的发送方。
func (s *bidiSender) ack(seq uint64) {
	s.mu.Lock()
	delete(s.unacked, seq)
	if int64(seq) > s.ackedUpTo {
		s.ackedUpTo = int64(seq)
	}
	s.mu.Unlock()
	select {
	case s.ackNotify <- struct{}{}:
	default:
	}
}

func (s *bidiSender) outcome() StreamOutcome {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := StreamOutcome{AckedUpTo: s.ackedUpTo}
	for seq := range s.unacked {
		out.Undetermined = append(out.Undetermined, seq)
	}
	sort.Slice(out.Undetermined, func(i, j int) bool { return out.Undetermined[i] < out.Undetermined[j] })
	return out
}

// BidiStream 是客户端一条双向数据流的句柄。
// 两个方向各自独立：CloseSend 只结束发送方向，Recv 继续到对方半关闭；
// Cancel 取消整条流，不影响同一连接上的其他流与请求。
type BidiStream struct {
	session *clientSession
	id      uint64
	method  string
	sender  bidiSender
	recvCh  chan *Envelope
	done    chan struct{}
	recvEOF bool
	mu      sync.Mutex
	once    sync.Once
}

// Send 发送一个分段。分段进入发送窗口即返回；窗口满时阻塞，
// 直到对方确认旧分段、流被取消或连接断开。
func (s *BidiStream) Send(ctx context.Context, payload []byte) error {
	cfg := s.session.cfg
	if len(payload) > cfg.MaxFrameSize {
		return ErrMessageTooLarge
	}
	for {
		s.sender.mu.Lock()
		switch {
		case s.sender.canceled:
			s.sender.mu.Unlock()
			return ErrCanceled
		case s.sender.sendClosed:
			s.sender.mu.Unlock()
			return ErrStreamClosed
		}
		if len(s.sender.unacked) < bidiWindow {
			seq := s.sender.nextSeq
			s.sender.nextSeq++
			s.sender.unacked[seq] = struct{}{}
			s.sender.mu.Unlock()
			env := &Envelope{Type: TypeBidiData, ID: s.id, Seq: seq, Payload: payload, Serializer: cfg.Codec.ID()}
			frame, err := EncodeEnvelope(env, cfg.MaxFrameSize)
			if err != nil {
				s.sender.ack(seq)
				return err
			}
			if err := s.session.enqueue(frame); err != nil {
				s.sender.ack(seq)
				return err
			}
			return nil
		}
		s.sender.mu.Unlock()
		select {
		case <-s.sender.ackNotify:
		case <-s.done:
			select {
			case <-s.session.closed:
				return ErrConnectionClosed
			default:
			}
			return ErrCanceled
		case <-s.session.closed:
			return ErrConnectionClosed
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Recv 接收对方发送的下一个分段。对方半关闭后返回 io.EOF；
// 流被取消返回 ErrCanceled；连接断开返回 ErrConnectionClosed。
func (s *BidiStream) Recv(ctx context.Context) ([]byte, error) {
	s.mu.Lock()
	if s.recvEOF {
		s.mu.Unlock()
		return nil, io.EOF
	}
	s.mu.Unlock()
	select {
	case env := <-s.recvCh:
		if env == nil {
			return nil, ErrConnectionClosed
		}
		if env.Type == TypeError {
			s.finish()
			return nil, &CallError{Message: env.Error}
		}
		s.session.sendBidiAck(s.id, env.Seq)
		if env.Flags&FlagEnd != 0 {
			s.sender.mu.Lock()
			s.sender.remoteClosed = true
			s.sender.mu.Unlock()
			s.mu.Lock()
			s.recvEOF = true
			s.mu.Unlock()
			s.maybeFinish()
			return nil, io.EOF
		}
		return env.Payload, nil
	case <-s.done:
		select {
		case <-s.session.closed:
			return nil, ErrConnectionClosed
		default:
		}
		return nil, ErrCanceled
	case <-s.session.closed:
		return nil, ErrConnectionClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// CloseSend 半关闭发送方向：发送带结束标记的最后一个序号分段，
// 之后 Send 返回 ErrStreamClosed，Recv 不受影响。
func (s *BidiStream) CloseSend() error {
	cfg := s.session.cfg
	s.sender.mu.Lock()
	if s.sender.canceled {
		s.sender.mu.Unlock()
		return ErrCanceled
	}
	if s.sender.sendClosed {
		s.sender.mu.Unlock()
		return nil
	}
	s.sender.sendClosed = true
	seq := s.sender.nextSeq
	s.sender.nextSeq++
	s.sender.unacked[seq] = struct{}{}
	s.sender.mu.Unlock()
	env := &Envelope{Type: TypeBidiData, ID: s.id, Seq: seq, Flags: FlagEnd, Serializer: cfg.Codec.ID()}
	frame, err := EncodeEnvelope(env, cfg.MaxFrameSize)
	if err != nil {
		return err
	}
	if err := s.session.enqueue(frame); err != nil {
		return err
	}
	s.maybeFinish()
	return nil
}

// Cancel 取消整条流：通知对方并立即释放本地状态，
// 后续 Send/Recv 返回 ErrCanceled。
func (s *BidiStream) Cancel() {
	s.sender.mu.Lock()
	already := s.sender.canceled
	s.sender.canceled = true
	s.sender.mu.Unlock()
	if !already {
		s.session.cancel(s.id, s.method, s.session.cfg)
	}
	s.finish()
}

// Outcome 返回发送方向已确认与不能确定的分段序号。
// 连接断开后未确认的分段不会也不会被自动重发。
func (s *BidiStream) Outcome() StreamOutcome { return s.sender.outcome() }

func (s *BidiStream) maybeFinish() {
	s.sender.mu.Lock()
	done := s.sender.sendClosed && s.sender.remoteClosed
	s.sender.mu.Unlock()
	if done {
		s.finish()
	}
}

func (s *BidiStream) finish() {
	s.once.Do(func() {
		close(s.done)
		s.session.removeBidi(s.id)
	})
}

// ServerBidiStream 是服务端一条双向数据流的句柄，语义与 BidiStream 对称。
type ServerBidiStream struct {
	id         uint64
	serializer byte
	st         *serverBidi
	write      func(context.Context, *Envelope) error
	connDone   <-chan struct{}
	recvEOF    bool
	mu         sync.Mutex
}

func (s *ServerBidiStream) Send(ctx context.Context, payload []byte, headers map[string]string) error {
	for {
		s.st.sender.mu.Lock()
		switch {
		case s.st.sender.canceled:
			s.st.sender.mu.Unlock()
			return ErrCanceled
		case s.st.sender.sendClosed:
			s.st.sender.mu.Unlock()
			return ErrStreamClosed
		}
		if len(s.st.sender.unacked) < bidiWindow {
			seq := s.st.sender.nextSeq
			s.st.sender.nextSeq++
			s.st.sender.unacked[seq] = struct{}{}
			s.st.sender.mu.Unlock()
			env := &Envelope{Type: TypeBidiData, ID: s.id, Seq: seq, Payload: payload, Headers: headers, Serializer: s.serializer}
			if err := s.write(ctx, env); err != nil {
				s.st.sender.ack(seq)
				return err
			}
			return nil
		}
		s.st.sender.mu.Unlock()
		select {
		case <-s.st.sender.ackNotify:
		case <-ctx.Done():
			return ctx.Err()
		case <-s.connDone:
			return ErrConnectionClosed
		}
	}
}

func (s *ServerBidiStream) Recv(ctx context.Context) ([]byte, error) {
	s.mu.Lock()
	if s.recvEOF {
		s.mu.Unlock()
		return nil, io.EOF
	}
	s.mu.Unlock()
	select {
	case env := <-s.st.recvCh:
		if env == nil {
			return nil, ErrConnectionClosed
		}
		s.st.sendAck(env.Seq)
		if env.Flags&FlagEnd != 0 {
			s.st.sender.mu.Lock()
			s.st.sender.remoteClosed = true
			s.st.sender.mu.Unlock()
			s.mu.Lock()
			s.recvEOF = true
			s.mu.Unlock()
			return nil, io.EOF
		}
		return env.Payload, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.connDone:
		return nil, ErrConnectionClosed
	}
}

func (s *ServerBidiStream) CloseSend(ctx context.Context) error {
	s.st.sender.mu.Lock()
	if s.st.sender.canceled {
		s.st.sender.mu.Unlock()
		return ErrCanceled
	}
	if s.st.sender.sendClosed {
		s.st.sender.mu.Unlock()
		return nil
	}
	s.st.sender.sendClosed = true
	seq := s.st.sender.nextSeq
	s.st.sender.nextSeq++
	s.st.sender.unacked[seq] = struct{}{}
	s.st.sender.mu.Unlock()
	return s.write(ctx, &Envelope{Type: TypeBidiData, ID: s.id, Seq: seq, Flags: FlagEnd, Serializer: s.serializer})
}

// Outcome 返回服务端发送方向已确认与不能确定的分段序号。
func (s *ServerBidiStream) Outcome() StreamOutcome { return s.st.sender.outcome() }
