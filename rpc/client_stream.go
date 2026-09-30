package rpc

import (
	"context"
	"io"
	"strconv"
	"sync"
)

// Stream is a client handle for a server-streaming call. Recv returns
// consecutive fragments in order; io.EOF marks the clean end of the stream.
// Cancel (or canceling the call context) ends waiting on both sides and
// releases every unconsumed fragment.
type Stream struct {
	s        *clientSession
	id       uint64
	method   string
	cfg      ClientConfig
	mbox     *streamMailbox
	codec    PayloadCodec
	assemble []byte

	closeOnce sync.Once
	closed    bool
	closeMu   sync.Mutex
	terminal  error
	doneCh    chan struct{}
}

// Recv returns the next logical fragment. Chunked frames (HeaderStreamMore)
// are reassembled transparently. io.EOF is returned when the stream finishes.
func (st *Stream) Recv(ctx context.Context) (StreamItem, error) {
	for {
		env := st.mbox.next()
		if env == nil {
			return StreamItem{}, st.terminalErr()
		}
		if len(st.assemble) > 0 {
			st.assemble = append(st.assemble, env.Payload...)
		} else {
			st.assemble = append([]byte(nil), env.Payload...)
		}
		more := env.Headers != nil && env.Headers[HeaderStreamMore] != ""
		consumed := int64(len(env.Payload))
		// The frame has already left the byte-bounded mailbox, so grant its
		// bytes now. A reader that stops calling Recv stops granting, and the
		// sender blocks after at most one window of in-flight data.
		st.sendAck(consumed)
		if more {
			continue
		}
		item := StreamItem{Payload: st.assemble, Headers: env.Headers}
		st.assemble = nil
		if env.Error != "" || (env.Headers != nil && env.Headers[HeaderStreamError] != "") {
			msg := env.Error
			if msg == "" {
				msg = env.Headers[HeaderStreamError]
			}
			st.finishLocked(&CallError{Message: msg})
			return StreamItem{}, &CallError{Message: msg}
		}
		if env.Headers != nil && env.Headers[HeaderStreamEnd] != "" {
			st.finishLocked(io.EOF)
			return StreamItem{}, io.EOF
		}
		return item, nil
	}
}

func (st *Stream) sendAck(n int64) {
	if n <= 0 {
		return
	}
	frame, err := EncodeEnvelope(&Envelope{
		Type:       TypeStream,
		ID:         st.id,
		Method:     st.method,
		Headers:    map[string]string{HeaderStreamAck: strconv.FormatInt(n, 10)},
		Serializer: st.cfg.Codec.ID(),
	}, st.cfg.MaxFrameSize)
	if err != nil {
		return
	}
	_ = st.s.enqueue(frame)
}

// BufferedBytes reports fragment payload bytes received but not yet consumed.
func (st *Stream) BufferedBytes() int { return st.mbox.queuedBytes() }

// Cancel stops waiting locally, sends a cancel frame and frees buffered data.
func (st *Stream) Cancel() {
	st.closeOnce.Do(func() {
		close(st.doneCh)
		st.closeMu.Lock()
		st.closed = true
		st.terminal = context.Canceled
		st.closeMu.Unlock()
		st.s.removeStream(st.id)
		st.s.cancel(st.id, st.method, st.cfg)
		st.mbox.fail()
	})
}

// fail is used by session teardown (disconnect), not by the user.
func (st *Stream) fail(err error) {
	st.closeOnce.Do(func() {
		close(st.doneCh)
		st.closeMu.Lock()
		st.closed = true
		st.terminal = err
		st.closeMu.Unlock()
		st.mbox.fail()
	})
}

func (st *Stream) finishLocked(err error) {
	st.closeOnce.Do(func() {
		close(st.doneCh)
		st.closeMu.Lock()
		st.closed = true
		st.terminal = err
		st.closeMu.Unlock()
		st.s.removeStream(st.id)
		st.mbox.fail()
	})
}

func (st *Stream) terminalErr() error {
	st.closeMu.Lock()
	err := st.terminal
	st.closeMu.Unlock()
	if err == nil {
		return ErrConnectionClosed
	}
	return err
}

// Decode is a convenience helper for typed fragments.
func (st *Stream) Decode(item StreamItem, value any) error {
	return st.codec.Decode(item.Payload, value)
}
