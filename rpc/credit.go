package rpc

import (
	"context"
	"errors"
	"sync"
)

// defaultStreamBuffer bounds the total unacknowledged stream payload bytes
// in flight on one connection.
const defaultStreamBuffer = int64(1 << 20)

// errStreamDone is returned by bufferCredit.acquire when the stream waiting
// for credit finished while waiting; callers map it to the stream's real
// terminal error.
var errStreamDone = errors.New("lrpc: stream finished while waiting for buffer credit")

// bufferCredit is a connection-wide budget of unacknowledged stream payload
// bytes. Data segments must acquire credit before they are sent and release
// it once the peer acknowledges them, so one large stream cannot exhaust
// memory and starve other streams. Control frames (ack, cancel, half-close)
// never consume credit, so cancellations and end-of-stream notifications
// always flow.
type bufferCredit struct {
	limit  int64
	mu     sync.Mutex
	used   int64
	notify chan struct{}
}

func newBufferCredit(limit int64) *bufferCredit {
	if limit <= 0 {
		limit = defaultStreamBuffer
	}
	return &bufferCredit{limit: limit, notify: make(chan struct{})}
}

// acquire blocks until n bytes of credit are available. A segment larger
// than the whole limit is allowed once the connection is fully idle, so a
// single oversized segment can never deadlock the connection. closed is the
// connection shutdown channel; done (optional) is the stream's.
func (b *bufferCredit) acquire(ctx context.Context, closed <-chan struct{}, done <-chan struct{}, n int64) error {
	for {
		b.mu.Lock()
		if b.used == 0 || b.used+n <= b.limit {
			b.used += n
			b.mu.Unlock()
			return nil
		}
		ch := b.notify
		b.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		case <-closed:
			return ErrConnectionClosed
		case <-done:
			return errStreamDone
		}
	}
}

func (b *bufferCredit) release(n int64) {
	if n <= 0 {
		return
	}
	b.mu.Lock()
	b.used -= n
	if b.used < 0 {
		b.used = 0
	}
	close(b.notify)
	b.notify = make(chan struct{})
	b.mu.Unlock()
}

// reset frees all credit and wakes every waiter; used when the connection
// closes so accounting stays explainable instead of leaking.
func (b *bufferCredit) reset() {
	b.mu.Lock()
	b.used = 0
	close(b.notify)
	b.notify = make(chan struct{})
	b.mu.Unlock()
}

func (b *bufferCredit) state() (used, limit int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used, b.limit
}
