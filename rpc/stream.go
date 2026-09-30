package rpc

import (
	"strconv"
	"sync"
)

const defaultStreamWindow = 1 << 16

// StreamItem is one consecutive fragment delivered to the reading side.
type StreamItem struct {
	Payload []byte
	Headers map[string]string
}

func parseAck(value string) int64 {
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// streamMailbox is a per-stream ordered queue with no byte-based blocking of
// the connection reader: flow control happens via credit frames (acks), so a
// full stream never stalls delivery to other streams/unary calls. A reader
// that stops draining simply stops granting credit and the sender stops.
type streamMailbox struct {
	mu     sync.Mutex
	queue  []*Envelope
	queued int
	closed bool
	cond   *sync.Cond
}

func newStreamMailbox() *streamMailbox {
	m := &streamMailbox{}
	m.cond = sync.NewCond(&m.mu)
	return m
}

func (m *streamMailbox) offer(env *Envelope) {
	m.mu.Lock()
	if !m.closed {
		m.queue = append(m.queue, env)
		if env.Type == TypeStream && env.Error == "" {
			m.queued += len(env.Payload)
		}
		m.cond.Broadcast()
	}
	m.mu.Unlock()
}

// next returns the next frame in order, blocking until one is available.
func (m *streamMailbox) next() *Envelope {
	m.mu.Lock()
	defer m.mu.Unlock()
	for len(m.queue) == 0 && !m.closed {
		m.cond.Wait()
	}
	if len(m.queue) == 0 {
		return nil
	}
	env := m.queue[0]
	m.queue = m.queue[1:]
	if env.Type == TypeStream && env.Error == "" {
		m.queued -= len(env.Payload)
		if m.queued < 0 {
			m.queued = 0
		}
		m.cond.Broadcast()
	}
	return env
}

func (m *streamMailbox) queuedBytes() int {
	m.mu.Lock()
	n := m.queued
	m.mu.Unlock()
	return n
}

func (m *streamMailbox) fail() {
	m.mu.Lock()
	m.closed = true
	m.queue = nil
	m.queued = 0
	m.cond.Broadcast()
	m.mu.Unlock()
}
