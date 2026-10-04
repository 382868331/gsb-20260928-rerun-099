package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
	"weak"
)

// Finish reasons recorded by StreamSnapshot. A snapshot only records what
// was observed locally; an undetermined segment after a connection break
// is never rewritten into a success.
const (
	FinishReasonOpen             = ""
	FinishReasonEOF              = "eof"
	FinishReasonCanceled         = "canceled"
	FinishReasonReset            = "reset"
	FinishReasonConnectionClosed = "connection_closed"
	FinishReasonError            = "error"
)

// StreamSnapshot is a savable, point-in-time diagnostic view of one
// bidirectional stream. Segment numbers start at 1 and are independent
// per stream:
//
//   - [1, Acked] was confirmed by the peer (effect known);
//   - (Acked, Sent] was handed to the transport and is still pending;
//     after a connection break its outcome is undetermined.
//
// Snapshot values are plain data: capturing one never sends a frame,
// cancels a stream or replays a request.
type StreamSnapshot struct {
	StreamID     uint64 `json:"stream_id"`
	Method       string `json:"method"`
	Sent         uint64 `json:"sent"`
	Acked        uint64 `json:"acked"`
	PendingLo    uint64 `json:"pending_lo"`
	PendingHi    uint64 `json:"pending_hi"`
	Received     uint64 `json:"received"`
	SendClosed   bool   `json:"send_closed"`
	RecvClosed   bool   `json:"recv_closed"`
	Done         bool   `json:"done"`
	FinishReason string `json:"finish_reason"`
	Err          string `json:"err,omitempty"`
}

// Pending reports the inclusive range of segments handed to the
// transport but not yet confirmed, as [lo, hi]. ok is false when nothing
// is pending. After a disconnect these segments are undetermined, never
// silently marked successful.
func (s StreamSnapshot) Pending() (lo, hi uint64, ok bool) {
	if s.Sent <= s.Acked {
		return 0, 0, false
	}
	return s.Acked + 1, s.Sent, true
}

// Inflight is the number of unacknowledged segments, each consuming one
// grant from the connection-shared credit pool.
func (s StreamSnapshot) Inflight() uint64 {
	lo, hi, ok := s.Pending()
	if !ok {
		return 0
	}
	return hi - lo + 1
}

func (s StreamSnapshot) String() string {
	pending := "none"
	if lo, hi, ok := s.Pending(); ok {
		pending = fmt.Sprintf("[%d,%d]", lo, hi)
	}
	return fmt.Sprintf("stream %d %s: sent=%d acked=[1,%d] pending=%s received=%d finish=%s",
		s.StreamID, s.Method, s.Sent, s.Acked, pending, s.Received, s.FinishReason)
}

// ConnectionSnapshot is a savable diagnostic view of one client
// connection session, including the shared bidi credit pool at capture
// time. It stays readable after the connection closed.
type ConnectionSnapshot struct {
	TakenAt          time.Time        `json:"taken_at"`
	Closed           bool             `json:"closed"`
	ClosedAt         *time.Time       `json:"closed_at,omitempty"`
	TotalCredits     int              `json:"total_credits"`
	AvailableCredits int              `json:"available_credits"`
	InFlightCredits  int              `json:"inflight_credits"`
	Streams          []StreamSnapshot `json:"streams"`
}

// Stream finds the snapshot for one stream. ok is false if the stream is
// not part of this snapshot.
func (c *ConnectionSnapshot) Stream(id uint64) (StreamSnapshot, bool) {
	for _, st := range c.Streams {
		if st.StreamID == id {
			return st, true
		}
	}
	return StreamSnapshot{}, false
}

// JSON encodes the snapshot as indented JSON so it can be saved and
// inspected offline. The encoding contains diagnostic facts only.
func (c *ConnectionSnapshot) JSON() ([]byte, error) {
	if c.Streams == nil {
		c.Streams = []StreamSnapshot{}
	}
	return json.MarshalIndent(c, "", "  ")
}

// SaveJSON writes the snapshot to path. It performs no I/O on the RPC
// connection and triggers no request replay.
func (c *ConnectionSnapshot) SaveJSON(path string) error {
	data, err := c.JSON()
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func (c *ConnectionSnapshot) String() string {
	head := fmt.Sprintf("connection snapshot at %s: closed=%v credits=%d/%d available",
		c.TakenAt.Format(time.RFC3339), c.Closed, c.AvailableCredits, c.TotalCredits)
	lines := []string{head}
	for _, st := range c.Streams {
		lines = append(lines, "  "+st.String())
	}
	return strings.Join(lines, "\n")
}

// StreamAckProgress reports what was confirmed for one stream between two
// snapshots: segments in [NewlyConfirmedLo, NewlyConfirmedHi]
// (inclusive) moved from the pending range into the acked range.
type StreamAckProgress struct {
	StreamID         uint64 `json:"stream_id"`
	Method           string `json:"method"`
	WasAcked         uint64 `json:"was_acked"`
	NowAcked         uint64 `json:"now_acked"`
	NewlyConfirmedLo uint64 `json:"newly_confirmed_lo"`
	NewlyConfirmedHi uint64 `json:"newly_confirmed_hi"`
	Count            uint64 `json:"count"`
}

// SnapshotDiff compares two snapshots of the same connection. Only
// confirmed ranges are reported: a segment missing after a disconnect
// stays pending/undetermined and never appears as confirmed.
type SnapshotDiff struct {
	Confirmed      []StreamAckProgress `json:"confirmed"`
	NewStreams     []uint64            `json:"new_streams"`
	MissingStreams []uint64            `json:"missing_streams"`
}

// CompareSnapshots reports, per stream, which segment ranges became
// confirmed between old and new. Streams present in new only and old only
// are listed separately. Acks never move backwards; a decrease leaves the
// stream out of Confirmed rather than inventing a range.
func CompareSnapshots(old, new *ConnectionSnapshot) SnapshotDiff {
	var diff SnapshotDiff
	oldByID := make(map[uint64]StreamSnapshot, len(old.Streams))
	for _, st := range old.Streams {
		oldByID[st.StreamID] = st
	}
	seen := make(map[uint64]bool, len(new.Streams))
	for _, st := range new.Streams {
		seen[st.StreamID] = true
		prev, ok := oldByID[st.StreamID]
		if !ok {
			diff.NewStreams = append(diff.NewStreams, st.StreamID)
			continue
		}
		if st.Acked > prev.Acked {
			diff.Confirmed = append(diff.Confirmed, StreamAckProgress{
				StreamID:         st.StreamID,
				Method:           st.Method,
				WasAcked:         prev.Acked,
				NowAcked:         st.Acked,
				NewlyConfirmedLo: prev.Acked + 1,
				NewlyConfirmedHi: st.Acked,
				Count:            st.Acked - prev.Acked,
			})
		}
	}
	for _, st := range old.Streams {
		if !seen[st.StreamID] {
			diff.MissingStreams = append(diff.MissingStreams, st.StreamID)
		}
	}
	sort.Slice(diff.Confirmed, func(i, j int) bool { return diff.Confirmed[i].StreamID < diff.Confirmed[j].StreamID })
	sort.Slice(diff.NewStreams, func(i, j int) bool { return diff.NewStreams[i] < diff.NewStreams[j] })
	sort.Slice(diff.MissingStreams, func(i, j int) bool { return diff.MissingStreams[i] < diff.MissingStreams[j] })
	return diff
}

func (p StreamAckProgress) String() string {
	return fmt.Sprintf("stream %d %s: seq [%d,%d] confirmed (%d segment(s))",
		p.StreamID, p.Method, p.NewlyConfirmedLo, p.NewlyConfirmedHi, p.Count)
}

func finishReason(st BidiState) (reason, detail string) {
	if !st.Done {
		return FinishReasonOpen, ""
	}
	switch {
	case st.Err == nil:
		return FinishReasonEOF, ""
	case errors.Is(st.Err, context.Canceled):
		return FinishReasonCanceled, st.Err.Error()
	case errors.Is(st.Err, ErrStreamReset):
		return FinishReasonReset, st.Err.Error()
	case errors.Is(st.Err, ErrConnectionClosed):
		return FinishReasonConnectionClosed, st.Err.Error()
	default:
		return FinishReasonError, st.Err.Error()
	}
}

func (s *BidiStream) Snapshot() StreamSnapshot {
	state := s.State()
	reason, detail := finishReason(state)
	snap := StreamSnapshot{
		StreamID:     s.call.id,
		Method:       s.call.method,
		Sent:         state.Sent,
		Acked:        state.Acked,
		Received:     state.Received,
		SendClosed:   state.SendClosed,
		RecvClosed:   state.RecvClosed,
		Done:         state.Done,
		FinishReason: reason,
		Err:          detail,
	}
	if lo, hi, ok := snap.Pending(); ok {
		snap.PendingLo, snap.PendingHi = lo, hi
	}
	return snap
}

// ConnectionSnapshot captures the whole connection session this stream
// belongs to. It works after the connection closed.
func (s *BidiStream) ConnectionSnapshot() *ConnectionSnapshot {
	return s.session.Snapshot()
}

func (p *creditPool) available() int { return len(p.ch) }
func (p *creditPool) total() int     { return cap(p.ch) }

func (s *clientSession) registerBidi(stream *BidiStream) {
	s.streamsMu.Lock()
	s.bidiStreams[stream.call.id] = weak.Pointer[BidiStream](weak.Make(stream))
	s.streamsMu.Unlock()
}

// Snapshot records diagnostic facts for every still-referenced bidi stream
// on this session and the shared credit pool. It never blocks on I/O and
// never changes stream state. After the session closed it reports the
// credit pool as it was observed at shutdown.
func (s *clientSession) Snapshot() *ConnectionSnapshot {
	snap := &ConnectionSnapshot{
		TakenAt:      time.Now(),
		TotalCredits: s.bidiCredits.total(),
	}
	select {
	case <-s.closed:
		snap.Closed = true
		closedAt := s.closedAt
		snap.ClosedAt = &closedAt
		snap.AvailableCredits = s.creditsAtClose
	default:
		snap.AvailableCredits = s.bidiCredits.available()
	}
	snap.InFlightCredits = snap.TotalCredits - snap.AvailableCredits

	s.streamsMu.Lock()
	type streamRef struct {
		id  uint64
		ref weak.Pointer[BidiStream]
	}
	refs := make([]streamRef, 0, len(s.bidiStreams))
	for id, ref := range s.bidiStreams {
		refs = append(refs, streamRef{id: id, ref: ref})
	}
	s.streamsMu.Unlock()
	sort.Slice(refs, func(i, j int) bool { return refs[i].id < refs[j].id })
	for _, item := range refs {
		if stream := item.ref.Value(); stream != nil {
			snap.Streams = append(snap.Streams, stream.Snapshot())
		}
	}
	if snap.Streams == nil {
		snap.Streams = []StreamSnapshot{}
	}
	return snap
}

// Snapshot captures the client's current or last connection session,
// including after it closed. A new call after a break starts a fresh
// session that replaces the old one; save the snapshot beforehand if it
// is needed later. BidiStream.ConnectionSnapshot always reads the session
// the stream belongs to, even after Client.Close.
func (c *Client) Snapshot() (*ConnectionSnapshot, error) {
	c.mu.Lock()
	s := c.session
	closed := c.closed
	c.mu.Unlock()
	if s == nil {
		if closed {
			return nil, ErrConnectionClosed
		}
		return nil, ErrConnectionClosed
	}
	return s.Snapshot(), nil
}
