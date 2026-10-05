package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"io"
)

// SeqRange is a closed, one-based sequence interval: [Start, End].
type SeqRange struct {
	Start uint64
	End   uint64
}

// EndReason is a stable, serializable classification of why a stream or a
// connection stopped. The zero value means "still active" and older snapshot
// files that omit the field decode as EndReasonActive rather than silently
// becoming an error.
type EndReason string

const (
	EndReasonActive   EndReason = ""             // stream/connection still usable
	EndReasonEOF      EndReason = "eof"          // clean finish: both directions drained
	EndReasonCanceled EndReason = "canceled"     // this side canceled the stream
	EndReasonReset    EndReason = "reset"        // peer reset the stream
	EndReasonTimeout  EndReason = "timeout"      // call deadline/timeout elapsed
	EndReasonBroken   EndReason = "broken"       // underlying connection broke
	EndReasonFailed   EndReason = "failed"       // peer/application error frame
	EndReasonBadFrame EndReason = "bad_frame"    // protocol/frame error
	EndReasonUnknown  EndReason = "unknown"      // terminal, reason not classifiable
)

// Err maps the stable code back to the sentinel error used by live calls,
// so a snapshot reloaded after a process restart still distinguishes a
// clean EOF from cancellation, timeout and connection loss. EndReasonActive
// and EndReasonUnknown return nil.
func (r EndReason) Err() error {
	switch r {
	case EndReasonEOF:
		return io.EOF
	case EndReasonCanceled:
		return context.Canceled
	case EndReasonReset:
		return ErrStreamReset
	case EndReasonTimeout:
		return context.DeadlineExceeded
	case EndReasonBroken:
		return ErrConnectionClosed
	case EndReasonBadFrame:
		return ErrBadFrame
	case EndReasonFailed, EndReasonUnknown:
		return nil
	default:
		return nil
	}
}

func classifyEndReason(err error) EndReason {
	switch {
	case err == nil:
		return EndReasonActive
	case errors.Is(err, io.EOF):
		return EndReasonEOF
	case errors.Is(err, context.Canceled):
		return EndReasonCanceled
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, ErrTimeout):
		return EndReasonTimeout
	case errors.Is(err, ErrConnectionClosed):
		return EndReasonBroken
	case errors.Is(err, ErrStreamReset):
		return EndReasonReset
	case errors.Is(err, ErrBadFrame):
		return EndReasonBadFrame
	default:
		var callErr *CallError
		if errors.As(err, &callErr) {
			return EndReasonFailed
		}
		return EndReasonUnknown
	}
}

// CreditSnapshot is an instantaneous, connection-shared flow-control view.
// Used counts grants currently held by unacknowledged stream segments.
type CreditSnapshot struct {
	Limit     uint64
	Available uint64
	Used      uint64
}

// ConnectionSnapshot records only diagnostic connection facts. It never
// owns a connection and taking it does not send or replay a frame.
type ConnectionSnapshot struct {
	Closed bool `json:"closed"`
	// Reason is the stable, persistable close classification. CloseReason
	// is the live error for in-process callers and is intentionally not
	// marshaled (error values cannot round-trip); use Reason/EndError after
	// a reload.
	Reason      EndReason      `json:"reason"`
	CloseReason error          `json:"-"`
	Credits     CreditSnapshot `json:"credits"`
}

// StreamSnapshot is a saveable value copy of one bidirectional stream's
// diagnostic state. SegmentSizes maps one-based local send sequence numbers
// to payload bytes; confirmed sequences are always the prefix [1, Acked].
type StreamSnapshot struct {
	StreamID      uint64            `json:"stream_id"`
	Method        string            `json:"method"`
	Sent          uint64            `json:"sent"`
	Acked         uint64            `json:"acked"`
	Received      uint64            `json:"received"`
	SegmentSizes  map[uint64]int    `json:"segment_sizes"`
	SendClosed    bool              `json:"send_closed"`
	ReceiveClosed bool              `json:"receive_closed"`
	Done          bool              `json:"done"`
	Reason        EndReason         `json:"reason"`
	// CloseReason is the live terminal error and is not marshaled; the
	// stable Reason code survives save/reload and reconstructs it via
	// EndError. CloseReasonText only preserves messages for unclassifiable
	// application errors.
	CloseReason     error  `json:"-"`
	CloseReasonText string `json:"close_reason_text,omitempty"`
	Connection    ConnectionSnapshot `json:"connection"`
}

// End returns the stable terminal reason of the stream. For snapshots
// produced by this process it matches the error returned by the live
// Recv/Send calls; for reloaded snapshots it is reconstructed from the
// saved code, so EOF, cancellation, timeout and disconnect stay distinct.
func (s StreamSnapshot) End() EndReason {
	return s.Reason
}

// EndError returns the sentinel error corresponding to the saved end
// reason (io.EOF, context.Canceled, context.DeadlineExceeded,
// ErrConnectionClosed, ErrStreamReset, ...). It returns nil while the
// stream is active.
func (s StreamSnapshot) EndError() error {
	return s.Reason.Err()
}

// SegmentStatus is the known fate of one local send sequence number.
type SegmentStatus string

const (
	SegmentConfirmed SegmentStatus = "confirmed" // peer acknowledged the segment
	SegmentPending   SegmentStatus = "pending"   // handed to transport, result unknown
	SegmentUnknown   SegmentStatus = "unknown"   // sequence was never sent
)

// Segment reports the recorded size and confirmed/pending status of one
// local send sequence number. It is a pure read of the saved snapshot and
// works identically before and after persistence/reload.
func (s StreamSnapshot) Segment(seq uint64) (size int, status SegmentStatus, ok bool) {
	size, ok = s.SegmentSizes[seq]
	if !ok {
		return 0, SegmentUnknown, false
	}
	if seq <= s.Acked {
		return size, SegmentConfirmed, true
	}
	return size, SegmentPending, true
}

// ConfirmedRange returns [1, Acked] when at least one segment is confirmed.
func (s StreamSnapshot) ConfirmedRange() (SeqRange, bool) {
	if s.Acked == 0 {
		return SeqRange{}, false
	}
	return SeqRange{Start: 1, End: s.Acked}, true
}

// PendingRange returns sequences handed to the transport without a peer
// acknowledgment. Their result is unknown after a connection failure and
// must never be reported as successful.
func (s StreamSnapshot) PendingRange() (SeqRange, bool) {
	if s.Sent <= s.Acked {
		return SeqRange{}, false
	}
	return SeqRange{Start: s.Acked + 1, End: s.Sent}, true
}

// StreamSnapshotComparison reports confirmations gained between two saved
// snapshots of the same stream. ACKs are cumulative and contiguous.
type StreamSnapshotComparison struct {
	StreamID       uint64
	Method         string
	Before         StreamSnapshot
	After          StreamSnapshot
	NewlyConfirmed SeqRange
	HasChange      bool
}

// CompareStreamSnapshots computes the newly confirmed interval without
// performing I/O. Mismatched stream identities produce no interval.
func CompareStreamSnapshots(before, after StreamSnapshot) StreamSnapshotComparison {
	diff := StreamSnapshotComparison{StreamID: after.StreamID, Method: after.Method, Before: before, After: after}
	if before.StreamID != after.StreamID || before.Method != after.Method {
		return diff
	}
	if after.Acked > before.Acked {
		diff.NewlyConfirmed = SeqRange{Start: before.Acked + 1, End: after.Acked}
		diff.HasChange = true
	}
	return diff
}

// MarshalJSON persists the snapshot with stable reason codes instead of
// error interfaces (which cannot round-trip through JSON).
func (s StreamSnapshot) MarshalJSON() ([]byte, error) {
	type wire struct {
		StreamID      uint64            `json:"stream_id"`
		Method        string            `json:"method"`
		Sent          uint64            `json:"sent"`
		Acked         uint64            `json:"acked"`
		Received      uint64            `json:"received"`
		SegmentSizes  map[uint64]int    `json:"segment_sizes"`
		SendClosed    bool              `json:"send_closed"`
		ReceiveClosed bool              `json:"receive_closed"`
		Done          bool              `json:"done"`
		Reason        EndReason         `json:"reason"`
		CloseReason   string            `json:"close_reason,omitempty"`
		Connection    ConnectionSnapshot `json:"connection"`
	}
	out := wire{
		StreamID: s.StreamID, Method: s.Method, Sent: s.Sent, Acked: s.Acked,
		Received: s.Received, SegmentSizes: s.SegmentSizes,
		SendClosed: s.SendClosed, ReceiveClosed: s.ReceiveClosed, Done: s.Done,
		Reason: s.Reason, Connection: s.Connection,
	}
	if s.CloseReason != nil {
		out.CloseReason = s.CloseReason.Error()
	}
	return json.Marshal(out)
}

// UnmarshalJSON restores a saved snapshot. Live-call sentinel errors stay
// available through EndError; the decoded snapshot never touches a
// connection and cannot trigger any replay.
func (s *StreamSnapshot) UnmarshalJSON(data []byte) error {
	type wire struct {
		StreamID      uint64            `json:"stream_id"`
		Method        string            `json:"method"`
		Sent          uint64            `json:"sent"`
		Acked         uint64            `json:"acked"`
		Received      uint64            `json:"received"`
		SegmentSizes  map[uint64]int    `json:"segment_sizes"`
		SendClosed    bool              `json:"send_closed"`
		ReceiveClosed bool              `json:"receive_closed"`
		Done          bool              `json:"done"`
		Reason        EndReason         `json:"reason"`
		CloseReason   string            `json:"close_reason,omitempty"`
		Connection    ConnectionSnapshot `json:"connection"`
	}
	var in wire
	if err := json.Unmarshal(data, &in); err != nil {
		return err
	}
	s.StreamID = in.StreamID
	s.Method = in.Method
	s.Sent = in.Sent
	s.Acked = in.Acked
	s.Received = in.Received
	s.SegmentSizes = in.SegmentSizes
	s.SendClosed = in.SendClosed
	s.ReceiveClosed = in.ReceiveClosed
	s.Done = in.Done
	s.Reason = in.Reason
	s.CloseReasonText = in.CloseReason
	s.Connection = in.Connection
	if s.SegmentSizes == nil {
		s.SegmentSizes = map[uint64]int{}
	}
	return nil
}

// SnapshotStore is a saveable collection of stream snapshots keyed by the
// real on-wire StreamID together with the method name. Snapshots are frozen
// values: storing or reloading them never sends a frame, and later changes
// of the live connection cannot mutate a previously stored snapshot.
type SnapshotStore struct {
	streams map[streamKey]StreamSnapshot
}

type streamKey struct {
	StreamID uint64
	Method   string
}

// NewSnapshotStore creates an empty snapshot store.
func NewSnapshotStore() *SnapshotStore {
	return &SnapshotStore{streams: make(map[streamKey]StreamSnapshot)}
}

// Put stores a value copy of the snapshot, keyed by StreamID and method.
func (st *SnapshotStore) Put(snap StreamSnapshot) {
	if st.streams == nil {
		st.streams = make(map[streamKey]StreamSnapshot)
	}
	sizes := make(map[uint64]int, len(snap.SegmentSizes))
	for seq, size := range snap.SegmentSizes {
		sizes[seq] = size
	}
	snap.SegmentSizes = sizes
	st.streams[streamKey{StreamID: snap.StreamID, Method: snap.Method}] = snap
}

// Get retrieves a snapshot by its original stream ID and method.
func (st *SnapshotStore) Get(streamID uint64, method string) (StreamSnapshot, bool) {
	snap, ok := st.streams[streamKey{StreamID: streamID, Method: method}]
	return snap, ok
}

// GetByID retrieves a snapshot using only the original stream ID, which is
// unique within a connection. It returns false if no stream carries it.
func (st *SnapshotStore) GetByID(streamID uint64) (StreamSnapshot, bool) {
	for key, snap := range st.streams {
		if key.StreamID == streamID {
			return snap, true
		}
	}
	return StreamSnapshot{}, false
}

// Streams returns all stored snapshots in stream-ID order.
func (st *SnapshotStore) Streams() []StreamSnapshot {
	out := make([]StreamSnapshot, 0, len(st.streams))
	for _, snap := range st.streams {
		out = append(out, snap)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1].StreamID > out[j].StreamID; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}

// Segment finds one stream by original ID/method and reports the recorded
// fate of the given local send sequence number after a save/reload cycle.
func (st *SnapshotStore) Segment(streamID uint64, method string, seq uint64) (size int, status SegmentStatus, ok bool) {
	snap, ok := st.Get(streamID, method)
	if !ok {
		return 0, SegmentUnknown, false
	}
	return snap.Segment(seq)
}

type persistedStore struct {
	Version int              `json:"version"`
	Streams []StreamSnapshot `json:"streams"`
}

// Save serializes all snapshots with their stable end reasons and credit
// facts. It performs no I/O on any RPC connection.
func (st *SnapshotStore) Save() ([]byte, error) {
	return json.Marshal(persistedStore{Version: 1, Streams: st.Streams()})
}

// LoadSnapshotStore parses bytes produced by Save. Reloading only restores
// diagnostic values: ranges and reasons can be queried again, but no frame
// is sent, no request is retried, and an undetermined segment stays
// pending instead of becoming successful.
func LoadSnapshotStore(data []byte) (*SnapshotStore, error) {
	var p persistedStore
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, err
	}
	st := NewSnapshotStore()
	for _, snap := range p.Streams {
		st.Put(snap)
	}
	return st, nil
}

func (p *creditPool) snapshot() CreditSnapshot {
	limit := uint64(cap(p.ch))
	available := uint64(len(p.ch))
	return CreditSnapshot{Limit: limit, Available: available, Used: limit - available}
}
