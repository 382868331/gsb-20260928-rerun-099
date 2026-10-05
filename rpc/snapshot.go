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

// CloseReasonKind is the stable, saveable classification of why a stream
// or connection ended. Unlike a Go error value it survives being written
// to disk and read back, so a saved diagnostic snapshot keeps its finish
// reason across restarts.
type CloseReasonKind string

const (
	// CloseReasonNone means the stream or connection is still active.
	CloseReasonNone CloseReasonKind = ""
	// CloseReasonEOF means both directions half-closed normally.
	CloseReasonEOF CloseReasonKind = "eof"
	// CloseReasonCanceled means the local or peer side canceled the stream.
	CloseReasonCanceled CloseReasonKind = "canceled"
	// CloseReasonTimeout means a deadline expired before completion.
	CloseReasonTimeout CloseReasonKind = "timeout"
	// CloseReasonConnectionClosed means the transport dropped underneath.
	CloseReasonConnectionClosed CloseReasonKind = "connection_closed"
	// CloseReasonReset means the peer reset the stream.
	CloseReasonReset CloseReasonKind = "reset"
	// CloseReasonError covers any other failure; the message is kept.
	CloseReasonError CloseReasonKind = "error"
)

// ClassifyCloseReason maps a terminal error to its stable kind. Normal
// EOF, cancellation, timeout and connection loss stay distinguishable
// after the snapshot is saved and read back.
func ClassifyCloseReason(err error) CloseReasonKind {
	switch {
	case err == nil:
		return CloseReasonNone
	case errors.Is(err, io.EOF):
		return CloseReasonEOF
	case errors.Is(err, context.Canceled), errors.Is(err, ErrCanceled):
		return CloseReasonCanceled
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, ErrTimeout):
		return CloseReasonTimeout
	case errors.Is(err, ErrConnectionClosed):
		return CloseReasonConnectionClosed
	case errors.Is(err, ErrStreamReset):
		return CloseReasonReset
	default:
		return CloseReasonError
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
	Closed      bool
	CloseReason error
	// ReasonKind is the saveable form of CloseReason; it classifies normal
	// EOF, cancellation, timeout and connection loss so they remain
	// distinguishable after the snapshot is persisted and read back.
	ReasonKind CloseReasonKind
	Credits    CreditSnapshot
}

// StreamSnapshot is a saveable value copy of one bidirectional stream's
// diagnostic state. SegmentSizes maps one-based local send sequence numbers
// to payload bytes; confirmed sequences are always the prefix [1, Acked].
type StreamSnapshot struct {
	StreamID      uint64
	Method        string
	Sent          uint64
	Acked         uint64
	Received      uint64
	SegmentSizes  map[uint64]int
	SendClosed    bool
	ReceiveClosed bool
	Done          bool
	CloseReason   error
	// ReasonKind is the saveable form of CloseReason: a normal EOF finish,
	// an explicit cancel, a timeout and a connection drop keep distinct
	// kinds even after the snapshot is saved and reloaded.
	ReasonKind CloseReasonKind
	Connection ConnectionSnapshot
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

func (p *creditPool) snapshot() CreditSnapshot {
	limit := uint64(cap(p.ch))
	available := uint64(len(p.ch))
	return CreditSnapshot{Limit: limit, Available: available, Used: limit - available}
}

// savedStreamSnapshot is the wire form of StreamSnapshot. Go error values
// do not survive serialization, so the finish reason is stored as its
// stable CloseReasonKind plus the message text; reloading never
// reconstructs a live error and never turns an unknown result into a
// success.
type savedStreamSnapshot struct {
	StreamID      uint64            `json:"stream_id"`
	Method        string            `json:"method"`
	Sent          uint64            `json:"sent"`
	Acked         uint64            `json:"acked"`
	Received      uint64            `json:"received"`
	SegmentSizes  map[uint64]int    `json:"segment_sizes,omitempty"`
	SendClosed    bool              `json:"send_closed"`
	ReceiveClosed bool              `json:"receive_closed"`
	Done          bool              `json:"done"`
	ReasonKind    CloseReasonKind   `json:"reason_kind"`
	ReasonMessage string            `json:"reason_message,omitempty"`
	Connection    savedConnSnapshot `json:"connection"`
}

type savedConnSnapshot struct {
	Closed        bool            `json:"closed"`
	ReasonKind    CloseReasonKind `json:"reason_kind"`
	ReasonMessage string          `json:"reason_message,omitempty"`
	Credits       CreditSnapshot  `json:"credits"`
}

func reasonMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func saveStreamSnapshot(snap StreamSnapshot) savedStreamSnapshot {
	return savedStreamSnapshot{
		StreamID:      snap.StreamID,
		Method:        snap.Method,
		Sent:          snap.Sent,
		Acked:         snap.Acked,
		Received:      snap.Received,
		SegmentSizes:  snap.SegmentSizes,
		SendClosed:    snap.SendClosed,
		ReceiveClosed: snap.ReceiveClosed,
		Done:          snap.Done,
		ReasonKind:    snap.ReasonKind,
		ReasonMessage: reasonMessage(snap.CloseReason),
		Connection: savedConnSnapshot{
			Closed:        snap.Connection.Closed,
			ReasonKind:    snap.Connection.ReasonKind,
			ReasonMessage: reasonMessage(snap.Connection.CloseReason),
			Credits:       snap.Connection.Credits,
		},
	}
}

// SaveStreamSnapshots encodes snapshots into a stable, reloadable form.
// It performs no I/O beyond writing to w and never touches a live
// connection, so saving cannot trigger replay or mutate stream state.
func SaveStreamSnapshots(w io.Writer, snaps ...StreamSnapshot) error {
	saved := make([]savedStreamSnapshot, len(snaps))
	for i, snap := range snaps {
		saved[i] = saveStreamSnapshot(snap)
	}
	return json.NewEncoder(w).Encode(saved)
}

// LoadStreamSnapshots reads snapshots saved by SaveStreamSnapshots. The
// finish reason comes back as its stable CloseReasonKind plus the original
// message text, keyed by the original StreamID and sequence numbers, so a
// reloaded snapshot can still be queried by stream and segment. Reloaded
// snapshots are plain values: they cannot send, replay, or confirm
// anything, and pending ranges stay pending.
func LoadStreamSnapshots(r io.Reader) ([]StreamSnapshot, error) {
	var saved []savedStreamSnapshot
	if err := json.NewDecoder(r).Decode(&saved); err != nil {
		return nil, err
	}
	snaps := make([]StreamSnapshot, len(saved))
	for i, s := range saved {
		snaps[i] = StreamSnapshot{
			StreamID:      s.StreamID,
			Method:        s.Method,
			Sent:          s.Sent,
			Acked:         s.Acked,
			Received:      s.Received,
			SegmentSizes:  s.SegmentSizes,
			SendClosed:    s.SendClosed,
			ReceiveClosed: s.ReceiveClosed,
			Done:          s.Done,
			ReasonKind:    s.ReasonKind,
			Connection: ConnectionSnapshot{
				Closed:     s.Connection.Closed,
				ReasonKind: s.Connection.ReasonKind,
				Credits:    s.Connection.Credits,
			},
		}
		if s.ReasonMessage != "" {
			snaps[i].CloseReason = errors.New(s.ReasonMessage)
		}
		if s.Connection.ReasonMessage != "" {
			snaps[i].Connection.CloseReason = errors.New(s.Connection.ReasonMessage)
		}
	}
	return snaps, nil
}

// FindStreamSnapshot returns the snapshot for streamID, whether live or
// reloaded from a save. Streams are correlated by their original StreamID
// and Method, never by position.
func FindStreamSnapshot(snaps []StreamSnapshot, streamID uint64) (StreamSnapshot, bool) {
	for _, snap := range snaps {
		if snap.StreamID == streamID {
			return snap, true
		}
	}
	return StreamSnapshot{}, false
}
