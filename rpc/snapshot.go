package rpc

// SeqRange is a closed, one-based sequence interval: [Start, End].
type SeqRange struct {
	Start uint64
	End   uint64
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
	Credits     CreditSnapshot
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
	Connection    ConnectionSnapshot
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
