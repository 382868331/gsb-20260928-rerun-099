package rpc

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

// holdBidiHandler receives one segment, then blocks until released so the
// client can snapshot a pending (unconfirmed) segment.
func holdBidiHandler(release <-chan struct{}) func(context.Context, []byte, map[string]string, *ServerBidiStream) error {
	return func(ctx context.Context, _ []byte, _ map[string]string, stream *ServerBidiStream) error {
		if _, _, err := stream.Recv(ctx); err != nil {
			return err
		}
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		if _, _, err := stream.Recv(ctx); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	}
}

func TestSnapshotSaveLoadPreservesFinishReasons(t *testing.T) {
	release := make(chan struct{})
	server := NewServer(ServerConfig{Address: "pipe", MaxHandlers: 16})
	server.HandleBidi("echo", func(ctx context.Context, _ []byte, _ map[string]string, stream *ServerBidiStream) error {
		return echoBidiHandler(ctx, stream)
	})
	server.HandleBidi("hold", holdBidiHandler(release))
	t.Cleanup(func() { _ = server.Close() })
	client := dialTestClient(t, server, ClientConfig{})

	// Stream 1: normal EOF finish.
	eofStream, err := client.BidiStream(context.Background(), "echo", testPayload{Value: "eof"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := eofStream.Send(context.Background(), bidiSegment("eof", 1, 64), nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := eofStream.Recv(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := eofStream.CloseSend(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := eofStream.Recv(context.Background()); err != io.EOF {
		t.Fatalf("want io.EOF, got %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for !eofStream.Snapshot().Done && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	eofSnap := eofStream.Snapshot()
	if eofSnap.ReasonKind != CloseReasonEOF {
		t.Fatalf("normal finish reason = %q (%v), want eof", eofSnap.ReasonKind, eofSnap.CloseReason)
	}

	// Stream 2: explicit cancel.
	cancelStream, err := client.BidiStream(context.Background(), "hold", testPayload{Value: "cancel"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := cancelStream.Send(context.Background(), bidiSegment("cancel", 1, 128), nil); err != nil {
		t.Fatal(err)
	}
	cancelStream.Cancel()
	cancelSnap := cancelStream.Snapshot()
	if cancelSnap.ReasonKind != CloseReasonCanceled || !errors.Is(cancelSnap.CloseReason, context.Canceled) {
		t.Fatalf("cancel reason = %q (%v), want canceled", cancelSnap.ReasonKind, cancelSnap.CloseReason)
	}

	// Stream 3: send timeout while the peer holds the segment.
	timeoutStream, err := client.BidiStream(context.Background(), "hold", testPayload{Value: "timeout"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := timeoutStream.Send(context.Background(), bidiSegment("timeout", 1, 32), nil); err != nil {
		t.Fatal(err)
	}
	sendCtx, cancelSend := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelSend()
	if err := timeoutStream.Send(sendCtx, bidiSegment("timeout", 2, 32), nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("send timeout = %v, want deadline exceeded", err)
	}
	timeoutSnap := timeoutStream.Snapshot()
	if timeoutSnap.ReasonKind != CloseReasonTimeout {
		t.Fatalf("timeout reason = %q (%v), want timeout", timeoutSnap.ReasonKind, timeoutSnap.CloseReason)
	}
	if pending, ok := timeoutSnap.PendingRange(); !ok || pending != (SeqRange{Start: 2, End: 2}) {
		t.Fatalf("timeout pending = %v %t, want [2,2]", pending, ok)
	}

	// Stream 4: connection drop with an unconfirmed segment.
	dropStream, err := client.BidiStream(context.Background(), "hold", testPayload{Value: "drop"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := dropStream.Send(context.Background(), bidiSegment("drop", 1, 96), nil); err != nil {
		t.Fatal(err)
	}
	dropDone := make(chan error, 1)
	go func() { dropDone <- dropStream.Send(context.Background(), bidiSegment("drop", 2, 48), nil) }()
	time.Sleep(20 * time.Millisecond)
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-dropDone; err != nil && !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("drop send = %v", err)
	}
	dropSnap := dropStream.Snapshot()
	if dropSnap.ReasonKind != CloseReasonConnectionClosed || !dropSnap.Connection.Closed || dropSnap.Connection.ReasonKind != CloseReasonConnectionClosed {
		t.Fatalf("drop reason = %q conn=%q", dropSnap.ReasonKind, dropSnap.Connection.ReasonKind)
	}

	// Save all four, reload, and verify every finish reason survives.
	var buf bytes.Buffer
	snaps := []StreamSnapshot{eofSnap, cancelSnap, timeoutSnap, dropSnap}
	if err := SaveStreamSnapshots(&buf, snaps...); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadStreamSnapshots(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != len(snaps) {
		t.Fatalf("loaded %d snapshots, want %d", len(loaded), len(snaps))
	}
	wantKinds := []CloseReasonKind{CloseReasonEOF, CloseReasonCanceled, CloseReasonTimeout, CloseReasonConnectionClosed}
	for i, want := range wantKinds {
		got, ok := FindStreamSnapshot(loaded, snaps[i].StreamID)
		if !ok {
			t.Fatalf("snapshot for stream %d missing after reload", snaps[i].StreamID)
		}
		if got.ReasonKind != want || got.Method != snaps[i].Method {
			t.Fatalf("stream %d reloaded reason = %q method=%q, want %q", got.StreamID, got.ReasonKind, got.Method, want)
		}
		if got.CloseReason == nil || got.CloseReason.Error() != snaps[i].CloseReason.Error() {
			t.Fatalf("stream %d reloaded message = %v, want %v", got.StreamID, got.CloseReason, snaps[i].CloseReason)
		}
		if got.Sent != snaps[i].Sent || got.Acked != snaps[i].Acked {
			t.Fatalf("stream %d reloaded counters = %d/%d, want %d/%d", got.StreamID, got.Sent, got.Acked, snaps[i].Sent, snaps[i].Acked)
		}
		for seq, size := range snaps[i].SegmentSizes {
			if got.SegmentSizes[seq] != size {
				t.Fatalf("stream %d segment %d size = %d, want %d", got.StreamID, seq, got.SegmentSizes[seq], size)
			}
		}
	}
	// The reloaded drop snapshot still reports its pending range as unknown.
	reloadedDrop, _ := FindStreamSnapshot(loaded, dropSnap.StreamID)
	if pending, ok := reloadedDrop.PendingRange(); !ok || pending != (SeqRange{Start: 2, End: 2}) {
		t.Fatalf("reloaded pending = %v %t, want [2,2]", pending, ok)
	}
	if !reloadedDrop.Connection.Closed || reloadedDrop.Connection.ReasonKind != CloseReasonConnectionClosed {
		t.Fatalf("reloaded connection fact = %+v", reloadedDrop.Connection)
	}
	close(release)
}

func TestSavedSnapshotsAreStableAndCompareOnlyNewConfirmations(t *testing.T) {
	release := make(chan struct{})
	server := NewServer(ServerConfig{Address: "pipe", MaxHandlers: 16, StreamCredits: 4})
	server.HandleBidi("hold", holdBidiHandler(release))
	t.Cleanup(func() { _ = server.Close() })
	client := dialTestClient(t, server, ClientConfig{StreamCredits: 4})

	sizes := []int{16, 4096, 100000}
	streams := make([]*BidiStream, len(sizes))
	before := make([]StreamSnapshot, len(sizes))
	for i, size := range sizes {
		stream, err := client.BidiStream(context.Background(), "hold", testPayload{Value: strings.Repeat("s", i+1)}, nil)
		if err != nil {
			t.Fatal(err)
		}
		streams[i] = stream
		if err := stream.Send(context.Background(), bidiSegment("stable", 1, size), nil); err != nil {
			t.Fatal(err)
		}
		before[i] = stream.Snapshot()
	}

	// Save the first round, then let the second segment confirm.
	var buf bytes.Buffer
	if err := SaveStreamSnapshots(&buf, before...); err != nil {
		t.Fatal(err)
	}
	close(release)
	for i, size := range sizes {
		if err := streams[i].Send(context.Background(), bidiSegment("stable", 2, size*2), nil); err != nil {
			t.Fatal(err)
		}
	}
	after := make([]StreamSnapshot, len(streams))
	for i, stream := range streams {
		after[i] = stream.Snapshot()
	}

	// Reload the old save: it must not have moved with the live connection.
	loaded, err := LoadStreamSnapshots(&buf)
	if err != nil {
		t.Fatal(err)
	}
	for i := range before {
		got, ok := FindStreamSnapshot(loaded, before[i].StreamID)
		if !ok {
			t.Fatalf("stream %d missing after reload", before[i].StreamID)
		}
		if got.Acked != 1 || got.Sent != 1 || got.ReasonKind != CloseReasonNone || got.Connection.Closed {
			t.Fatalf("reloaded snapshot mutated with live connection: %+v", got)
		}
		// Only the genuinely newly confirmed interval may be reported.
		diff := CompareStreamSnapshots(got, after[i])
		if !diff.HasChange || diff.NewlyConfirmed != (SeqRange{Start: 2, End: 2}) {
			t.Fatalf("stream %d diff = %+v, want newly confirmed [2,2]", got.StreamID, diff)
		}
		if after[i].Acked != 2 || after[i].SegmentSizes[2] != sizes[i]*2 {
			t.Fatalf("stream %d after = %+v", got.StreamID, after[i])
		}
	}
	// Identical snapshots yield no confirmation; a reloaded snapshot can
	// never confirm anything by itself.
	for i := range before {
		if diff := CompareStreamSnapshots(loaded[i], loaded[i]); diff.HasChange {
			t.Fatalf("self compare reported change: %+v", diff)
		}
	}
}
