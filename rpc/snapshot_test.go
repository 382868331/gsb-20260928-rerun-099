package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestSnapshotLiveReportsRangesSharedCreditsAndDiff(t *testing.T) {
	server := newBidiTestServer(t)
	client := dialTestClient(t, server, ClientConfig{StreamCredits: 4})

	sizes := [][]int{{1, 4096}, {100000, 3}}
	tags := []string{"snap-a", "snap-b"}
	streams := make([]*BidiStream, len(sizes))
	for i := range sizes {
		stream, err := client.BidiStream(context.Background(), "echo", testPayload{Value: tags[i]}, nil)
		if err != nil {
			t.Fatal(err)
		}
		streams[i] = stream
	}

	before, err := client.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if before.Closed || len(before.Streams) != 2 || before.TotalCredits != 4 {
		t.Fatalf("before snapshot: %+v", before)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	for i, stream := range streams {
		wg.Add(1)
		go func(i int, stream *BidiStream) {
			defer wg.Done()
			seq := 1
			if err := stream.Send(context.Background(), bidiSegment(tags[i], seq, sizes[i][seq-1]), nil); err != nil {
				errCh <- err
				return
			}
			if _, _, err := stream.Recv(context.Background()); err != nil {
				errCh <- err
			}
		}(i, stream)
	}
	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}

	mid, err := client.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	diff := CompareSnapshots(before, mid)
	if len(diff.Confirmed) != 2 {
		t.Fatalf("confirmed entries=%d, want 2: %+v", len(diff.Confirmed), diff.Confirmed)
	}
	for _, progress := range diff.Confirmed {
		if progress.WasAcked != 0 || progress.NowAcked != 1 || progress.NewlyConfirmedLo != 1 || progress.NewlyConfirmedHi != 1 || progress.Count != 1 {
			t.Fatalf("unexpected progress: %+v", progress)
		}
	}
	for _, st := range mid.Streams {
		if st.Acked != 1 || st.Sent != 1 || st.FinishReason != FinishReasonOpen {
			t.Fatalf("stream %d snapshot: %+v", st.StreamID, st)
		}
		if lo, hi, ok := st.Pending(); ok {
			t.Fatalf("stream %d pending=[%d,%d], want none", st.StreamID, lo, hi)
		}
	}

	// Confirm one more segment on stream 0 only: diff must name the real
	// stream id and exact range.
	if err := streams[0].Send(context.Background(), bidiSegment(tags[0], 2, sizes[0][1]), nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := streams[0].Recv(context.Background()); err != nil {
		t.Fatal(err)
	}
	after, err := client.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	diff = CompareSnapshots(mid, after)
	if len(diff.Confirmed) != 1 {
		t.Fatalf("confirmed entries=%+v", diff.Confirmed)
	}
	p := diff.Confirmed[0]
	if p.StreamID != streams[0].StreamID() || p.Method != "echo" || p.NewlyConfirmedLo != 2 || p.NewlyConfirmedHi != 2 || p.Count != 1 {
		t.Fatalf("unexpected second diff: %+v", p)
	}

	// Savable JSON round-trips and keeps every field needed to correlate
	// diagnostics with real streams and sequence numbers.
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.json")
	if err := after.SaveJSON(path); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var loaded ConnectionSnapshot
	if err := json.Unmarshal(raw, &loaded); err != nil {
		t.Fatal(err)
	}
	st, ok := loaded.Stream(streams[0].StreamID())
	if !ok || st.Acked != 2 || st.Method != "echo" {
		t.Fatalf("loaded snapshot mismatch: %+v ok=%v", st, ok)
	}
}

func TestSnapshotAfterDisconnectKeepsUndeterminedRanges(t *testing.T) {
	server := newBidiTestServer(t)
	client := dialTestClient(t, server, ClientConfig{StreamCredits: 8})

	stream, err := client.BidiStream(context.Background(), "sink-once", testPayload{Value: "x"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(context.Background(), bidiSegment("snap-lost", 1, 32), nil); err != nil {
		t.Fatal(err)
	}
	blocked := make(chan error, 1)
	go func() {
		blocked <- stream.Send(context.Background(), bidiSegment("snap-lost", 2, 64), nil)
	}()
	time.Sleep(100 * time.Millisecond)

	before, err := client.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	st, _ := before.Stream(stream.StreamID())
	if st.Acked != 1 || st.Sent != 2 {
		t.Fatalf("pre-close stream: %+v", st)
	}
	if lo, hi, ok := st.Pending(); !ok || lo != 2 || hi != 2 {
		t.Fatalf("pre-close pending: %d,%d,%v", lo, hi, ok)
	}
	if got := before.InFlightCredits; got != 1 {
		t.Fatalf("inflight credits=%d, want 1", got)
	}

	client.session.close(ErrConnectionClosed)
	select {
	case err := <-blocked:
		if !errors.Is(err, ErrConnectionClosed) {
			t.Fatalf("blocked send = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked send did not end on close")
	}

	after := stream.ConnectionSnapshot()
	if !after.Closed || after.ClosedAt == nil {
		t.Fatalf("post-close snapshot: %+v", after)
	}
	fromClient, err := client.Snapshot()
	if err != nil || !fromClient.Closed {
		t.Fatalf("client post-close snapshot: %+v err=%v", fromClient, err)
	}
	if after.AvailableCredits+after.InFlightCredits != after.TotalCredits {
		t.Fatalf("credit accounting: %+v", after)
	}
	st, _ = after.Stream(stream.StreamID())
	if st.FinishReason != FinishReasonConnectionClosed || st.Err == "" {
		t.Fatalf("post-close stream: %+v", st)
	}
	if st.Acked != 1 || st.Sent != 2 || !st.Done {
		t.Fatalf("post-close counters: %+v", st)
	}
	if lo, hi, ok := st.Pending(); !ok || lo != 2 || hi != 2 {
		t.Fatalf("post-close pending must stay undetermined: %d,%d,%v", lo, hi, ok)
	}
	// Nothing becomes confirmed after the break.
	diff := CompareSnapshots(before, after)
	if len(diff.Confirmed) != 0 {
		t.Fatalf("disconnect confirmed ranges: %+v", diff.Confirmed)
	}
}
