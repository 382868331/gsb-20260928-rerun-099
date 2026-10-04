package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"lrpc/demo"
	"lrpc/rpc"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:19090", "server address")
	token := flag.String("token", "local-demo-token", "bearer token")
	phase := flag.String("phase", "all", "stream or reconnect")
	outdir := flag.String("outdir", ".", "directory for diagnostic snapshot JSON files")
	flag.Parse()

	client, err := rpc.Dial(context.Background(), rpc.ClientConfig{
		Address: *addr,
		Headers: map[string]string{"authorization": "Bearer " + *token},
	})
	if err != nil {
		log.Fatal(err)
	}

	switch *phase {
	case "stream", "all":
		runStreamPhase(client)
		runBidiPhase(client)
		runReconnectPhase(client)
		_ = client.Close()
	case "snapshot":
		runSnapshotPhase(client, *outdir)
	case "reconnect":
		_ = client.Close()
		newClient, dialErr := rpc.Dial(context.Background(), rpc.ClientConfig{Address: *addr, Headers: map[string]string{"authorization": "Bearer " + *token}})
		if dialErr != nil {
			log.Fatal(dialErr)
		}
		defer newClient.Close()
		runReconnectPhase(newClient)
	default:
		log.Fatalf("unknown phase %q", *phase)
	}
}

func runStreamPhase(client *rpc.Client) {
	sum, _, err := rpc.Call[demo.AddRequest, demo.AddResponse](context.Background(), client, "demo.Add", demo.AddRequest{A: 40, B: 2}, nil)
	if err != nil {
		log.Fatalf("unary add: %v", err)
	}
	fmt.Printf("add result=%d\n", sum.Sum)

	stream, err := rpc.CallStream[demo.NumbersRequest, demo.NumbersItem](context.Background(), client, "demo.Numbers", demo.NumbersRequest{Count: 6}, nil)
	if err != nil {
		log.Fatal(err)
	}
	received := 0
	lastIndex := -1
	pacer := time.NewTicker(120 * time.Millisecond)
	defer pacer.Stop()
	for {
		<-pacer.C
		item, _, recvErr := stream.Recv(context.Background())
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			log.Fatalf("stream recv: %v", recvErr)
		}
		if item.Index != lastIndex+1 {
			log.Fatalf("stream order broken: got %d after %d", item.Index, lastIndex)
		}
		lastIndex = item.Index
		received++
	}
	if received != 6 {
		log.Fatalf("received %d chunks, want 6", received)
	}
	fmt.Printf("stream received=%d last=%d max_inflight=1 state=eof\n", received, lastIndex)

	watchCtx, cancel := context.WithCancel(context.Background())
	watch, err := rpc.CallStream[demo.WatchRequest, demo.NumbersItem](watchCtx, client, "demo.Watch", demo.WatchRequest{ID: "live"}, nil)
	if err != nil {
		log.Fatal(err)
	}
	first, _, err := watch.Recv(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	cancel()
	_, _, err = watch.Recv(context.Background())
	if !errors.Is(err, context.Canceled) {
		log.Fatalf("watch final error=%v, want context canceled", err)
	}
	fmt.Printf("watch first=%q state=canceled\n", first.Value)
	time.Sleep(50 * time.Millisecond)

	status, _, err := rpc.Call[demo.StatusRequest, demo.StatusResponse](context.Background(), client, "demo.Status", demo.StatusRequest{}, nil)
	if err != nil {
		log.Fatalf("status: %v", err)
	}
	fmt.Printf("server canceled_watches=%d state=observed\n", status.CanceledWatches)
}

func runBidiPhase(client *rpc.Client) {
	chat, err := rpc.CallBidi[demo.ChatRequest](context.Background(), client, "demo.Chat", demo.ChatRequest{ID: "room-1"}, nil)
	if err != nil {
		log.Fatal(err)
	}
	sizes := []int{4, 64, 1024, 16, 256}
	for i, size := range sizes {
		text := strings.Repeat("m", size)
		if err := rpc.BidiSend(context.Background(), rpc.JSONCodec{}, chat.Send, demo.ChatMessage{Seq: i + 1, Text: text}); err != nil {
			log.Fatalf("bidi send %d: %v", i+1, err)
		}
		echo, err := rpc.BidiRecv[demo.ChatMessage](context.Background(), rpc.JSONCodec{}, chat.Recv)
		if err != nil {
			log.Fatalf("bidi recv %d: %v", i+1, err)
		}
		if echo.Seq != i+1 || echo.Text != "echo:"+text {
			log.Fatalf("bidi echo %d mismatch", i+1)
		}
	}
	if err := chat.CloseSend(context.Background()); err != nil {
		log.Fatal(err)
	}
	sum, _, err := rpc.Call[demo.AddRequest, demo.AddResponse](context.Background(), client, "demo.Add", demo.AddRequest{A: 1, B: 2}, nil)
	if err != nil || sum.Sum != 3 {
		log.Fatalf("unary during half-closed bidi: %+v, %v", sum, err)
	}
	summary, err := rpc.BidiRecv[demo.ChatMessage](context.Background(), rpc.JSONCodec{}, chat.Recv)
	if err != nil {
		log.Fatalf("bidi summary: %v", err)
	}
	if summary.Text != "done:5" {
		log.Fatalf("bidi summary = %q", summary.Text)
	}
	if _, err := rpc.BidiRecv[demo.ChatMessage](context.Background(), rpc.JSONCodec{}, chat.Recv); err != io.EOF {
		log.Fatalf("bidi final recv = %v", err)
	}
	state := chat.State()
	if !state.Done || state.Err != nil || state.Sent != 5 || state.Acked != 5 || state.Received != 6 {
		log.Fatalf("bidi final state: %+v", state)
	}
	fmt.Printf("bidi sent=%d acked=%d received=%d state=eof\n", state.Sent, state.Acked, state.Received)
}

func runReconnectPhase(client *rpc.Client) {
	sum, _, err := rpc.Call[demo.AddRequest, demo.AddResponse](context.Background(), client, "demo.Add", demo.AddRequest{A: 100, B: 23}, nil)
	if err != nil {
		log.Fatalf("reconnected add: %v", err)
	}
	fmt.Printf("reconnect add=%d state=ok\n", sum.Sum)
}

// runSnapshotPhase exercises the savable diagnostic snapshots: it opens
// several concurrent bidi streams with differently sized segments, saves
// two snapshots while the connection is alive, then closes the connection
// (server is killed by the demo script) and saves a post-close snapshot.
// The diff shows exactly which ranges became confirmed between the first
// two snapshots; the final pending range stays undetermined and is never
// rewritten as success or replayed.
func runSnapshotPhase(client *rpc.Client, outdir string) {
	const streamCount = 3
	sizes := []int{16, 4096, 102400}
	tags := []string{"small", "medium", "large"}
	errCh := make(chan error, 16)
	var wg sync.WaitGroup
	streams := make([]*rpc.BidiStream, 0, streamCount)
	streamIDs := make([]uint64, 0, streamCount)
	for i := 0; i < streamCount; i++ {
		stream, err := rpc.CallBidi[demo.ChatRequest](context.Background(), client, "demo.SnapshotSink",
			demo.ChatRequest{ID: tags[i]}, map[string]string{"x-tag": tags[i]})
		if err != nil {
			log.Fatalf("snapshot stream %d: %v", i, err)
		}
		streams = append(streams, stream)
		streamIDs = append(streamIDs, stream.StreamID())
	}

	before, snapErr := client.Snapshot()
	if snapErr != nil {
		log.Fatal(snapErr)
	}
	if err := before.SaveJSON(filepath.Join(outdir, "snapshot-before.json")); err != nil {
		log.Fatalf("save before snapshot: %v", err)
	}
	fmt.Printf("snapshot before: %d streams, credits %d/%d available; saved snapshot-before.json\n",
		len(before.Streams), before.AvailableCredits, before.TotalCredits)

	// First segment per stream is delivered and acked by the sink, with
	// varying payload sizes so diagnostics map ranges to real flows.
	for i, stream := range streams {
		wg.Add(1)
		go func(i int, stream *rpc.BidiStream) {
			defer wg.Done()
			body := strings.Repeat(string(rune('a'+i)), sizes[i])
			err := rpc.BidiSend(context.Background(), rpc.JSONCodec{}, stream.Send,
				demo.ChatMessage{Seq: 1, Text: tags[i] + ":" + body})
			if err != nil {
				errCh <- fmt.Errorf("stream %s segment 1: %w", tags[i], err)
			}
		}(i, stream)
	}
	wg.Wait()
	select {
	case err := <-errCh:
		log.Fatal(err)
	default:
	}

	mid, snapErr := client.Snapshot()
	if snapErr != nil {
		log.Fatal(snapErr)
	}
	if err := mid.SaveJSON(filepath.Join(outdir, "snapshot-mid.json")); err != nil {
		log.Fatalf("save mid snapshot: %v", err)
	}
	diff := rpc.CompareSnapshots(before, mid)
	fmt.Printf("snapshot mid: %d streams, credits %d/%d available; saved snapshot-mid.json\n",
		len(mid.Streams), mid.AvailableCredits, mid.TotalCredits)
	for _, progress := range diff.Confirmed {
		fmt.Printf("  %s\n", progress)
	}

	// Second segments are queued to the transport but are never consumed
	// by the sink, so they stay in the undetermined (Acked, Sent] range.
	// The sends block on the one-segment window; start them without
	// waiting, then close the connection when the script kills the server
	// and collect the ErrConnectionClosed results.
	sendErr := make(chan error, streamCount)
	for i, stream := range streams {
		wg.Add(1)
		go func(i int, stream *rpc.BidiStream) {
			defer wg.Done()
			err := rpc.BidiSend(context.Background(), rpc.JSONCodec{}, stream.Send,
				demo.ChatMessage{Seq: 2, Text: tags[i] + ":pending"})
			sendErr <- err
		}(i, stream)
	}
	// Give the sends time to reach the transport window; the server is
	// killed by the demo script during this window.
	time.Sleep(1200 * time.Millisecond)
	client.Close()
	wg.Wait()
	for i := 0; i < streamCount; i++ {
		err := <-sendErr
		if err != nil && !errors.Is(err, rpc.ErrConnectionClosed) {
			log.Fatalf("stream %d pending send returned unexpected error: %v", i, err)
		}
	}

	afterSnap, snapErr := client.Snapshot()
	if snapErr != nil {
		log.Fatalf("post-close snapshot: %v", snapErr)
	}
	if err := afterSnap.SaveJSON(filepath.Join(outdir, "snapshot-after.json")); err != nil {
		log.Fatalf("save after snapshot: %v", err)
	}
	fmt.Printf("snapshot after: closed=%v credits %d/%d available at close; saved snapshot-after.json\n",
		afterSnap.Closed, afterSnap.AvailableCredits, afterSnap.TotalCredits)
	for _, st := range afterSnap.Streams {
		if st.FinishReason != rpc.FinishReasonConnectionClosed {
			log.Fatalf("stream %d finish=%q, want %q", st.StreamID, st.FinishReason, rpc.FinishReasonConnectionClosed)
		}
		lo, hi, ok := st.Pending()
		if !ok || lo != 2 || st.Acked != 1 || hi < 2 {
			log.Fatalf("stream %d ranges: acked=%d pending=%v(%d,%d)", st.StreamID, st.Acked, ok, lo, hi)
		}
		fmt.Printf("  confirmed=[1,1] undetermined=[2,%d] reason=%s stream_id=%d (%s)\n", hi, st.FinishReason, st.StreamID, st.Method)
	}
	fmt.Printf("snapshot stream ids=%v; mid->after confirmed ranges unchanged (no unknown rewritten to success)\n", streamIDs)
}
