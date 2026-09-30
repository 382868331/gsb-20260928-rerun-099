package demo

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"lrpc/rpc"
)

type AddRequest struct {
	A int `json:"a"`
	B int `json:"b"`
}

type AddResponse struct {
	Sum int `json:"sum"`
}

type DelayRequest struct {
	Value string        `json:"value"`
	Delay time.Duration `json:"delay"`
}

type DelayResponse struct {
	Value string `json:"value"`
}

type JobRequest struct {
	ID string `json:"id"`
}

type JobResponse struct {
	Done bool `json:"done"`
}

type NumbersRequest struct {
	Count int `json:"count"`
}

type NumbersItem struct {
	Index int    `json:"index"`
	Value string `json:"value"`
}

type WatchRequest struct {
	ID string `json:"id"`
}

type StatusRequest struct{}

type StatusResponse struct {
	CanceledWatches int64 `json:"canceled_watches"`
}

var CanceledJobs atomic.Int64
var CanceledWatches atomic.Int64

func Add(ctx context.Context, req *AddRequest) (AddResponse, error) {
	return AddResponse{Sum: req.A + req.B}, nil
}

func Delay(ctx context.Context, req *DelayRequest) (DelayResponse, error) {
	timer := time.NewTimer(req.Delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return DelayResponse{Value: req.Value}, nil
	case <-ctx.Done():
		return DelayResponse{}, ctx.Err()
	}
}

func Job(ctx context.Context, req *JobRequest) (JobResponse, error) {
	<-ctx.Done()
	CanceledJobs.Add(1)
	return JobResponse{}, fmt.Errorf("job %s: %w", req.ID, ctx.Err())
}

func Numbers(ctx context.Context, req *NumbersRequest, out *rpc.TypedServerStream[NumbersItem]) error {
	for i := 0; i < req.Count; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if _, err := out.Send(ctx, NumbersItem{Index: i, Value: fmt.Sprintf("number-%d", i)}); err != nil {
			return err
		}
	}
	return nil
}

func Watch(ctx context.Context, req *WatchRequest, out *rpc.TypedServerStream[NumbersItem]) error {
	if _, err := out.Send(ctx, NumbersItem{Index: 0, Value: "watching:" + req.ID}); err != nil {
		return err
	}
	<-ctx.Done()
	CanceledWatches.Add(1)
	return ctx.Err()
}

func Status(context.Context, *StatusRequest) (StatusResponse, error) {
	return StatusResponse{CanceledWatches: CanceledWatches.Load()}, nil
}
