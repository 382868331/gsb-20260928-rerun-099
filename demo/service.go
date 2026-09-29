package demo

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
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

var CanceledJobs atomic.Int64

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
