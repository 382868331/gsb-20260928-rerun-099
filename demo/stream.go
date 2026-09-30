package demo

import (
	"context"
	"strings"
)

// Count emits consecutive numbered fragments. The framework flow-controls
// emit: a slow client blocks it naturally, and cancel/disconnect makes emit
// return an error so the server side stops producing.
func Count(ctx context.Context, req *CountRequest, emit func(CountItem) error) error {
	tag := strings.Repeat("p", req.Size)
	for i := 0; i < req.Count; i++ {
		if err := emit(CountItem{Seq: i, Tag: tag}); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return nil
}
