package localrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type EchoRequest struct {
	Text string `json:"text"`
}

type EchoResponse struct {
	Text string `json:"text"`
}

type DelayRequest struct {
	Text        string `json:"text"`
	DelayMillis int    `json:"delay_millis"`
}

func JSONEcho(_ context.Context, payload []byte) ([]byte, error) {
	var request EchoRequest
	if err := json.Unmarshal(payload, &request); err != nil {
		return nil, err
	}
	return json.Marshal(EchoResponse{Text: request.Text})
}

func JSONDelay(ctx context.Context, payload []byte) ([]byte, error) {
	var request DelayRequest
	if err := json.Unmarshal(payload, &request); err != nil {
		return nil, err
	}
	timer := time.NewTimer(time.Duration(request.DelayMillis) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return json.Marshal(EchoResponse{Text: fmt.Sprintf("delayed:%s", request.Text)})
	}
}
