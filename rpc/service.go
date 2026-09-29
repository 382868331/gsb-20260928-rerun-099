package rpc

import "context"

type TypedHandler[Arg any, Reply any] func(context.Context, *Arg) (Reply, error)

func Call[Arg any, Reply any](ctx context.Context, c *Client, method string, arg Arg, headers map[string]string) (Reply, map[string]string, error) {
	var reply Reply
	h, err := c.Call(ctx, method, arg, &reply, headers)
	return reply, h, err
}
