package rpc

import (
	"context"
)

type TypedHandler[Arg any, Reply any] func(context.Context, *Arg) (Reply, error)

func Call[Arg any, Reply any](ctx context.Context, c *Client, method string, arg Arg, headers map[string]string) (Reply, map[string]string, error) {
	var reply Reply
	h, err := c.Call(ctx, method, arg, &reply, headers)
	return reply, h, err
}

// TypedStreamHandler is a typed server-streaming handler. Each emitted reply
// becomes one consecutive fragment delivered to the client in order.
type TypedStreamHandler[Arg any, Item any] func(ctx context.Context, arg *Arg, emit func(Item) error) error

func HandleStreamTyped[Arg any, Item any](s *Server, method string, fn TypedStreamHandler[Arg, Item]) {
	codec := s.cfg.Codec
	s.HandleStream(method, func(ctx context.Context, payload []byte, headers map[string]string, out StreamSender) error {
		var arg Arg
		if err := codec.Decode(payload, &arg); err != nil {
			return err
		}
		emit := func(item Item) error {
			data, err := codec.Encode(item)
			if err != nil {
				return err
			}
			return out.Send(data, nil)
		}
		return fn(ctx, &arg, emit)
	})
}

// OpenStreamTyped starts a typed streaming call.
func OpenStreamTyped[Arg any](ctx context.Context, c *Client, method string, arg Arg, headers map[string]string) (*TypedStream, error) {
	st, err := c.OpenStream(ctx, method, arg, headers)
	if err != nil {
		return nil, err
	}
	return &TypedStream{Stream: st}, nil
}

// TypedStream decodes consecutive fragments for a streaming call.
type TypedStream struct {
	*Stream
}

// Recv returns the next decoded fragment; io.EOF marks clean completion.
func (t *TypedStream) Recv(ctx context.Context, value any) error {
	item, err := t.Stream.Recv(ctx)
	if err != nil {
		return err
	}
	return t.Stream.codec.Decode(item.Payload, value)
}
