package rpc

import (
	"context"
	"io"
)

type TypedHandler[Arg any, Reply any] func(context.Context, *Arg) (Reply, error)

func Call[Arg any, Reply any](ctx context.Context, c *Client, method string, arg Arg, headers map[string]string) (Reply, map[string]string, error) {
	var reply Reply
	h, err := c.Call(ctx, method, arg, &reply, headers)
	return reply, h, err
}

type TypedStreamHandler[Arg any, Item any] func(context.Context, *Arg, *TypedServerStream[Item]) error

func HandleStream[Arg any, Item any](s *Server, method string, fn TypedStreamHandler[Arg, Item]) {
	codec := s.cfg.Codec
	s.HandleStream(method, func(ctx context.Context, payload []byte, headers map[string]string, raw *ServerStream) error {
		var arg Arg
		if err := codec.Decode(payload, &arg); err != nil {
			return err
		}
		stream := &TypedServerStream[Item]{raw: raw, codec: codec}
		return fn(ctx, &arg, stream)
	})
}

type TypedServerStream[Item any] struct {
	raw   *ServerStream
	codec PayloadCodec
}

func (s *TypedServerStream[Item]) Send(ctx context.Context, value Item) (map[string]string, error) {
	payload, err := s.codec.Encode(value)
	if err != nil {
		return nil, err
	}
	return s.raw.Send(ctx, payload, nil)
}

func CallStream[Arg any, Item any](ctx context.Context, c *Client, method string, arg Arg, headers map[string]string) (*TypedStream[Item], error) {
	raw, err := c.Stream(ctx, method, arg, headers)
	if err != nil {
		return nil, err
	}
	return &TypedStream[Item]{raw: raw, codec: c.cfg.Codec}, nil
}

type TypedStream[Item any] struct {
	raw   *Stream
	codec PayloadCodec
}

func (s *TypedStream[Item]) Recv(ctx context.Context) (Item, map[string]string, error) {
	var value Item
	payload, headers, err := s.raw.Recv(ctx)
	if err != nil {
		return value, headers, err
	}
	if err := s.codec.Decode(payload, &value); err != nil {
		return value, headers, err
	}
	return value, headers, nil
}

func (s *TypedStream[Item]) Close() error { return s.raw.Close() }

func IsEnd(err error) bool { return err == io.EOF }
