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

type TypedBidiHandler[Arg any] func(context.Context, *Arg, *ServerBidiStream) error

func HandleBidi[Arg any](s *Server, method string, fn TypedBidiHandler[Arg]) {
	codec := s.cfg.Codec
	s.HandleBidi(method, func(ctx context.Context, payload []byte, headers map[string]string, stream *ServerBidiStream) error {
		var arg Arg
		if err := codec.Decode(payload, &arg); err != nil {
			return err
		}
		return fn(ctx, &arg, stream)
	})
}

func CallBidi[Arg any](ctx context.Context, c *Client, method string, arg Arg, headers map[string]string) (*BidiStream, error) {
	return c.BidiStream(ctx, method, arg, headers)
}

func BidiSend[Item any](ctx context.Context, codec PayloadCodec, send func(context.Context, []byte, map[string]string) error, item Item) error {
	payload, err := codec.Encode(item)
	if err != nil {
		return err
	}
	return send(ctx, payload, nil)
}

func BidiRecv[Item any](ctx context.Context, codec PayloadCodec, recv func(context.Context) ([]byte, map[string]string, error)) (Item, error) {
	var item Item
	payload, _, err := recv(ctx)
	if err != nil {
		return item, err
	}
	if err := codec.Decode(payload, &item); err != nil {
		return item, err
	}
	return item, nil
}
