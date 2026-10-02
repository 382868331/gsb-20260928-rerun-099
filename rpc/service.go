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

type TypedBidiHandler[Arg any, Item any] func(context.Context, *Arg, *TypedServerBidi[Item]) error

func HandleBidi[Arg any, Item any](s *Server, method string, fn TypedBidiHandler[Arg, Item]) {
	codec := s.cfg.Codec
	s.HandleBidi(method, func(ctx context.Context, payload []byte, headers map[string]string, raw *ServerBidiStream) error {
		var arg Arg
		if err := codec.Decode(payload, &arg); err != nil {
			return err
		}
		return fn(ctx, &arg, &TypedServerBidi[Item]{raw: raw, codec: codec})
	})
}

type TypedServerBidi[Item any] struct {
	raw   *ServerBidiStream
	codec PayloadCodec
}

func (s *TypedServerBidi[Item]) Send(ctx context.Context, value Item) error {
	payload, err := s.codec.Encode(value)
	if err != nil {
		return err
	}
	return s.raw.Send(ctx, payload, nil)
}

func (s *TypedServerBidi[Item]) Recv(ctx context.Context) (Item, error) {
	var value Item
	payload, err := s.raw.Recv(ctx)
	if err != nil {
		return value, err
	}
	if err := s.codec.Decode(payload, &value); err != nil {
		return value, err
	}
	return value, nil
}

func (s *TypedServerBidi[Item]) CloseSend(ctx context.Context) error { return s.raw.CloseSend(ctx) }

func (s *TypedServerBidi[Item]) Outcome() StreamOutcome { return s.raw.Outcome() }

func CallBidi[Arg any, Item any](ctx context.Context, c *Client, method string, arg Arg, headers map[string]string) (*TypedBidi[Item], error) {
	raw, err := c.Bidi(ctx, method, arg, headers)
	if err != nil {
		return nil, err
	}
	return &TypedBidi[Item]{raw: raw, codec: c.cfg.Codec}, nil
}

type TypedBidi[Item any] struct {
	raw   *BidiStream
	codec PayloadCodec
}

func (s *TypedBidi[Item]) Send(ctx context.Context, value Item) error {
	payload, err := s.codec.Encode(value)
	if err != nil {
		return err
	}
	return s.raw.Send(ctx, payload)
}

func (s *TypedBidi[Item]) Recv(ctx context.Context) (Item, error) {
	var value Item
	payload, err := s.raw.Recv(ctx)
	if err != nil {
		return value, err
	}
	if err := s.codec.Decode(payload, &value); err != nil {
		return value, err
	}
	return value, nil
}

func (s *TypedBidi[Item]) CloseSend() error { return s.raw.CloseSend() }

func (s *TypedBidi[Item]) Cancel() { s.raw.Cancel() }

func (s *TypedBidi[Item]) Outcome() StreamOutcome { return s.raw.Outcome() }
