package localrpc

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const (
	DefaultMaxMessageSize = 1 << 20
	DefaultMaxPending     = 128
	DefaultMaxQueue       = 128
)

const (
	frameMagic0  byte = 'R'
	frameMagic1  byte = 'C'
	frameVersion byte = 1
)

const (
	FrameRequest byte = iota + 1
	FrameResponse
	FrameCancel
)

const (
	CodeInternal = iota + 1
	CodeBadRequest
	CodeUnknownMethod
	CodeCanceled
	CodeDeadlineExceeded
	CodeUnavailable
	CodeFrameTooLarge
	CodeQueueFull
)

var (
	ErrFrameTooLarge   = errors.New("localrpc: frame too large")
	ErrBadFrame        = errors.New("localrpc: bad frame")
	ErrConnClosed      = errors.New("localrpc: connection closed")
	ErrQueueFull       = errors.New("localrpc: wait queue is full")
	ErrTooManyPending  = errors.New("localrpc: too many pending calls")
	ErrMessageTooLarge = errors.New("localrpc: message too large")
)

type Message struct {
	ID      uint64            `json:"id"`
	Service string            `json:"service,omitempty"`
	Method  string            `json:"method,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Payload []byte            `json:"payload,omitempty"`
	Error   *RemoteError      `json:"error,omitempty"`
}

type RemoteError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *RemoteError) Error() string {
	return fmt.Sprintf("localrpc: remote error %d: %s", e.Code, e.Message)
}

type Codec interface {
	Marshal(Message) ([]byte, error)
	Unmarshal([]byte, *Message) error
}

type JSONCodec struct{}

func (JSONCodec) Marshal(msg Message) ([]byte, error) {
	return json.Marshal(msg)
}

func (JSONCodec) Unmarshal(data []byte, msg *Message) error {
	return json.Unmarshal(data, msg)
}

func WriteFrame(w io.Writer, msg Message, frameType byte, codec Codec, maxSize int) error {
	if codec == nil {
		codec = JSONCodec{}
	}
	if maxSize <= 0 {
		maxSize = DefaultMaxMessageSize
	}
	data, err := codec.Marshal(msg)
	if err != nil {
		return err
	}
	if len(data) > maxSize {
		return fmt.Errorf("%w: %d > %d", ErrMessageTooLarge, len(data), maxSize)
	}
	header := make([]byte, 16)
	header[0] = frameMagic0
	header[1] = frameMagic1
	header[2] = frameVersion
	header[3] = frameType
	binary.BigEndian.PutUint32(header[4:8], uint32(len(data)))
	binary.BigEndian.PutUint64(header[8:16], msg.ID)
	if _, err := w.Write(header); err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	_, err = w.Write(data)
	return err
}

func ReadFrame(r io.Reader, codec Codec, maxSize int) (Message, byte, error) {
	if codec == nil {
		codec = JSONCodec{}
	}
	if maxSize <= 0 {
		maxSize = DefaultMaxMessageSize
	}
	header := make([]byte, 16)
	if _, err := io.ReadFull(r, header); err != nil {
		return Message{}, 0, err
	}
	if header[0] != frameMagic0 || header[1] != frameMagic1 || header[2] != frameVersion {
		return Message{}, 0, ErrBadFrame
	}
	frameType := header[3]
	size := int(binary.BigEndian.Uint32(header[4:8]))
	if size > maxSize {
		return Message{}, frameType, fmt.Errorf("%w: %d > %d", ErrFrameTooLarge, size, maxSize)
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(r, data); err != nil {
		return Message{}, frameType, err
	}
	var msg Message
	if err := codec.Unmarshal(data, &msg); err != nil {
		return Message{}, frameType, fmt.Errorf("%w: %v", ErrBadFrame, err)
	}
	if msg.ID != binary.BigEndian.Uint64(header[8:16]) {
		return Message{}, frameType, fmt.Errorf("%w: frame id does not match message id", ErrBadFrame)
	}
	return msg, frameType, nil
}

func remoteError(err error) *RemoteError {
	switch {
	case errors.Is(err, context.Canceled):
		return &RemoteError{Code: CodeCanceled, Message: err.Error()}
	case errors.Is(err, context.DeadlineExceeded):
		return &RemoteError{Code: CodeDeadlineExceeded, Message: err.Error()}
	case errors.Is(err, ErrQueueFull):
		return &RemoteError{Code: CodeQueueFull, Message: err.Error()}
	default:
		return &RemoteError{Code: CodeInternal, Message: err.Error()}
	}
}
