package rpc

import "errors"

var (
	ErrConnectionClosed = errors.New("lrpc: connection closed")
	ErrTimeout          = errors.New("lrpc: call timed out")
	ErrCanceled         = errors.New("lrpc: call canceled")
	ErrBusy             = errors.New("lrpc: too many pending calls")
	ErrMessageTooLarge  = errors.New("lrpc: message too large")
	ErrBadFrame         = errors.New("lrpc: bad frame")
	ErrUnsupported      = errors.New("lrpc: unsupported operation")
	ErrStreamClosed     = errors.New("lrpc: stream direction closed")
)

type CallError struct {
	Message string
}

func (e *CallError) Error() string {
	return e.Message
}
