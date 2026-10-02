package rpc

import (
	"encoding/binary"
	"fmt"
)

const (
	protocolVersion byte = 1
	serializerJSON  byte = 1

	TypeRequest  byte = 1
	TypeResponse byte = 2
	TypeError    byte = 3
	TypeCancel   byte = 4
	TypeStream   byte = 5
	TypeAck      byte = 6
	TypeBidiData byte = 7
	TypeBidiAck  byte = 8
	FlagStream   byte = 1
	FlagBidi     byte = 2
	FlagEnd      byte = 4
)

type Envelope struct {
	Type       byte
	ID         uint64
	Seq        uint64
	Method     string
	Error      string
	Headers    map[string]string
	Payload    []byte
	Serializer byte
	Flags      byte
}

func EncodeEnvelope(e *Envelope, maxSize int) ([]byte, error) {
	size := 1 + 1 + 1 + 1 + 8 + 8 + 2 + len(e.Method) + 2 + len(e.Error) + 2
	for key, value := range e.Headers {
		size += 2 + len(key) + 2 + len(value)
	}
	size += 4 + len(e.Payload)
	if size > maxSize {
		return nil, ErrMessageTooLarge
	}
	b := make([]byte, 0, size)
	b = append(b, protocolVersion)
	b = append(b, e.Type)
	serializer := e.Serializer
	if serializer == 0 {
		serializer = serializerJSON
	}
	b = append(b, serializer)
	b = append(b, e.Flags)
	b = binary.BigEndian.AppendUint64(b, e.ID)
	b = binary.BigEndian.AppendUint64(b, e.Seq)
	b = appendString(b, e.Method)
	b = appendString(b, e.Error)
	b = binary.BigEndian.AppendUint16(b, uint16(len(e.Headers)))
	for key, value := range e.Headers {
		b = appendString(b, key)
		b = appendString(b, value)
	}
	b = binary.BigEndian.AppendUint32(b, uint32(len(e.Payload)))
	b = append(b, e.Payload...)
	return b, nil
}

func DecodeEnvelope(frame []byte) (*Envelope, error) {
	if len(frame) < 24 {
		return nil, ErrBadFrame
	}
	pos := 0
	if frame[pos] != protocolVersion {
		return nil, fmt.Errorf("%w: unknown protocol version", ErrBadFrame)
	}
	pos++
	e := &Envelope{Type: frame[pos]}
	pos++
	e.Serializer = frame[pos]
	pos++
	e.Flags = frame[pos]
	pos++
	switch e.Type {
	case TypeRequest, TypeResponse, TypeError, TypeCancel, TypeStream, TypeAck, TypeBidiData, TypeBidiAck:
	default:
		return nil, fmt.Errorf("%w: unknown frame type", ErrBadFrame)
	}
	if e.Serializer != serializerJSON {
		return nil, fmt.Errorf("%w: unsupported serializer", ErrBadFrame)
	}
	e.ID = binary.BigEndian.Uint64(frame[pos:])
	pos += 8
	e.Seq = binary.BigEndian.Uint64(frame[pos:])
	pos += 8
	method, next, err := readString(frame, pos)
	if err != nil {
		return nil, err
	}
	e.Method = method
	pos = next
	errText, next, err := readString(frame, pos)
	if err != nil {
		return nil, err
	}
	e.Error = errText
	pos = next
	if len(frame[pos:]) < 2 {
		return nil, ErrBadFrame
	}
	hCount := int(binary.BigEndian.Uint16(frame[pos:]))
	pos += 2
	if hCount > 0 {
		e.Headers = make(map[string]string, hCount)
	}
	for i := 0; i < hCount; i++ {
		key, next, err := readString(frame, pos)
		if err != nil {
			return nil, err
		}
		pos = next
		value, next, err := readString(frame, pos)
		if err != nil {
			return nil, err
		}
		pos = next
		e.Headers[key] = value
	}
	if len(frame[pos:]) < 4 {
		return nil, ErrBadFrame
	}
	payloadLen := int(binary.BigEndian.Uint32(frame[pos:]))
	pos += 4
	if len(frame[pos:]) != payloadLen {
		return nil, ErrBadFrame
	}
	e.Payload = append([]byte(nil), frame[pos:]...)
	return e, nil
}

func appendString(b []byte, value string) []byte {
	b = binary.BigEndian.AppendUint16(b, uint16(len(value)))
	return append(b, value...)
}

func readString(b []byte, pos int) (string, int, error) {
	if len(b[pos:]) < 2 {
		return "", 0, ErrBadFrame
	}
	n := int(binary.BigEndian.Uint16(b[pos:]))
	pos += 2
	if len(b[pos:]) < n {
		return "", 0, ErrBadFrame
	}
	return string(b[pos : pos+n]), pos + n, nil
}
