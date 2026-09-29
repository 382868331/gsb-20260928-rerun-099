package rpc

import (
	"encoding/binary"
	"io"
)

const defaultMaxFrameSize = 4 << 20

func WriteFrame(w io.Writer, maxSize int, frame []byte) error {
	if len(frame) > maxSize {
		return ErrMessageTooLarge
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(frame)))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	for len(frame) > 0 {
		n, err := w.Write(frame)
		if err != nil {
			return err
		}
		frame = frame[n:]
	}
	return nil
}

func ReadFrame(r io.Reader, maxSize int, buf []byte) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	size := int(binary.BigEndian.Uint32(header[:]))
	if size <= 0 || size > maxSize {
		return nil, ErrBadFrame
	}
	if cap(buf) < size {
		buf = make([]byte, size)
	} else {
		buf = buf[:size]
	}
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
