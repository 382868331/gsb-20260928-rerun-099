package rpc

import "encoding/json"

type PayloadCodec interface {
	ID() byte
	Encode(value any) ([]byte, error)
	Decode(data []byte, value any) error
}

type JSONCodec struct{}

func (JSONCodec) ID() byte { return serializerJSON }

func (JSONCodec) Encode(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	return json.Marshal(value)
}

func (JSONCodec) Decode(data []byte, value any) error {
	if len(data) == 0 {
		return nil
	}
	return json.Unmarshal(data, value)
}
