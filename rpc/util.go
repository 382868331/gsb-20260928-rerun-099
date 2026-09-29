package rpc

import (
	"crypto/rand"
	"encoding/binary"
	"strconv"
	"time"
)

func fastrandID() uint64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return uint64(time.Now().UnixNano())
	}
	return binary.BigEndian.Uint64(b[:])
}

func formatDeadline(t time.Time) string {
	return strconv.FormatInt(t.UnixNano(), 10)
}

func mergeHeaders(groups ...map[string]string) map[string]string {
	var result map[string]string
	for _, group := range groups {
		for key, value := range group {
			if result == nil {
				result = make(map[string]string)
			}
			result[key] = value
		}
	}
	return result
}
