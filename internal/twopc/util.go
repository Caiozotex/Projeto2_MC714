package twopc

import (
	"crypto/rand"
	"encoding/hex"
)

func newReqID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
