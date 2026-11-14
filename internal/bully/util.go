package bully

import (
	"crypto/rand"
	"encoding/hex"
)

func newReqID(_ int) string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
