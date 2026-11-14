package heartbeat

import (
	"crypto/rand"
	"encoding/hex"
)

// newReqID gera um identificador único e opaco por requisição.
// Usa 12 bytes aleatórios → 96 bits de entropia, suficiente para LRU de
// deduplicação e correlação de logs.
func newReqID(nodeID int) string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
