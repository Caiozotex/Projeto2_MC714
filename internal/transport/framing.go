package transport

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

const (
	// maxFrameSize limita o tamanho de um frame individual (16 MiB).
	maxFrameSize = 16 << 20
	// protocolVersion versão do envelope (incrementar em mudanças incompatíveis).
	protocolVersion = 1
)

// Envelope é o contrato de fio comum a TODOS os RPCs internos do sistema.
type Envelope struct {
	V        int             `json:"v"`
	Src      int             `json:"src"`
	Dst      int             `json:"dst"`
	Kind     string          `json:"kind"`
	ReqID    string          `json:"req_id"`
	TxID     string          `json:"tx_id,omitempty"`
	SentAtMs int64           `json:"sent_at_ms"`
	Body     json.RawMessage `json:"body,omitempty"`
}

// NewEnvelope constrói um envelope com versão e timestamp preenchidos.
func NewEnvelope(src, dst int, kind, reqID, txID string, body any) (*Envelope, error) {
	raw, err := json.Marshal(body)
	if err != nil { return nil, fmt.Errorf("marshal body: %w", err) }
	return &Envelope{
		V:        protocolVersion,
		Src:      src,
		Dst:      dst,
		Kind:     kind,
		ReqID:    reqID,
		TxID:     txID,
		SentAtMs: nowMillis(),
		Body:     raw,
	}, nil
}

// WriteFrame serializa `data` como [uint32 tam BE][payload].
func WriteFrame(w io.Writer, data []byte) error {
	if len(data) > maxFrameSize {
		return fmt.Errorf("frame acima do limite: %d > %d", len(data), maxFrameSize)
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(data)))
	if _, err := w.Write(hdr[:]); err != nil { return err }
	_, err := w.Write(data)
	return err
}

// ReadFrame lê um frame [uint32 tam BE][payload].
func ReadFrame(r io.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil { return nil, err }
	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 || n > maxFrameSize {
		return nil, fmt.Errorf("frame com tamanho inválido: %d", n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil { return nil, err }
	return buf, nil
}
