package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Entry é uma linha do WAL — append-only, um JSON por linha.
type Entry struct {
	TxID    string          `json:"tx_id"`
	State   string          `json:"state"`           // INIT | READY | COMMIT | ABORT | DECISION_COMMIT | DECISION_ABORT
	Ts      int64           `json:"ts_ms"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// WAL é um log persistente append-only, fsync em cada Append.
type WAL struct {
	path string
	mu   sync.Mutex
	f    *os.File
}

// Open abre (ou cria) o WAL em <dir>/wal.log garantindo diretório existente.
func Open(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("criar WAL_DIR %s: %w", dir, err)
	}
	path := filepath.Join(dir, "wal.log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil { return nil, fmt.Errorf("abrir WAL %s: %w", path, err) }
	return &WAL{path: path, f: f}, nil
}

// Append grava e faz fsync. Retorna apenas após durabilidade garantida.
func (w *WAL) Append(e Entry) error {
	if e.Ts == 0 { e.Ts = time.Now().UnixNano() / int64(time.Millisecond) }
	data, err := json.Marshal(e)
	if err != nil { return err }
	data = append(data, '\n')

	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.f.Write(data); err != nil { return err }
	return w.f.Sync()
}

// Replay lê todo o WAL e devolve as entradas em ordem de escrita.
// Chamado no boot para reconstruir FSMs 2PC.
func (w *WAL) Replay() ([]Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.f.Seek(0, 0); err != nil { return nil, err }
	defer w.f.Seek(0, 2) // volta ao final

	var out []Entry
	sc := bufio.NewScanner(w.f)
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 { continue }
		var e Entry
		if err := json.Unmarshal(line, &e); err != nil {
			// Linha truncada (crash no meio do write): parar replay.
			break
		}
		out = append(out, e)
	}
	return out, sc.Err()
}

// Close fecha o arquivo do WAL.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil { return nil }
	err := w.f.Close()
	w.f = nil
	return err
}
