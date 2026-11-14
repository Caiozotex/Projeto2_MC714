package twopc

import (
	"encoding/json"
	"fmt"
	"sync"

	"distributed-tx/internal/storage"
)

// TxStatus rastreia o estado de uma transação vista por este nó,
// independentemente do papel (coord ou part).
type TxStatus struct {
	TxID     string
	Role     string // "coord" | "participant"
	State    string // INIT | WAIT | READY | COMMIT | ABORT
	Decision string // COMMIT | ABORT (só se decidiu)
	Ops      []Op
}

// Store é a tabela em memória de estados transacionais, espelhando o WAL.
// Após um crash, é reconstruída em ReplayWAL.
type Store struct {
	mu  sync.RWMutex
	wal *storage.WAL
	txs map[string]*TxStatus // tx_id → status
}

// NewStore cria o store vazio.
func NewStore(wal *storage.WAL) *Store {
	return &Store{wal: wal, txs: make(map[string]*TxStatus)}
}

// Begin registra o início de uma transação (INIT). Idempotente.
func (s *Store) Begin(txID, role string, ops []Op) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.txs[txID]; ok {
		return nil
	}
	st := &TxStatus{TxID: txID, Role: role, State: PartInit, Ops: ops}
	s.txs[txID] = st

	payload, _ := json.Marshal(map[string]any{"role": role, "ops": ops})
	return s.wal.Append(storage.Entry{TxID: txID, State: PartInit, Payload: payload})
}

// SetState atualiza o estado e persiste. Para transições finais
// (COMMIT/ABORT), seta Decision e torna-se idempotente após gravação.
func (s *Store) SetState(txID, state, decision string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.txs[txID]
	if !ok {
		return fmt.Errorf("SetState: tx %q desconhecida", txID)
	}
	// Idempotência: se já estamos em decisão final, não regride.
	if st.State == PartCommit || st.State == PartAbort {
		return nil
	}
	if state == PartCommit || state == PartAbort {
		st.Decision = decision
	}
	st.State = state
	return s.wal.Append(storage.Entry{TxID: txID, State: state})
}

// Get devolve uma cópia do estado (nil se desconhecido).
func (s *Store) Get(txID string) *TxStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if st, ok := s.txs[txID]; ok {
		cp := *st
		return &cp
	}
	return nil
}

// All devolve todos os estados (cópia rasa; Ops compartilhado, imutável).
func (s *Store) All() []*TxStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*TxStatus, 0, len(s.txs))
	for _, st := range s.txs {
		cp := *st
		out = append(out, &cp)
	}
	return out
}

// ReplayWAL reconstrói o mapa a partir das entradas persistidas no boot.
// Entradas de INIT/WAIT/READY sem decisão final são tratadas como
// candidatas à recuperação cooperativa (o caller decide o que fazer).
func (s *Store) ReplayWAL() error {
	entries, err := s.wal.Replay()
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range entries {
		st, ok := s.txs[e.TxID]
		if !ok {
			st = &TxStatus{TxID: e.TxID}
			if len(e.Payload) > 0 {
				var meta struct {
					Role string `json:"role"`
					Ops  []Op   `json:"ops"`
				}
				_ = json.Unmarshal(e.Payload, &meta)
				st.Role = meta.Role
				st.Ops = meta.Ops
			}
			s.txs[e.TxID] = st
		}
		switch e.State {
		case PartInit:
			st.State = PartInit
		case PartReady:
			st.State = PartReady
		case PartCommit:
			st.State = PartCommit
			st.Decision = DecisionCommit
		case PartAbort:
			st.State = PartAbort
			st.Decision = DecisionAbort
		}
	}
	return nil
}
