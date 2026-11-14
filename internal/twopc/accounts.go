package twopc

import (
	"fmt"
	"sync"
)

// Shard é o estado de negócio local: contas cujo hash cai neste nó.
// O 2PC aplica as mudanças em `staged` durante a Fase 1 e faz o swap
// para `committed` na Fase 2 se a decisão for COMMIT.
type Shard struct {
	mu        sync.RWMutex
	committed map[string]int64
	staged    map[string]int64 // cópia isolada durante a Fase 1
}

// NewShard inicializa com um saldo inicial fixo (simula base de dados).
func NewShard(initialBalance int64, accounts []string) *Shard {
	c := make(map[string]int64, len(accounts))
	for _, a := range accounts {
		c[a] = initialBalance
	}
	return &Shard{committed: c, staged: nil}
}

// Balance consulta o saldo commited.
func (s *Shard) Balance(account string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.committed[account]
}

// Stage prepara as alterações em `staged`. A regra "não pode ficar negativo"
// é verificada AQUI — se falhar, o participante vota ABORT.
func (s *Shard) Stage(ops []Op) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	draft := make(map[string]int64, len(s.committed))
	for k, v := range s.committed {
		draft[k] = v
	}
	for _, op := range ops {
		// Aplica somente se a conta pertence a este shard.
		cur, ok := draft[op.Account]
		if !ok {
			continue
		}
		newV := cur + op.Delta
		if newV < 0 {
			return fmt.Errorf("saldo insuficiente em %s (atual=%d delta=%d)",
				op.Account, cur, op.Delta)
		}
		draft[op.Account] = newV
	}
	s.staged = draft
	return nil
}

// Commit aplica o `staged` como estado oficial (idempotente).
func (s *Shard) Commit() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.staged != nil {
		s.committed = s.staged
		s.staged = nil
	}
}

// Abort descarta o `staged` (idempotente).
func (s *Shard) Abort() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.staged = nil
}
