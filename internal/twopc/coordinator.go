package twopc

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"distributed-tx/internal/config"
	"distributed-tx/internal/metrics"
	"distributed-tx/internal/transport"
)

// Coordinator executa a FSM INIT → WAIT → COMMIT|ABORT para cada transação
// que este nó inicia. Só o líder Bully corrente deve chamar Execute.
type Coordinator struct {
	cfg      *config.Config
	client   *transport.Client
	store    *Store
	registry *metrics.Registry

	mu  sync.Mutex
	rtt []time.Duration // para log/diagnóstico
}

// NewCoordinator constrói o coordenador.
func NewCoordinator(cfg *config.Config, client *transport.Client, store *Store, reg *metrics.Registry) *Coordinator {
	return &Coordinator{cfg: cfg, client: client, store: store, registry: reg}
}

// Execute roda o 2PC para uma transação. Devolve a decisão final
// ("COMMIT" | "ABORT") ou erro fatal de contexto.
//
// Pré-condição: `parts` contém os IDs dos participantes que possuem ao
// menos uma conta envolvida em `ops`. `txID` deve ser globalmente único
// (gerado pelo cliente; sugerimos UUID).
func (c *Coordinator) Execute(ctx context.Context, txID string, ops []Op, parts []int) (string, error) {
	if err := c.store.Begin(txID, "coord", ops); err != nil {
		return "", fmt.Errorf("Begin: %w", err)
	}
	c.registry.Inc("2pc.coord.started_total")

	if err := c.store.SetState(txID, CoordWait, ""); err != nil {
		return "", err
	}

	// === Fase 1: VOTE_REQUEST ===
	voteCtx, cancel := context.WithTimeout(ctx,
		time.Duration(c.cfg.TxTimeoutMs)*time.Millisecond)
	defer cancel()

	type voteResult struct {
		peer  int
		vote  string
		error error
	}
	results := make(chan voteResult, len(parts))

	for _, pid := range parts {
		go func(pid int) {
			body := VoteRequest{Ops: ops, Term: 0}
			env, _ := transport.NewEnvelope(
				c.cfg.NodeID, pid, "2pc.vote_req", newReqID(), txID, body)
			resp, err := c.client.Call(voteCtx, pid, env)
			if err != nil {
				results <- voteResult{peer: pid, error: err}
				return
			}
			var vr VoteResponse
			if err := json.Unmarshal(resp.Body, &vr); err != nil {
				results <- voteResult{peer: pid, error: err}
				return
			}
			results <- voteResult{peer: pid, vote: vr.Vote}
		}(pid)
	}

	unanimousCommit := true
	var abortReason string
	for i := 0; i < len(parts); i++ {
		select {
		case r := <-results:
			if r.error != nil {
				unanimousCommit = false
				abortReason = fmt.Sprintf("peer %d: %v", r.peer, r.error)
			} else if r.vote != "COMMIT" {
				unanimousCommit = false
				abortReason = fmt.Sprintf("peer %d votou %s", r.peer, r.vote)
			}
		case <-voteCtx.Done():
			unanimousCommit = false
			abortReason = "timeout na Fase 1"
			// Consome o restante para não vazar goroutines.
			for j := i; j < len(parts); j++ {
				<-results
			}
			goto decide
		}
	}

decide:
	decision := DecisionCommit
	if !unanimousCommit {
		decision = DecisionAbort
	}
	if decision == DecisionAbort {
		slog.Warn("2pc: abortando",
			"node_id", c.cfg.NodeID, "tx_id", txID, "reason", abortReason)
	}

	// === Persiste decisão ANTES de disseminar (WAL obrigatório) ===
	if decision == DecisionCommit {
		if err := c.store.SetState(txID, CoordCommit, DecisionCommit); err != nil {
			return "", err
		}
	} else {
		if err := c.store.SetState(txID, CoordAbort, DecisionAbort); err != nil {
			return "", err
		}
	}

	// === Fase 2: dissemina decisão ===
	globalCtx, cancel2 := context.WithTimeout(ctx,
		time.Duration(c.cfg.TxTimeoutMs)*time.Millisecond)
	defer cancel2()

	var wg sync.WaitGroup
	for _, pid := range parts {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			body := GlobalDecision{Decision: decision, Term: 0}
			env, _ := transport.NewEnvelope(
				c.cfg.NodeID, pid, "2pc.global", newReqID(), txID, body)
			_, _ = c.client.Call(globalCtx, pid, env)
		}(pid)
	}
	wg.Wait()

	if decision == DecisionCommit {
		c.registry.Inc("2pc.coord.committed_total")
	} else {
		c.registry.Inc("2pc.coord.aborted_total")
	}
	return decision, nil
}

// ReplayFromWAL reenvia decisões finais que possam não ter chegado aos
// participantes antes de um crash do coordenador. Chamado no boot.
func (c *Coordinator) ReplayFromWAL(ctx context.Context) {
	for _, st := range c.store.All() {
		if st.Role != "coord" {
			continue
		}
		if st.State != CoordCommit && st.State != CoordAbort {
			// Coordenador caiu ANTES de decidir: aborta com segurança.
			_ = c.store.SetState(st.TxID, CoordAbort, DecisionAbort)
			st.Decision = DecisionAbort
			st.State = CoordAbort
		}
		// Reenvia GLOBAL_* (idempotente no participante).
		decision := st.Decision
		for _, pid := range c.cfg.PeerIDs() {
			body := GlobalDecision{Decision: decision, Term: 0}
			env, _ := transport.NewEnvelope(
				c.cfg.NodeID, pid, "2pc.global", newReqID(), st.TxID, body)
			cctx, cancel := context.WithTimeout(ctx,
				time.Duration(c.cfg.TxTimeoutMs)*time.Millisecond)
			_, _ = c.client.Call(cctx, pid, env)
			cancel()
		}
	}
}
