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

// Participant executa a FSM INIT → READY → COMMIT|ABORT para cada
// transação em que este nó é participante. Também expõe o protocolo
// cooperativo de recuperação (2pc.status_q).
type Participant struct {
	cfg      *config.Config
	client   *transport.Client
	store    *Store
	shard    *Shard
	registry *metrics.Registry

	// recoveryOnce garante que só uma goroutine de recuperação por tx
	// roda por vez.
	recoveryMu   sync.Mutex
	recoveryInFlight map[string]bool
}

// NewParticipant constrói o participante.
func NewParticipant(cfg *config.Config, client *transport.Client, store *Store, shard *Shard, reg *metrics.Registry) *Participant {
	return &Participant{
		cfg:              cfg,
		client:           client,
		store:            store,
		shard:            shard,
		registry:         reg,
		recoveryInFlight: make(map[string]bool),
	}
}

// HandleVoteRequest (Fase 1a → 1b): valida, stageia e responde.
func (p *Participant) HandleVoteRequest(ctx context.Context, txID string, req VoteRequest) VoteResponse {
	// Idempotência: se já votamos COMMIT antes, reenvia o mesmo voto.
	if st := p.store.Get(txID); st != nil {
		if st.State == PartReady {
			return VoteResponse{Vote: "COMMIT"}
		}
		if st.State == PartCommit {
			return VoteResponse{Vote: "COMMIT"} // já commitado; coordinator reenvia global
		}
		if st.State == PartAbort {
			return VoteResponse{Vote: "ABORT", Reason: "tx anteriormente abortada"}
		}
	}

	if err := p.store.Begin(txID, "participant", req.Ops); err != nil {
		return VoteResponse{Vote: "ABORT", Reason: err.Error()}
	}

	// Verifica pré-condição de negócio (saldos).
	if err := p.shard.Stage(req.Ops); err != nil {
		_ = p.store.SetState(txID, PartAbort, DecisionAbort)
		p.registry.Inc("2pc.part.abort_local_total")
		return VoteResponse{Vote: "ABORT", Reason: err.Error()}
	}

	// WAL ANTES de responder COMMIT (crash-recovery seguro).
	if err := p.store.SetState(txID, PartReady, ""); err != nil {
		return VoteResponse{Vote: "ABORT", Reason: "wal: " + err.Error()}
	}
	p.registry.Inc("2pc.part.ready_total")
	return VoteResponse{Vote: "COMMIT"}
}

// HandleGlobalDecision (Fase 2a → 2b): aplica decisão idempotentemente.
func (p *Participant) HandleGlobalDecision(ctx context.Context, txID string, dec GlobalDecision) Ack {
	st := p.store.Get(txID)
	if st == nil {
		// Não conhecemos esta tx (participante "esquecido"). Idempotente
		// mas registramos para convergência.
		_ = p.store.Begin(txID, "participant", nil)
	}

	if dec.Decision == DecisionCommit {
		if err := p.store.SetState(txID, PartCommit, DecisionCommit); err == nil {
			p.shard.Commit()
			p.registry.Inc("2pc.part.committed_total")
		}
	} else {
		if err := p.store.SetState(txID, PartAbort, DecisionAbort); err == nil {
			p.shard.Abort()
			p.registry.Inc("2pc.part.aborted_total")
		}
	}
	return Ack{Applied: true}
}

// HandleStatusQuery responde a consultas do protocolo cooperativo.
func (p *Participant) HandleStatusQuery(txID string) StatusReply {
	st := p.store.Get(txID)
	if st == nil {
		return StatusReply{State: "UNKNOWN"}
	}
	return StatusReply{State: st.State}
}

// RecoverReadyPendentes inicia o protocolo cooperativo para transações
// presas em READY (após timeout ou no boot pós-crash).
func (p *Participant) RecoverReadyPendentes(ctx context.Context) {
	for _, st := range p.store.All() {
		if st.State == PartReady && st.Role == "participant" {
			go p.recoverOne(ctx, st.TxID)
		}
	}
}

func (p *Participant) recoverOne(ctx context.Context, txID string) {
	p.recoveryMu.Lock()
	if p.recoveryInFlight[txID] {
		p.recoveryMu.Unlock()
		return
	}
	p.recoveryInFlight[txID] = true
	p.recoveryMu.Unlock()
	defer func() {
		p.recoveryMu.Lock()
		delete(p.recoveryInFlight, txID)
		p.recoveryMu.Unlock()
	}()

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(p.cfg.TxTimeoutMs) * time.Millisecond):
		}

		decided := false
		for _, pid := range p.cfg.PeerIDs() {
			body := StatusQuery{}
			env, _ := transport.NewEnvelope(
				p.cfg.NodeID, pid, "2pc.status_q", newReqID(), txID, body)
			cctx, cancel := context.WithTimeout(ctx,
				time.Duration(p.cfg.TxTimeoutMs)*time.Millisecond)
			resp, err := p.client.Call(cctx, pid, env)
			cancel()
			if err != nil {
				continue
			}
			var sr StatusReply
			if err := json.Unmarshal(resp.Body, &sr); err != nil {
				continue
			}
			switch sr.State {
			case PartCommit:
				p.HandleGlobalDecision(ctx, txID, GlobalDecision{Decision: DecisionCommit})
				decided = true
			case PartAbort:
				p.HandleGlobalDecision(ctx, txID, GlobalDecision{Decision: DecisionAbort})
				decided = true
			case PartInit:
				// Nenhum par decidiu ainda: continua aguardando coordenador.
			}
			if decided {
				return
			}
		}

		if !decided {
			// Nenhum par decidiu. Mantemos READY (bloqueio seguro do 2PC):
			// decidir ABORT aqui violaria Agreement se o coordenador original
			// gravou COMMIT no WAL antes de cair.
			slog.Info("2pc: recuperação aguardando decisão",
				"node_id", p.cfg.NodeID, "tx_id", txID)
		}
	}
	slog.Warn("2pc: recovery timeout",
		"node_id", p.cfg.NodeID, "tx_id", txID,
		"state", PartReady, "msg", "mantendo bloqueio seguro (2PC bloqueante)")
}

// HandleEnvelope é o despachante usado pelo servidor TCP.
func (p *Participant) HandleEnvelope(ctx context.Context, env *transport.Envelope) (*transport.Envelope, error) {
	switch env.Kind {
	case "2pc.vote_req":
		var req VoteRequest
		if err := json.Unmarshal(env.Body, &req); err != nil {
			return nil, err
		}
		vr := p.HandleVoteRequest(ctx, env.TxID, req)
		return reply(env.Src, "2pc.vote_resp", env.ReqID, env.TxID, vr)
	case "2pc.global":
		var dec GlobalDecision
		if err := json.Unmarshal(env.Body, &dec); err != nil {
			return nil, err
		}
		ack := p.HandleGlobalDecision(ctx, env.TxID, dec)
		return reply(env.Src, "2pc.ack", env.ReqID, env.TxID, ack)
	case "2pc.status_q":
		sr := p.HandleStatusQuery(env.TxID)
		return reply(env.Src, "2pc.status_r", env.ReqID, env.TxID, sr)
	}
	return nil, fmt.Errorf("participant: kind desconhecido %q", env.Kind)
}

func reply(dst int, kind, reqID, txID string, body any) (*transport.Envelope, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return &transport.Envelope{
		V: 1, Src: dst, Dst: dst, Kind: kind, ReqID: reqID, TxID: txID,
		Body: raw,
	}, nil
}
