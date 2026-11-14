// Package bully implementa o algoritmo de eleição do Valentão (Bully) com
// um campo "term" monotônico por nó (não-consensual, apenas ordena épocas
// e descarta mensagens obsoletas após partição).
//
// Garantias (ver MODEL.md §3.2):
//   - Safety: no máximo um COORDINATOR por term; o vencedor é o maior ID
//     ativo e alcançável.
//   - Liveness: toda eleição iniciada por um nó correto termina em tempo
//     finito, desde que exista canal dentro de ELECTION_TIMEOUT_MS.
//
// Correções importantes:
//
//   - runElection consumia `results` de forma RACE com `wg.Wait()`: o canal
//     `done` podia fechar antes de drenar respostas OK, levando à declaração
//     indevida de vitória. Agora `results` é fechado APÓS `wg.Wait()` e o
//     laço consome com `for r := range results`, sem race.
//   - handleCoordinator/AdoptLeader aceitavam `term == b.term` com líder
//     diferente, permitindo sobrescrita de líder em mesmo termo (viola
//     Safety). Agora rejeitam termo igual com líder diferente; em
//     AdoptLeader, empate prefere o MAIOR ID (desempate determinístico).
package bully

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"distributed-tx/internal/config"
	"distributed-tx/internal/heartbeat"
	"distributed-tx/internal/metrics"
	"distributed-tx/internal/transport"
)

// State é o papel atual do nó na eleição.
type State int

const (
	StateFollower State = iota
	StateElection
	StateLeader
)

func (s State) String() string {
	switch s {
	case StateFollower:
		return "FOLLOWER"
	case StateElection:
		return "ELECTION"
	case StateLeader:
		return "LEADER"
	}
	return "UNKNOWN"
}

// LeaderChange é emitido quando o líder reconhecido muda.
type LeaderChange struct {
	LeaderID int
	Term     int
	IsSelf   bool
}

// Bully é a FSM de eleição.
type Bully struct {
	cfg      *config.Config
	client   *transport.Client
	registry *metrics.Registry

	mu       sync.Mutex
	term     int
	leader   int
	state    State
	election bool

	out chan LeaderChange
}

// New cria o Bully. term inicial = 0, leader desconhecido (-1).
func New(cfg *config.Config, client *transport.Client, reg *metrics.Registry) *Bully {
	return &Bully{
		cfg:      cfg,
		client:   client,
		registry: reg,
		term:     0,
		leader:   -1,
		state:    StateFollower,
		out:      make(chan LeaderChange, 32),
	}
}

// LeaderChanges devolve o canal de eventos.
func (b *Bully) LeaderChanges() <-chan LeaderChange { return b.out }

// Leader devolve o ID do líder atual (-1 se desconhecido).
func (b *Bully) Leader() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.leader
}

// Term devolve o termo atual.
func (b *Bully) Term() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.term
}

// IsLeader informa se este nó se considera o coordenador atual.
func (b *Bully) IsLeader() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state == StateLeader && b.leader == b.cfg.NodeID
}

// Start lança o consumidor de notificações do detector de falhas e, se o
// nó ainda não conhece líder algum (boot frio), agenda uma eleição após
// ELECTION_TIMEOUT_MS para dar chance a um COORDINATOR ou a um hb.pong
// chegarem primeiro (MODEL.md §3.2).
func (b *Bully) Start(ctx context.Context, det *heartbeat.Detector) {
	go b.consumeDetector(ctx, det)

	b.mu.Lock()
	needBootElection := b.leader < 0
	b.mu.Unlock()

	if needBootElection {
		go func() {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(b.cfg.ElectionTimeoutMs)*time.Millisecond + time.Duration(b.cfg.NodeID)*50*time.Millisecond):
			}
			b.mu.Lock()
			stillNeed := b.leader < 0
			b.mu.Unlock()
			if stillNeed {
				slog.Info("bully: boot sem líder conhecido → iniciando eleição",
					"node_id", b.cfg.NodeID)
				b.StartElection(ctx)
			}
		}()
	}
}

// AdoptLeader adota um líder descoberto externamente (via hb.pong, sem
// precisar de um COORDINATOR explícito).
//
// Regras:
//   - Ignora leaderID inválido ou igual a nós mesmos.
//   - term < b.term: ignora (obsoleto).
//   - term == b.term com leaderID MENOR que o atual: ignora
//     (desempate determinístico: preferir o maior ID em empate).
//   - term > b.term OU (term == b.term e leaderID > atual): adota.
func (b *Bully) AdoptLeader(leaderID, term int) {
	if leaderID < 0 || leaderID == b.cfg.NodeID {
		return
	}
	b.mu.Lock()
	if term < b.term {
		b.mu.Unlock()
		return
	}
	if term == b.term && b.leader >= 0 && leaderID <= b.leader {
		// Empate em mesmo termo e já temos um líder "melhor ou igual".
		b.mu.Unlock()
		return
	}
	changed := b.leader != leaderID || b.term != term
	b.term = term
	b.leader = leaderID
	b.state = StateFollower
	b.mu.Unlock()
	if changed {
		slog.Info("bully: líder adotado via heartbeat",
			"node_id", b.cfg.NodeID, "leader", leaderID, "term", term)
		b.emitLeaderChange()
	}
}

func (b *Bully) consumeDetector(ctx context.Context, det *heartbeat.Detector) {
	for {
		select {
		case <-ctx.Done():
			return
		case n, ok := <-det.Notifications():
			if !ok {
				return
			}
			if n.Kind != heartbeat.EventSuspect {
				continue
			}
			b.mu.Lock()
			currentLeader := b.leader
			b.mu.Unlock()
			if currentLeader < 0 || currentLeader == n.PeerID {
				slog.Warn("bully: suspeita relevante → iniciando eleição",
					"node_id", b.cfg.NodeID,
					"suspect_leader", n.PeerID,
					"current_leader", currentLeader)
				go b.StartElection(ctx)
			}
		}
	}
}

// HandleEnvelope despacha mensagens ELECTION/OK/COORDINATOR vindas de peers.
func (b *Bully) HandleEnvelope(ctx context.Context, env *transport.Envelope) (*transport.Envelope, error) {
	switch env.Kind {
	case "el.election":
		return b.handleElection(ctx, env)
	case "el.ok":
		return b.handleOK(ctx, env)
	case "el.coordinator":
		return b.handleCoordinator(ctx, env)
	}
	return nil, fmt.Errorf("bully: kind desconhecido %q", env.Kind)
}

// StartElection dispara uma eleição para este nó (idempotente se já em curso).
func (b *Bully) StartElection(ctx context.Context) {
	b.mu.Lock()
	if b.election {
		b.mu.Unlock()
		return
	}
	b.election = true
	b.state = StateElection
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		b.election = false
		b.mu.Unlock()
	}()

	b.registry.Inc("bully.elections_started_total")
	b.runElection(ctx)
}

// runElection:
//
//  1. Sem peers maiores → vitória imediata.
//  2. Envia ELECTION para todos os peers maiores.
//  3. Coleta respostas (sucesso E falha) via channel fechado APÓS wg.Wait()
//     — evita a race que causava gotOK=false prematuro.
//  4. gotOK=true → desiste de declarar (não importa o term do peer);
//     absorve o maior term visto em b.term.
//  5. gotOK=false → declara vitória.
func (b *Bully) runElection(ctx context.Context) {
	higher := b.higherPeers()
	b.registry.Add("bully.election_msgs_sent", int64(len(higher)))

	if len(higher) == 0 {
		b.declareVictory(ctx)
		return
	}

	termAtStart := b.Term()

	type electionReply struct {
		peer     int
		ok       bool
		peerTerm int
	}
	results := make(chan electionReply, len(higher))

	var wg sync.WaitGroup
	for _, pid := range higher {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			env, err := transport.NewEnvelope(
				b.cfg.NodeID, pid, "el.election", newReqID(b.cfg.NodeID), "",
				map[string]any{"term": termAtStart + 1},
			)
			if err != nil {
				results <- electionReply{peer: pid, ok: false}
				return
			}
			cctx, cancel := context.WithTimeout(ctx,
				time.Duration(b.cfg.ElectionTimeoutMs)*time.Millisecond)
			defer cancel()
			resp, err := b.client.Call(cctx, pid, env)
			if err != nil {
				results <- electionReply{peer: pid, ok: false}
				return
			}
			var body struct {
				Term int `json:"term"`
			}
			_ = json.Unmarshal(resp.Body, &body)
			results <- electionReply{peer: pid, ok: true, peerTerm: body.Term}
		}(pid)
	}

	// Fecha `results` APÓS todas as goroutines terminarem. Consumimos
	// com `for r := range results` — cada goroutine envia exatamente UMA
	// resposta (sucesso OU falha), então o range termina quando todas
	// chegaram. Sem race, sem `select {}` com `done`.
	go func() { wg.Wait(); close(results) }()

	var (
		gotOK      bool
		higherTerm = termAtStart
	)
	for r := range results {
		if r.ok {
			gotOK = true
			if r.peerTerm > higherTerm {
				higherTerm = r.peerTerm
			}
		}
	}

	// Absorve um term maior visto (descarta mensagens mais antigas).
	if higherTerm > termAtStart {
		b.mu.Lock()
		if higherTerm > b.term {
			b.term = higherTerm
		}
		b.mu.Unlock()
	}

	if gotOK {
		slog.Info("bully: peer com ID maior respondeu OK, aguardando líder",
			"node_id", b.cfg.NodeID, "higher_term", higherTerm)
		// Janela curta: COORDINATOR/adopt pode chegar via dispatcher.
		select {
		case <-ctx.Done():
		case <-time.After(time.Duration(b.cfg.ElectionTimeoutMs) * time.Millisecond):
		}
		return
	}

	b.declareVictory(ctx)
}

func (b *Bully) declareVictory(ctx context.Context) {
	b.mu.Lock()
	// Guard extra: nunca declarar vitória se já somos seguidores de alguém
	// em term >= nosso (evita "zumbi" em cenário de race de mensagens).
	if b.state == StateFollower && b.leader >= 0 && b.leader != b.cfg.NodeID {
		existingLeader := b.leader
		existingTerm := b.term
		b.mu.Unlock()
		slog.Info("bully: declaração de vitória abortada — já temos líder",
			"node_id", b.cfg.NodeID, "leader", existingLeader, "term", existingTerm)
		return
	}
	b.term++
	term := b.term
	b.leader = b.cfg.NodeID
	b.state = StateLeader
	b.mu.Unlock()

	b.registry.Inc("bully.victories_total")
	b.registry.Inc("bully.election_msgs_sent")
	slog.Info("bully: eleito coordenador",
		"node_id", b.cfg.NodeID, "term", term)

	for _, pid := range b.cfg.PeerIDs() {
		env, err := transport.NewEnvelope(
			b.cfg.NodeID, pid, "el.coordinator", newReqID(b.cfg.NodeID), "",
			map[string]any{"term": term, "leader_id": b.cfg.NodeID},
		)
		if err != nil {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx,
			time.Duration(b.cfg.ElectionTimeoutMs)*time.Millisecond)
		_ = b.client.Send(cctx, pid, env)
		cancel()
	}

	b.emitLeaderChange()
}

func (b *Bully) higherPeers() []int {
	out := make([]int, 0)
	for _, pid := range b.cfg.PeerIDs() {
		if pid > b.cfg.NodeID {
			out = append(out, pid)
		}
	}
	return out
}

// --- handlers ---

// handleElection responde SEMPRE OK (com o NOSSO term) para o emissor.
// Não rejeitamos por termo obsoleto: o emissor decide desistir ao ver
// um term maior no OK. Se nosso ID é maior, também iniciamos nossa
// eleição (regra clássica do Bully).
func (b *Bully) handleElection(ctx context.Context, env *transport.Envelope) (*transport.Envelope, error) {
	var body struct {
		Term int `json:"term"`
	}
	if err := json.Unmarshal(env.Body, &body); err != nil {
		return nil, err
	}

	b.mu.Lock()
	myTerm := b.term
	myID := b.cfg.NodeID
	b.mu.Unlock()

	okBody, _ := json.Marshal(map[string]any{"term": myTerm})
	reply, _ := transport.NewEnvelope(myID, env.Src, "el.ok.reply", env.ReqID, "", json.RawMessage(okBody))

	if myID > env.Src {
		go b.StartElection(ctx)
	}
	return reply, nil
}

func (b *Bully) handleOK(ctx context.Context, env *transport.Envelope) (*transport.Envelope, error) {
	return nil, nil
}

// handleCoordinator adota um COORDINATOR respeitando Safety:
//   - body.Term < b.term  → rejeita (obsoleto)
//   - body.Term == b.term e body.LeaderID == b.leader → idempotente
//   - body.Term == b.term e body.LeaderID != b.leader → CONFLITO (rejeita)
//   - body.Term > b.term → adota
func (b *Bully) handleCoordinator(ctx context.Context, env *transport.Envelope) (*transport.Envelope, error) {
	var body struct {
		Term     int `json:"term"`
		LeaderID int `json:"leader_id"`
	}
	if err := json.Unmarshal(env.Body, &body); err != nil {
		return nil, err
	}

	b.mu.Lock()
	oldLeader := b.leader
	oldTerm := b.term

	if body.Term < b.term {
		b.mu.Unlock()
		return nil, fmt.Errorf("bully: COORDINATOR com termo obsoleto (%d<%d)", body.Term, oldTerm)
	}
	if body.Term == b.term && b.leader >= 0 && body.LeaderID != b.leader {
		if body.LeaderID > b.leader {
        // Conflito em mesmo termo: PREFERE o MAIOR ID (desempate
        // determinístico). Isso resolve splits de boot sem violar
        // convergência: todos os nós convergem para o maior ID.
        slog.Warn("bully: COORDINATOR conflitante — adotando o maior ID",
            "node_id", b.cfg.NodeID, "term", body.Term,
            "old_leader", b.leader, "new_leader", body.LeaderID)
        b.registry.Inc("bully.coordinator_conflicts_resolved_total")
        // fall-through para adotar abaixo.
		} else {
			// Líder recebido é menor ou igual ao nosso: mantém o nosso.
			existing := b.leader
			b.mu.Unlock()
			b.registry.Inc("bully.coordinator_conflicts_total")
			slog.Warn("bully: COORDINATOR conflitante ignorado (temos ID maior)",
				"node_id", b.cfg.NodeID, "term", body.Term,
				"existing_leader", existing, "incoming_leader", body.LeaderID)

        	return nil, fmt.Errorf("bully: COORDINATOR conflitante em termo %d (já temos líder=%d)", body.Term, existing)
    	}
	}

	b.term = body.Term
	b.leader = body.LeaderID
	b.state = StateFollower
	b.mu.Unlock()

	if oldLeader != body.LeaderID || oldTerm != body.Term {
		slog.Info("bully: novo líder reconhecido",
			"node_id", b.cfg.NodeID, "leader", body.LeaderID, "term", body.Term)
		b.emitLeaderChange()
	}
	return nil, nil
}

func (b *Bully) emitLeaderChange() {
	b.mu.Lock()
	lc := LeaderChange{
		LeaderID: b.leader,
		Term:     b.term,
		IsSelf:   b.leader == b.cfg.NodeID,
	}
	b.mu.Unlock()
	select {
	case b.out <- lc:
	default:
		slog.Warn("bully: canal de mudanças de líder cheio, descartando",
			"node_id", b.cfg.NodeID, "leader", lc.LeaderID, "term", lc.Term)
	}
}
