// Package heartbeat implementa o detector de falhas baseado em heartbeats
// periódicos com timeout adaptativo por par, aproximando um detector da
// classe "eventualmente perfeito" (◇P) em sistemas parcialmente síncronos.
//
// Propriedades visadas (MODEL.md §3.1):
//   - Completude Forte: se q falha permanentemente, todo p correto
//     eventualmente marca q em Suspects.
//   - Precisão Forte Eventual: sob estabilidade, p remove q de Suspects ao
//     receber QUALQUER mensagem de q (heartbeat, ELECTION, vote_req, ...),
//     aumentando Timeout[q] (backoff aditivo) para reduzir falsos positivos
//     sob jitter.
//
// Integração:
//   - O dispatcher RPC chama MarkAlive(peer, term) em TODA mensagem recebida
//     (não apenas hb.pong), satisfazendo "last_seen = qualquer tráfego".
//   - sendPings usa Call (request/response) e, ao receber o pong, chama
//     MarkAlive e — se o pong trouxer leader_id >= 0 — dispara o callback
//     onLeaderDiscovered para o Bully adotar o líder atual.
package heartbeat

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"distributed-tx/internal/config"
	"distributed-tx/internal/metrics"
	"distributed-tx/internal/transport"
)

// Event é emitido para o módulo Bully quando a suspeita muda.
type Event int

const (
	EventSuspect Event = iota
	EventUnsuspect
)

// Notification descreve a mudança de estado de um peer.
type Notification struct {
	Kind   Event
	PeerID int
	Term   int // último termo conhecido do peer (opcional, via hb.pong)
}

// Detector envia heartbeats periódicos a todos os peers e mantém
// suspects/timeouts por peer, notificando via canal.
type Detector struct {
	cfg      *config.Config
	client   *transport.Client
	registry *metrics.Registry

	// onLeaderDiscovered é chamado quando um hb.pong traz leader_id válido.
	// Registrado pelo main via SetLeaderCallback (evita import cycle
	// heartbeat -> bully).
	onLeaderDiscovered func(leaderID, term int)

	mu       sync.Mutex
	timeouts map[int]time.Duration // timeout[q] adaptativo
	suspects map[int]bool
	lastSeen map[int]time.Time
	terms    map[int]int // último termo relatado pelo peer

	// Timeouts originais para reajuste.
	baseTimeout time.Duration
	maxTimeout  time.Duration

	out chan Notification
}

// NewDetector cria o detector com timeout inicial = cfg.HeartbeatTimeoutMs.
// O limite superior do backoff é 8× o timeout base (evita explosão).
func NewDetector(cfg *config.Config, client *transport.Client, reg *metrics.Registry) *Detector {
	base := time.Duration(cfg.HeartbeatTimeoutMs) * time.Millisecond
	return &Detector{
		cfg:         cfg,
		client:      client,
		registry:    reg,
		timeouts:    make(map[int]time.Duration),
		suspects:    make(map[int]bool),
		lastSeen:    make(map[int]time.Time),
		terms:       make(map[int]int),
		baseTimeout: base,
		maxTimeout:  base * 8,
		out:         make(chan Notification, 128),
	}
}

// SetLeaderCallback registra a função invocada quando um hb.pong revela um
// líder. Deve ser chamada antes de Start (o main conecta isso ao
// bully.AdoptLeader, criando a ponte heartbeat → bully).
func (d *Detector) SetLeaderCallback(fn func(leaderID, term int)) {
	d.mu.Lock()
	d.onLeaderDiscovered = fn
	d.mu.Unlock()
}

// Notifications devolve o canal de eventos (consumido pelo Bully).
func (d *Detector) Notifications() <-chan Notification { return d.out }

// TermOf devolve o último termo observado para o peer (0 se desconhecido).
func (d *Detector) TermOf(peer int) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.terms[peer]
}

// IsSuspected informa se o peer está no conjunto de suspeitos.
func (d *Detector) IsSuspected(peer int) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.suspects[peer]
}

// Suspects devolve uma cópia do conjunto atual.
func (d *Detector) Suspects() []int {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]int, 0, len(d.suspects))
	for id := range d.suspects {
		out = append(out, id)
	}
	return out
}

// TimeoutOf devolve o timeout adaptativo atual do peer (para métricas/logs).
func (d *Detector) TimeoutOf(peer int) time.Duration {
	d.mu.Lock()
	defer d.mu.Unlock()
	if t, ok := d.timeouts[peer]; ok {
		return t
	}
	return d.baseTimeout
}

// Start lança duas goroutines: envio periódico de pings e sweep de timeouts.
func (d *Detector) Start(ctx context.Context) {
	interval := time.Duration(d.cfg.HeartbeatIntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}

	// Inicializa timeout[peer] = base.
	d.mu.Lock()
	for _, pid := range d.cfg.PeerIDs() {
		d.timeouts[pid] = d.baseTimeout
		d.lastSeen[pid] = time.Now()
	}
	d.mu.Unlock()

	go d.pingLoop(ctx, interval)
	go d.sweepLoop(ctx, interval/2)
}

func (d *Detector) pingLoop(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			d.sendPings(ctx)
		}
	}
}

// sendPings envia hb.ping para todos os peers com deadline explícito.
// Usa Call (request/response) — fundamental para que lastSeen[peer] seja
// atualizado do lado do emissor. Ao receber o pong, chama MarkAlive e,
// se o pong trouxer leader_id válido, dispara onLeaderDiscovered.
func (d *Detector) sendPings(ctx context.Context) {
	myTerm := d.currentLeaderTermHint()
	for _, pid := range d.cfg.PeerIDs() {
		env, err := transport.NewEnvelope(
			d.cfg.NodeID, pid, "hb.ping", newReqID(d.cfg.NodeID), "",
			map[string]any{"term": myTerm, "epoch": 0},
		)
		if err != nil {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx,
			time.Duration(d.cfg.HeartbeatIntervalMs)*time.Millisecond)
		resp, err := d.client.Call(cctx, pid, env)
		cancel()
		if err != nil {
			// Peer inalcançável neste tick. NÃO marca suspeito aqui:
			// o sweep cuida disso com base em lastSeen, tolerando perdas
			// transitórias (MODEL.md: pings periódicos mascararam perdas).
			d.registry.Inc("heartbeat.ping_fail_total")
			continue
		}
		d.registry.Inc("heartbeat.pong_total")

		// Decodifica o pong (tolerante a formato parcial).
		var pong struct {
			Term     int  `json:"term"`
			IsLeader bool `json:"is_leader"`
			LeaderID int  `json:"leader_id"`
		}
		_ = json.Unmarshal(resp.Body, &pong)

		// Pong recebido = peer vivo. Atualiza lastSeen e remove suspeita.
		d.MarkAlive(pid, pong.Term)

		// Se o peer revelou um líder e não somos nós, adotamos.
		if pong.LeaderID >= 0 && pong.LeaderID != d.cfg.NodeID {
			d.mu.Lock()
			cb := d.onLeaderDiscovered
			d.mu.Unlock()
			if cb != nil {
				cb(pong.LeaderID, pong.Term)
			}
		}
	}
}

func (d *Detector) sweepLoop(ctx context.Context, tick time.Duration) {
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			d.sweep(now)
		}
	}
}

func (d *Detector) sweep(now time.Time) {
	d.mu.Lock()
	type transition struct {
		peer int
		term int
	}
	var transitions []transition
	for _, pid := range d.cfg.PeerIDs() {
		to := d.timeouts[pid]
		if to <= 0 {
			to = d.baseTimeout
		}
		last := d.lastSeen[pid]
		if now.Sub(last) > to && !d.suspects[pid] {
			d.suspects[pid] = true
			transitions = append(transitions, transition{peer: pid, term: d.terms[pid]})
			d.registry.Inc("heartbeat.suspect_total")
		}
	}
	d.mu.Unlock()

	for _, tr := range transitions {
		slog.Warn("heartbeat: peer suspeito",
			"node_id", d.cfg.NodeID, "peer_id", tr.peer,
			"timeout_ms", d.TimeoutOf(tr.peer).Milliseconds())
		d.emit(Notification{Kind: EventSuspect, PeerID: tr.peer, Term: tr.term})
	}
}

// MarkAlive deve ser chamado a cada mensagem recebida de `peer` (pong,
// ELECTION, OK, COORDINATOR, vote_req, ...). Marca lastSeen, remove de
// suspects e aplica backoff ADITIVO se o peer estava suspeito.
//
// Cumpre dois papéis:
//  1. dispatcher.go chama em toda mensagem recebida → "qualquer tráfego
//     conta como sinal de vida" (MODEL.md §3.1).
//  2. sendPings chama ao receber o pong → atualiza lastSeen do emissor.
func (d *Detector) MarkAlive(peer int, term int) {
	d.mu.Lock()
	wasSuspect := d.suspects[peer]
	oldTimeout := d.timeouts[peer]
	if wasSuspect {
		// Backoff aditivo: +25% do base, limitado a maxTimeout.
		newTimeout := oldTimeout + d.baseTimeout/4
		if newTimeout > d.maxTimeout {
			newTimeout = d.maxTimeout
		}
		d.timeouts[peer] = newTimeout
		delete(d.suspects, peer)
		d.registry.Inc("heartbeat.unsuspect_total")
		d.registry.Inc("heartbeat.timeout_adapt_total")
	}
	d.lastSeen[peer] = time.Now()
	if term > d.terms[peer] {
		d.terms[peer] = term
	}
	d.mu.Unlock()

	if wasSuspect {
		slog.Info("heartbeat: peer recuperado",
			"node_id", d.cfg.NodeID, "peer_id", peer,
			"old_timeout_ms", oldTimeout.Milliseconds(),
			"new_timeout_ms", d.TimeoutOf(peer).Milliseconds())
		d.emit(Notification{Kind: EventUnsuspect, PeerID: peer, Term: term})
	}
}

// ResetTimeout volta o timeout do peer ao valor base (usado em testes e
// após uma eleição bem-sucedida que confirme a saúde da rede).
func (d *Detector) ResetTimeout(peer int) {
	d.mu.Lock()
	d.timeouts[peer] = d.baseTimeout
	d.mu.Unlock()
}

func (d *Detector) emit(n Notification) {
	select {
	case d.out <- n:
	default:
		// Canal cheio: descarta evento (o estado é reconstruível).
		slog.Warn("heartbeat: canal de notificações cheio, descartando",
			"node_id", d.cfg.NodeID, "peer_id", n.PeerID, "kind", n.Kind)
	}
}

// currentLeaderTermHint consulta os termos observados para incluir no ping.
// O campo "term" no ping é meramente informativo; peers ignoram valores
// inferiores aos que já conhecem.
func (d *Detector) currentLeaderTermHint() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	max := 0
	for _, t := range d.terms {
		if t > max {
			max = t
		}
	}
	return max
}
