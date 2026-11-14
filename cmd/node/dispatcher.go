// rpcDispatcher roteia envelopes recebidos pelo transport.Server para o
// módulo correto (bully ou 2PC). Também atualiza o Detector de Falhas a
// cada mensagem recebida (lastSeen), requisito do MODEL.md §3.1.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"distributed-tx/internal/bully"
	"distributed-tx/internal/config"
	"distributed-tx/internal/heartbeat"
	"distributed-tx/internal/metrics"
	"distributed-tx/internal/transport"
	"distributed-tx/internal/twopc"
)

type rpcDispatcher struct {
	cfg         *config.Config
	bullyMod    *bully.Bully
	participant *twopc.Participant
	detector    *heartbeat.Detector
	registry    *metrics.Registry
}

// Handle roteia por prefixo de `kind`:
//   - "hb.*"      → responde pong e atualiza lastSeen
//   - "el.*"      → bully
//   - "2pc.*"     → participante 2PC
//
// Toda mensagem recebida (de QUALQUER kind) atualiza lastSeen do peer
// emissor via detector.MarkAlive — isso é o que sustenta a precisão
// eventual do detector (MODEL.md §3.1).
func (d *rpcDispatcher) Handle(ctx context.Context, env *transport.Envelope) (*transport.Envelope, error) {
	// 1. Marca o peer emissor como vivo ANTES de qualquer coisa.
	if d.detector != nil && env.Src != d.cfg.NodeID {
		// tenta extrair o termo (se presente) do corpo; ignoramos erro.
		d.detector.MarkAlive(env.Src, 0)
	}

	switch {
	case strings.HasPrefix(env.Kind, "hb."):
		return d.handleHeartbeat(ctx, env)
	case strings.HasPrefix(env.Kind, "el."):
		return d.bullyMod.HandleEnvelope(ctx, env)
	case strings.HasPrefix(env.Kind, "2pc."):
		return d.participant.HandleEnvelope(ctx, env)
	}
	return nil, fmt.Errorf("dispatcher: kind desconhecido %q", env.Kind)
}

func (d *rpcDispatcher) handleHeartbeat(_ context.Context, env *transport.Envelope) (*transport.Envelope, error) {
	d.registry.Inc("heartbeat.received_total")
	pong := map[string]any{
		"term":      d.bullyMod.Term(),
		"is_leader": d.bullyMod.IsLeader(),
                "leader_id": d.bullyMod.Leader(),
	}
	raw, _ := marshalBody(pong)
	resp := &transport.Envelope{
		V:        1,
		Src:      d.cfg.NodeID,
		Dst:      env.Src,
		Kind:     "hb.pong",
		ReqID:    env.ReqID,
		SentAtMs: nowMillis(),
		Body:     raw,
	}
	slog.Debug("dispatcher: heartbeat recebido", "from", env.Src)
	return resp, nil
}
