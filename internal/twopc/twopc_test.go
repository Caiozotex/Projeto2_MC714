package twopc

import (
	"context"
	"testing"

	"distributed-tx/internal/config"
	"distributed-tx/internal/metrics"
	"distributed-tx/internal/storage"
)

// newTestCfg cria uma config mínima só para os testes.
func newTestCfg(t *testing.T) *config.Config {
	t.Helper()
	return &config.Config{
		NodeID:              0,
		TxTimeoutMs:         500,
		HeartbeatIntervalMs: 50,
		HeartbeatTimeoutMs:  150,
		ElectionTimeoutMs:   200,
		Peers: map[int]string{
			0: "127.0.0.1:9000",
			1: "127.0.0.1:9001",
			2: "127.0.0.1:9002",
		},
		WalDir: t.TempDir(),
	}
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	wal, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatalf("WAL: %v", err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	return NewStore(wal)
}

func newTestReg() *metrics.Registry { return metrics.NewRegistry(0) }

// TestParticipantAbortInsufficientFunds valida que o participante vota
// ABORT se a operação tornaria algum saldo negativo.
func TestParticipantAbortInsufficientFunds(t *testing.T) {
	store := newTestStore(t)
	shard := NewShard(100, []string{"alice", "bob"})
	p := NewParticipant(newTestCfg(t), nil, store, shard, newTestReg())

	vote := p.HandleVoteRequest(context.Background(), "tx-1", VoteRequest{
		Ops: []Op{{Account: "alice", Delta: -150}},
	})
	if vote.Vote != "ABORT" {
		t.Fatalf("esperava ABORT, veio %q (motivo: %s)", vote.Vote, vote.Reason)
	}
	if got := shard.Balance("alice"); got != 100 {
		t.Fatalf("saldo não deveria mudar; veio %d", got)
	}
}

// TestParticipantReadyThenCommit valida o caminho feliz.
func TestParticipantReadyThenCommit(t *testing.T) {
	store := newTestStore(t)
	shard := NewShard(100, []string{"alice", "bob"})
	p := NewParticipant(newTestCfg(t), nil, store, shard, newTestReg())

	txID := "tx-2"
	ops := []Op{
		{Account: "alice", Delta: -40},
		{Account: "bob", Delta: +40},
	}
	vote := p.HandleVoteRequest(context.Background(), txID, VoteRequest{Ops: ops})
	if vote.Vote != "COMMIT" {
		t.Fatalf("esperava COMMIT, veio %q", vote.Vote)
	}
	if st := store.Get(txID); st == nil || st.State != PartReady {
		t.Fatalf("estado esperado READY, veio %+v", st)
	}
	if got := shard.Balance("alice"); got != 100 {
		t.Fatalf("saldo antes do global deveria ser 100; veio %d", got)
	}

	ack := p.HandleGlobalDecision(context.Background(), txID, GlobalDecision{Decision: DecisionCommit})
	if !ack.Applied {
		t.Fatal("ack.Applied deveria ser true")
	}
	if got := shard.Balance("alice"); got != 60 {
		t.Fatalf("alice esperado 60; veio %d", got)
	}
	if got := shard.Balance("bob"); got != 140 {
		t.Fatalf("bob esperado 140; veio %d", got)
	}
	if st := store.Get(txID); st == nil || st.State != PartCommit {
		t.Fatalf("estado esperado COMMIT, veio %+v", st)
	}
}

// TestIdempotentGlobalDecision valida que COMMIT repetido não duplica ops.
func TestIdempotentGlobalDecision(t *testing.T) {
	store := newTestStore(t)
	shard := NewShard(100, []string{"alice"})
	p := NewParticipant(newTestCfg(t), nil, store, shard, newTestReg())

	txID := "tx-3"
	vote := p.HandleVoteRequest(context.Background(), txID, VoteRequest{
		Ops: []Op{{Account: "alice", Delta: -30}},
	})
	if vote.Vote != "COMMIT" {
		t.Fatalf("esperava COMMIT, veio %q", vote.Vote)
	}

	p.HandleGlobalDecision(context.Background(), txID, GlobalDecision{Decision: DecisionCommit})
	p.HandleGlobalDecision(context.Background(), txID, GlobalDecision{Decision: DecisionCommit})
	p.HandleGlobalDecision(context.Background(), txID, GlobalDecision{Decision: DecisionCommit})

	if got := shard.Balance("alice"); got != 70 {
		t.Fatalf("idempotência violada: saldo deveria ser 70, veio %d", got)
	}
}

// TestNoDecisionAfterAbort valida irrevogabilidade de COMMIT.
func TestNoDecisionAfterAbort(t *testing.T) {
	store := newTestStore(t)
	shard := NewShard(100, []string{"alice"})
	p := NewParticipant(newTestCfg(t), nil, store, shard, newTestReg())

	txID := "tx-4"
	_ = p.HandleVoteRequest(context.Background(), txID, VoteRequest{
		Ops: []Op{{Account: "alice", Delta: -10}},
	})
	p.HandleGlobalDecision(context.Background(), txID, GlobalDecision{Decision: DecisionCommit})
	p.HandleGlobalDecision(context.Background(), txID, GlobalDecision{Decision: DecisionAbort})

	if got := shard.Balance("alice"); got != 90 {
		t.Fatalf("saldo deveria permanecer 90; veio %d", got)
	}
	st := store.Get(txID)
	if st.State != PartCommit || st.Decision != DecisionCommit {
		t.Fatalf("estado deveria ser COMMIT/COMMIT; veio %+v", st)
	}
}

// TestWalReplayReconstructsState valida que ReplayWAL reconstrói decisões.
func TestWalReplayReconstructsState(t *testing.T) {
	dir := t.TempDir()
	wal1, err := storage.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	store1 := NewStore(wal1)
	shard := NewShard(100, []string{"alice", "bob"})
	p := NewParticipant(newTestCfg(t), nil, store1, shard, newTestReg())

	txID := "tx-wal-replay"
	_ = p.HandleVoteRequest(context.Background(), txID, VoteRequest{
		Ops: []Op{{Account: "alice", Delta: -20}},
	})
	p.HandleGlobalDecision(context.Background(), txID, GlobalDecision{Decision: DecisionCommit})
	_ = wal1.Close()

	wal2, err := storage.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal2.Close() })
	store2 := NewStore(wal2)
	if err := store2.ReplayWAL(); err != nil {
		t.Fatalf("ReplayWAL: %v", err)
	}
	st := store2.Get(txID)
	if st == nil {
		t.Fatalf("tx %s não encontrada no replay", txID)
	}
	if st.State != PartCommit || st.Decision != DecisionCommit {
		t.Fatalf("replay inconsistente: %+v", st)
	}
}
