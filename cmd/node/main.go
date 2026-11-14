// Command node é o processo do nó lógico do sistema distribuído MC714.
//
// Responsabilidades:
//   - carregar a configuração via env vars (config.Load)
//   - abrir o WAL e reidratar a Store (replay pós-crash)
//   - subir o servidor TCP interno (:8000) e o cliente de RPC entre peers
//   - iniciar o Detector de Falhas (heartbeat) e o Bully
//   - replay de decisões finais do 2PC no coordenador (reenvio GLOBAL_*)
//   - recovery cooperativo dos participantes presos em READY
//   - subir a API HTTP externa (:8080)
//   - shutdown gracioso em SIGINT/SIGTERM
//
// Todos os módulos rodam sob o mesmo context.Context, cancelado por sinal.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"distributed-tx/internal/api"
	"distributed-tx/internal/bully"
	"distributed-tx/internal/config"
	"distributed-tx/internal/heartbeat"
	"distributed-tx/internal/metrics"
	"distributed-tx/internal/storage"
	"distributed-tx/internal/transport"
	"distributed-tx/internal/twopc"
)

func main() {
	if err := run(); err != nil {
		slog.Error("node: erro fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	// Modo healthcheck (usado pelo HEALTHCHECK do Dockerfile).
	if len(os.Args) > 1 && os.Args[1] == "-healthcheck" {
		addr := os.Getenv("HTTP_ADDR")
		if addr == "" {
			addr = ":8080"
		}
		if addr[0] == ':' {
			addr = "127.0.0.1" + addr
		}
		c := &http.Client{Timeout: 2 * time.Second}
		resp, err := c.Get("http://" + addr + "/healthz")
		if err != nil {
			fmt.Fprintln(os.Stderr, "healthcheck:", err)
			os.Exit(1)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			fmt.Fprintln(os.Stderr, "healthcheck: status", resp.StatusCode)
			os.Exit(1)
		}
		os.Exit(0)
	}

	// 1. Configuração externa.
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	// 2. Logging estruturado em JSON (requisito do trabalho).
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level:     slog.LevelInfo,
		AddSource: false,
	})))

	slog.Info("node: iniciando",
		"node_id", cfg.NodeID,
		"listen", cfg.ListenAddr,
		"http", cfg.HTTPAddr,
		"peers", cfg.Peers,
		"wal_dir", cfg.WalDir,
		"fault_seed", cfg.FaultSeed,
		"net_delay_ms", cfg.NetDelayMs,
		"net_jitter_ms", cfg.NetJitterMs,
		"net_drop_rate", cfg.NetDropRate,
	)

	// 3. Context raiz cancelado por SIGINT/SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 4. WAL + Store com replay.
	wal, err := storage.Open(cfg.WalDir)
	if err != nil {
		return fmt.Errorf("wal: %w", err)
	}
	defer func() { _ = wal.Close() }()

	store := twopc.NewStore(wal)
	if err := store.ReplayWAL(); err != nil {
		return fmt.Errorf("replay wal: %w", err)
	}
	if n := len(store.All()); n > 0 {
		slog.Info("node: WAL reidratado", "node_id", cfg.NodeID, "txs", n)
	}

	// 5. Shard local com um conjunto inicial de contas conhecidas.
	//    Em produção real isso viria de um catálogo; aqui, geramos as contas
	//    padrão do experimento: "alice", "bob", "carol", "dave", "erin".
	shard := twopc.NewShard(1_000_000, []string{
		"alice", "bob", "carol", "dave", "erin",
	})

	// 6. Registro de métricas do nó.
	registry := metrics.NewRegistry(cfg.NodeID)

	// 7. Cliente RPC entre peers.
	rpcClient := transport.NewClient(cfg.NodeID, cfg.Peers, transport.FaultConfig{
		DelayMs:  cfg.NetDelayMs,
		JitterMs: cfg.NetJitterMs,
		DropRate: cfg.NetDropRate,
		Seed:     cfg.FaultSeed,
	})
	defer rpcClient.Close()

	// 8. Bully (precisa do client).
	bullyMod := bully.New(cfg, rpcClient, registry)

	// 9. Coordenador e Participante 2PC.
	coordinator := twopc.NewCoordinator(cfg, rpcClient, store, registry)
	participant := twopc.NewParticipant(cfg, rpcClient, store, shard, registry)

	// 10. Detector de falhas — criado ANTES do dispatcher e do servidor,
	//     para que o dispatcher possa chamar MarkAlive em toda mensagem
	//     recebida (MODEL.md §3.1: precisão eventual depende disso).
	detector := heartbeat.NewDetector(cfg, rpcClient, registry)

	// 11. API HTTP externa — precisa saber como consultar partição do Client.
	apiSrv := api.NewServer(api.Dependencies{
		Cfg:       cfg,
		Registry:  registry,
		Bully:     bullyMod,
		Coord:     coordinator,
		Part:      participant,
		Store:     store,
		Shard:     shard,
		StartedAt: time.Now(),
	})
	rpcClient.SetPartition(apiSrv.PartitionFunc())

	// 12. Servidor TCP interno (heartbeat + bully + 2PC).
	dispatcher := &rpcDispatcher{
		cfg:         cfg,
		bullyMod:    bullyMod,
		participant: participant,
		detector:    detector,
		registry:    registry,
	}
	server := transport.NewServer(cfg.NodeID, cfg.ListenAddr, dispatcher.Handle)
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("node: servidor TCP parou", "err", err)
			stop()
		}
	}()

	// 13. Detector de falhas + Bully (integração).
	//     A ordem importa: o detector começa a enviar pings antes do Bully
	//     consumir notificações, evitando perda de eventos de suspeita.
        detector.SetLeaderCallback(bullyMod.AdoptLeader)
	detector.Start(ctx)
	bullyMod.Start(ctx, detector)

	// 14. Recovery cooperativo dos participantes READY e replay GLOBAL_*.
	go coordinator.ReplayFromWAL(ctx)
	go participant.RecoverReadyPendentes(ctx)

	// 15. API HTTP em goroutine.
	go func() {
		if err := apiSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("node: API parou", "err", err)
			stop()
		}
	}()

	// 16. Loop de status periódico (facilita debug dos logs).
	go statusLoop(ctx, cfg.NodeID, bullyMod, detector, registry)

	slog.Info("node: pronto", "node_id", cfg.NodeID)
	<-ctx.Done()
	slog.Info("node: shutdown iniciado", "node_id", cfg.NodeID)

	// 17. Shutdown gracioso com deadline fixo.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = apiSrv.Close(shutdownCtx)
	_ = server.Close()
	slog.Info("node: encerrado", "node_id", cfg.NodeID)
	return nil
}

// statusLoop loga o estado do nó a cada 5 s (útil no `docker compose logs -f`).
func statusLoop(ctx context.Context, nodeID int, b *bully.Bully, d *heartbeat.Detector, r *metrics.Registry) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			slog.Info("node: status",
				"node_id", nodeID,
				"leader", b.Leader(),
				"term", b.Term(),
				"is_leader", b.IsLeader(),
				"suspects", d.Suspects(),
				"txs_known", len(r.Snapshot()),
			)
		}
	}
}
