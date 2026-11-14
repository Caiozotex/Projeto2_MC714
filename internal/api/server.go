package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"distributed-tx/internal/bully"
	"distributed-tx/internal/config"
	"distributed-tx/internal/metrics"
	"distributed-tx/internal/twopc"
)

// Dependencies agrega tudo que a API precisa. Montamos via struct para
// evitar muitos parâmetros no construtor e facilitar a criação de test
// doubles.
type Dependencies struct {
	Cfg       *config.Config
	Registry  *metrics.Registry
	Bully     *bully.Bully
	Coord     *twopc.Coordinator
	Part      *twopc.Participant
	Store     *twopc.Store
	Shard     *twopc.Shard
	StartedAt time.Time
}

// Server é a fachada HTTP externa.
type Server struct {
	deps Dependencies

	// partMu protege a matriz de partição local (modificável via
	// /admin/partition). Chave = [src][dst].
	partMu      sync.Mutex
	partitions  map[[2]int]bool

	httpSrv *http.Server
	started atomic.Bool
}

// NewServer monta o handler HTTP com todas as rotas.
func NewServer(deps Dependencies) *Server {
	if deps.StartedAt.IsZero() {
		deps.StartedAt = time.Now()
	}
	s := &Server{
		deps:       deps,
		partitions: make(map[[2]int]bool),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /tx", s.handleTx)
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /leader", s.handleLeader)
	mux.HandleFunc("GET /metrics", s.deps.Registry.HTTPHandler().ServeHTTP)
	mux.HandleFunc("POST /admin/partition", s.handlePartitionSet)
	mux.HandleFunc("GET /admin/partition", s.handlePartitionGet)

	s.httpSrv = &http.Server{
		Addr:              deps.Cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	return s
}

// ListenAndServe bloqueia até Close() ou erro.
func (s *Server) ListenAndServe() error {
	if !s.started.CompareAndSwap(false, true) {
		return fmt.Errorf("api: servidor já iniciado")
	}
	slog.Info("api: escutando", "node_id", s.deps.Cfg.NodeID, "addr", s.deps.Cfg.HTTPAddr)
	return s.httpSrv.ListenAndServe()
}

// Close faz shutdown gracioso com deadline fixo.
func (s *Server) Close(ctx context.Context) error {
	return s.httpSrv.Shutdown(ctx)
}

// PartitionFunc devolve a função que o transport.Client vai usar para
// decidir se uma aresta está particionada. Thread-safe.
func (s *Server) PartitionFunc() func(src, dst int) bool {
	return func(src, dst int) bool {
		s.partMu.Lock()
		defer s.partMu.Unlock()
		return s.partitions[[2]int{src, dst}]
	}
}

// --- handlers ---

func (s *Server) handleTx(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	var req TxRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "json inválido: "+err.Error())
		return
	}
	if req.TxID == "" || len(req.Ops) == 0 {
		writeErr(w, http.StatusBadRequest, "tx_id e ops são obrigatórios")
		return
	}

	// Se já vimos este tx_id, respondemos idempotentemente com a decisão
	// registrada (retry de cliente / at-least-once delivery).
	if st := s.deps.Store.Get(req.TxID); st != nil {
		if st.State == twopc.PartCommit || st.State == twopc.CoordCommit {
			writeJSON(w, http.StatusOK, TxResponse{
				TxID: req.TxID, Decision: twopc.DecisionCommit,
				IsLeader: s.deps.Bully.IsLeader(),
				LeaderID: s.deps.Bully.Leader(),
				LatencyMs: time.Since(start).Milliseconds(),
				Reason:   "replay idempotente",
			})
			return
		}
		if st.State == twopc.PartAbort || st.State == twopc.CoordAbort {
			writeJSON(w, http.StatusOK, TxResponse{
				TxID: req.TxID, Decision: twopc.DecisionAbort,
				IsLeader: s.deps.Bully.IsLeader(),
				LeaderID: s.deps.Bully.Leader(),
				LatencyMs: time.Since(start).Milliseconds(),
				Reason:   "replay idempotente",
			})
			return
		}
	}

	// Só o líder aceita novas transações (contrato do MODEL.md §4).
	if !s.deps.Bully.IsLeader() {
		writeErr(w, http.StatusServiceUnavailable,
			fmt.Sprintf("nó %d não é líder (líder atual=%d)", s.deps.Cfg.NodeID, s.deps.Bully.Leader()))
		return
	}

	// Deduplica chaves de conta -> conjunto de participantes.
	parts := s.ownersOf(req.Ops)
	if len(parts) == 0 {
		// Nenhum dono local — teoricamente impossível, mas seguro.
		writeErr(w, http.StatusInternalServerError, "nenhum owner mapeado para as ops")
		return
	}

	ops := make([]twopc.Op, 0, len(req.Ops))
	for _, o := range req.Ops {
		ops = append(ops, twopc.Op{Account: o.Account, Delta: o.Delta})
	}

	decision, err := s.deps.Coord.Execute(r.Context(), req.TxID, ops, parts)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "2pc: "+err.Error())
		return
	}
	latency := time.Since(start).Milliseconds()
	s.deps.Registry.Observe("api.tx_latency_ms", float64(latency))

	writeJSON(w, http.StatusOK, TxResponse{
		TxID:      req.TxID,
		Decision:  decision,
		LeaderID:  s.deps.Cfg.NodeID,
		IsLeader:  true,
		LatencyMs: latency,
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, HealthResponse{
		NodeID:    s.deps.Cfg.NodeID,
		LeaderID:  s.deps.Bully.Leader(),
		Term:      s.deps.Bully.Term(),
		IsLeader:  s.deps.Bully.IsLeader(),
		UpSinceMs: s.deps.StartedAt.UnixMilli(),
	})
}

func (s *Server) handleLeader(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, LeaderResponse{
		NodeID:   s.deps.Cfg.NodeID,
		LeaderID: s.deps.Bully.Leader(),
		Term:     s.deps.Bully.Term(),
		IsLeader: s.deps.Bully.IsLeader(),
	})
}

func (s *Server) handlePartitionSet(w http.ResponseWriter, r *http.Request) {
	var req PartitionRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<18)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "json inválido: "+err.Error())
		return
	}
	s.partMu.Lock()
	for _, e := range req.Set {
		s.partitions[e] = true
	}
	for _, e := range req.Clear {
		delete(s.partitions, e)
	}
	snapshot := make([][2]int, 0, len(s.partitions))
	for e := range s.partitions {
		snapshot = append(snapshot, e)
	}
	s.partMu.Unlock()

	slog.Warn("api: matriz de partição alterada",
		"node_id", s.deps.Cfg.NodeID, "size", len(snapshot))
	writeJSON(w, http.StatusOK, PartitionResponse{
		NodeID: s.deps.Cfg.NodeID, Partitioned: sortedEdges(snapshot),
	})
}

func (s *Server) handlePartitionGet(w http.ResponseWriter, _ *http.Request) {
	s.partMu.Lock()
	snapshot := make([][2]int, 0, len(s.partitions))
	for e := range s.partitions {
		snapshot = append(snapshot, e)
	}
	s.partMu.Unlock()
	writeJSON(w, http.StatusOK, PartitionResponse{
		NodeID: s.deps.Cfg.NodeID, Partitioned: sortedEdges(snapshot),
	})
}

// --- helpers ---

// ownersOf retorna os IDs de peers que possuem pelo menos uma das contas.
// A regra de sharding é determinística: fnv32a(account) % N.
func (s *Server) ownersOf(ops []OpReq) []int {
	seen := make(map[int]bool)
	n := len(s.deps.Cfg.Peers)
	if n == 0 {
		return nil
	}
	for _, o := range ops {
		idx := int(fnv32a(o.Account) % uint32(n))
		// Mapeia índice 0..n-1 para IDs reais ordenados (estáveis).
		ids := sortedPeerIDs(s.deps.Cfg.Peers)
		if idx < len(ids) {
			seen[ids[idx]] = true
		}
	}
	out := make([]int, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Ints(out)
	return out
}

func sortedPeerIDs(peers map[int]string) []int {
	out := make([]int, 0, len(peers))
	for id := range peers {
		out = append(out, id)
	}
	sort.Ints(out)
	return out
}

func sortedEdges(in [][2]int) [][2]int {
	sort.Slice(in, func(i, j int) bool {
		if in[i][0] != in[j][0] {
			return in[i][0] < in[j][0]
		}
		return in[i][1] < in[j][1]
	})
	return in
}

// fnv32a implementa FNV-1a 32-bit (determinístico e independente de
// bibliotecas externas). Usado só para sharding, não para segurança.
func fnv32a(s string) uint32 {
	const (
		offset uint32 = 2166136261
		prime  uint32 = 16777619
	)
	h := offset
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime
	}
	return h
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, ErrorResponse{Error: msg})
}
