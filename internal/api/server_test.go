package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"distributed-tx/internal/bully"
	"distributed-tx/internal/config"
	"distributed-tx/internal/metrics"
	"distributed-tx/internal/storage"
	"distributed-tx/internal/twopc"
	"distributed-tx/internal/transport"
)

// fakeBully devolve o comportamento controlado de um bully sem rede.
// Não usamos *bully.Bully diretamente porque ele depende de Client e
// goroutines; queremos testar o handler isoladamente.
//
// Em vez de um fake, passamos um *bully.Bully real configurado como líder
// (leader=0, term=1) — o handler só consulta Leader()/Term()/IsLeader().
func makeLeaderBully(t *testing.T, cfg *config.Config) *bully.Bully {
	t.Helper()
	// Não há setter público em bully. Fazemos o truque: chamar
	// declareVictory via StartElection com nenhum peer maior.
	// NodeID = 0 já é o menor; para virar líder, forçamos peerIDs vazios
	// em uma config temporária.
	tmp := *cfg
	tmp.Peers = map[int]string{0: "127.0.0.1:9999"}
	cl := transport.NewClient(0, tmp.Peers, transport.FaultConfig{Seed: 1})
	t.Cleanup(cl.Close)
	b := bully.New(&tmp, cl, metrics.NewRegistry(0))
	return b
}

// Para testes, contornamos o problema acima setando um leader local
// via chamada de HandleEnvelope de COORDINATOR. Essa função reconstrói o
// Bully como líder "de verdade" (state=FOLLOWER, leader=0). Para forçar
// state=LEADER, dependemos de StartElection internamente.
func makeLeader(t *testing.T, cfg *config.Config) *bully.Bully {
	t.Helper()
	// Config com só este peer, NodeID = 0, portanto StartElection declara
	// vitória imediata.
	tmp := *cfg
	tmp.Peers = map[int]string{0: "127.0.0.1:9999"}
	cl := transport.NewClient(0, tmp.Peers, transport.FaultConfig{Seed: 1})
	t.Cleanup(cl.Close)
	b := bully.New(&tmp, cl, metrics.NewRegistry(0))
	b.StartElection(t.Context())
	// A chamada acima em outra goroutine ainda pode estar em curso; damos
	// uma janela generosa.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if b.IsLeader() {
			return b
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("bully não se tornou líder")
	return nil
}

func newTestServer(t *testing.T) (*Server, *config.Config) {
	t.Helper()
	cfg := &config.Config{
		NodeID:              0,
		HTTPAddr:            "127.0.0.1:0",
		ListenAddr:          "127.0.0.1:0",
		TxTimeoutMs:         500,
		HeartbeatIntervalMs: 50,
		HeartbeatTimeoutMs:  150,
		ElectionTimeoutMs:   200,
		Peers: map[int]string{
			0: "127.0.0.1:9990",
			1: "127.0.0.1:9991",
			2: "127.0.0.1:9992",
		},
		WalDir: t.TempDir(),
	}
	wal, err := storage.Open(cfg.WalDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	store := twopc.NewStore(wal)
	shard := twopc.NewShard(1000, []string{"alice", "bob", "carol"})
	reg := metrics.NewRegistry(cfg.NodeID)
	cl := transport.NewClient(cfg.NodeID, cfg.Peers, transport.FaultConfig{Seed: 1})
	t.Cleanup(cl.Close)
	coord := twopc.NewCoordinator(cfg, cl, store, reg)
	part := twopc.NewParticipant(cfg, cl, store, shard, reg)
	b := makeLeader(t, cfg)

	srv := NewServer(Dependencies{
		Cfg: cfg, Registry: reg, Bully: b, Coord: coord, Part: part,
		Store: store, Shard: shard,
	})
	return srv, cfg
}

func TestHealthAndLeaderEndpoints(t *testing.T) {
	srv, _ := newTestServer(t)

	// /healthz
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	// Acessamos o handler via um mux interno: monta rápido.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", srv.handleHealth)
	mux.HandleFunc("GET /leader", srv.handleLeader)
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("/healthz status: %d", rec.Code)
	}
	var hr HealthResponse
	if err := json.NewDecoder(rec.Body).Decode(&hr); err != nil {
		t.Fatal(err)
	}
	if hr.NodeID != 0 || !hr.IsLeader {
		t.Fatalf("/healthz inesperado: %+v", hr)
	}

	// /leader
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/leader", nil))
	var lr LeaderResponse
	if err := json.NewDecoder(rec.Body).Decode(&lr); err != nil {
		t.Fatal(err)
	}
	if lr.LeaderID != 0 || !lr.IsLeader {
		t.Fatalf("/leader inesperado: %+v", lr)
	}
}

func TestPostTxRejectsWithoutTxID(t *testing.T) {
	srv, _ := newTestServer(t)

	body, _ := json.Marshal(TxRequest{Ops: []OpReq{{Account: "alice", Delta: -10}}})
	rec := httptest.NewRecorder()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /tx", srv.handleTx)
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/tx", bytes.NewReader(body)))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("esperava 400, veio %d", rec.Code)
	}
}

func TestPostTxOnNonLeaderReturns503(t *testing.T) {
	srv, _ := newTestServer(t)
	// Forçamos um Bully que NÃO é líder trocando o deps.
	tmpCfg := *srv.deps.Cfg
	tmpCfg.NodeID = 0
	cl := transport.NewClient(0, tmpCfg.Peers, transport.FaultConfig{Seed: 2})
	t.Cleanup(cl.Close)
	nonLeader := bully.New(&tmpCfg, cl, metrics.NewRegistry(0))
	// nonLeader começa em FOLLOWER, leader = -1 → IsLeader()=false.
	srv.deps.Bully = nonLeader

	body, _ := json.Marshal(TxRequest{
		TxID: "t-non-leader",
		Ops:  []OpReq{{Account: "alice", Delta: -10}},
	})
	rec := httptest.NewRecorder()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /tx", srv.handleTx)
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/tx", bytes.NewReader(body)))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("esperava 503, veio %d", rec.Code)
	}
}

func TestPartitionSetAndGet(t *testing.T) {
	srv, _ := newTestServer(t)

	body, _ := json.Marshal(PartitionRequest{Set: [][2]int{{0, 1}, {1, 0}}})
	rec := httptest.NewRecorder()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /admin/partition", srv.handlePartitionSet)
	mux.HandleFunc("GET /admin/partition", srv.handlePartitionGet)
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/partition", bytes.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("POST partition status: %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/partition", nil))
	var pr PartitionResponse
	if err := json.NewDecoder(rec.Body).Decode(&pr); err != nil {
		t.Fatal(err)
	}
	if len(pr.Partitioned) != 2 {
		t.Fatalf("esperava 2 arestas, veio %d (%v)", len(pr.Partitioned), pr.Partitioned)
	}

	fn := srv.PartitionFunc()
	if !fn(0, 1) || !fn(1, 0) || fn(0, 2) {
		t.Fatalf("PartitionFunc inconsistente")
	}
}
