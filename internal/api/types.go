// Package api expõe a API HTTP externa (:8080) consumida pelo cliente de
// carga e pelos scripts de experimento. É também onde ficam os endpoints
// administrativos (partição lógica, health, métricas).
package api

// TxRequest é o corpo de POST /tx.
//
// tx_id é OBRIGATÓRIO e deve ser único globalmente (o cliente gera um
// UUID). Se o cliente reenviar o mesmo tx_id, o servidor trata como
// retry idempotente e devolve a decisão já registrada (se conhecida).
type TxRequest struct {
	TxID string `json:"tx_id"`
	Ops  []OpReq `json:"ops"`
}

// OpReq é uma operação de negócio (transferência/ajuste).
type OpReq struct {
	Account string `json:"account"`
	Delta   int64  `json:"delta"`
}

// TxResponse é o corpo de resposta de POST /tx (sucesso ou falha de negócio).
type TxResponse struct {
	TxID     string `json:"tx_id"`
	Decision string `json:"decision"` // COMMIT | ABORT
	LeaderID int    `json:"leader_id"`
	IsLeader bool   `json:"is_leader"`
	LatencyMs int64 `json:"latency_ms"`
	Reason   string `json:"reason,omitempty"`
}

// ErrorResponse é o envelope de erro (HTTP 4xx/5xx).
type ErrorResponse struct {
	Error string `json:"error"`
}

// HealthResponse é o corpo de GET /healthz.
type HealthResponse struct {
	NodeID      int    `json:"node_id"`
	LeaderID    int    `json:"leader_id"`
	Term        int    `json:"term"`
	IsLeader    bool   `json:"is_leader"`
	UpSinceMs   int64  `json:"up_since_ms"`
}

// PartitionRequest é o corpo de POST /admin/partition.
//
// Exemplo: {"set":[[0,1],[1,0]],"clear":[[2,3]]} → particiona as arestas
// 0→1 e 1→0, e remove a partição 2→3. Só o nó que recebe a chamada aplica
// localmente (aplicar em todos os nós é responsabilidade do script).
type PartitionRequest struct {
	Set   [][2]int `json:"set"`
	Clear [][2]int `json:"clear"`
}

// PartitionResponse devolve o estado atual da matriz de partição do nó.
type PartitionResponse struct {
	NodeID      int      `json:"node_id"`
	Partitioned [][2]int `json:"partitioned"`
}

// LeaderResponse é o corpo de GET /leader.
type LeaderResponse struct {
	NodeID int `json:"node_id"`
	LeaderID int `json:"leader_id"`
	Term   int `json:"term"`
	IsLeader bool `json:"is_leader"`
}
