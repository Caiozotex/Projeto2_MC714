// Package twopc implementa o protocolo Two-Phase Commit (2PC) com
// persistência via WAL, idempotência nas transições finais e protocolo
// cooperativo de recuperação para participantes presos em READY.
//
// Aplicação de negócio: transferências atômicas entre contas particionadas
// por hash(account) % N. Cada nó é dono (shard owner) de um subconjunto de
// contas; uma transação envolve um conjunto de shards, e cada shard executa
// localmente a operação sob a garantia tudo-ou-nada do 2PC.
package twopc

// Estados do coordenador (FSM).
const (
	CoordInit  = "INIT"
	CoordWait  = "WAIT"
	CoordCommit = "COMMIT"
	CoordAbort = "ABORT"
)

// Estados do participante (FSM).
const (
	PartInit   = "INIT"
	PartReady  = "READY"
	PartCommit = "COMMIT"
	PartAbort  = "ABORT"
)

// Rótulos de decisão global.
const (
	DecisionCommit = "COMMIT"
	DecisionAbort  = "ABORT"
)

// Ops é a operação de negócio carregada pela transação. Delta pode ser
// positivo ou negativo; o saldo final é verificado no VOTE_COMMIT local.
type Op struct {
	Account string  `json:"account"`
	Delta   int64   `json:"delta"`
}

// VoteRequest é a Fase 1a (coordenador → participante).
type VoteRequest struct {
	Ops  []Op `json:"ops"`
	Term int  `json:"term"`
}

// VoteResponse é a Fase 1b (participante → coordenador).
type VoteResponse struct {
	Vote   string `json:"vote"`             // COMMIT | ABORT
	Reason string `json:"reason,omitempty"` // motivo do ABORT
}

// GlobalDecision é a Fase 2a (coordenador → participante).
type GlobalDecision struct {
	Decision string `json:"decision"` // COMMIT | ABORT
	Term     int    `json:"term"`
}

// Ack é a Fase 2b (participante → coordenador).
type Ack struct {
	Applied bool `json:"applied"`
}

// StatusQuery é o protocolo cooperativo de recuperação: um nó preso em
// READY pergunta a cada par qual é o estado da transação.
type StatusQuery struct{}

// StatusReply responde com o estado do tx_id (COMMIT | ABORT | READY | INIT | UNKNOWN).
type StatusReply struct {
	State string `json:"state"`
}
