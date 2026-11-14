package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config agrega toda a configuração externa do nó, lida exclusivamente de
// variáveis de ambiente (requisito do trabalho: mesma imagem, diferenciação
// apenas por env vars no compose.yaml).
type Config struct {
	NodeID              int
	ListenAddr          string            // porta interna entre nós (:8000)
	HTTPAddr            string            // porta externa p/ clientes (:8080)
	Peers               map[int]string    // peerID -> host:porta (interna)
	HeartbeatIntervalMs int
	HeartbeatTimeoutMs  int
	ElectionTimeoutMs   int
	TxTimeoutMs         int
	WalDir              string
	FaultSeed           int64
	NetDelayMs          int
	NetJitterMs         int
	NetDropRate         float64
}

// Load lê e valida todas as env vars obrigatórias.
func Load() (*Config, error) {
	c := &Config{}

	idStr, err := mustEnv("NODE_ID")
	if err != nil { return nil, err }
	id, err := strconv.Atoi(idStr)
	if err != nil || id < 0 { return nil, fmt.Errorf("NODE_ID inválido: %q", idStr) }
	c.NodeID = id

	c.ListenAddr = getEnvDefault("LISTEN_ADDR", ":8000")
	c.HTTPAddr = getEnvDefault("HTTP_ADDR", ":8080")

	peersStr, err := mustEnv("PEERS")
	if err != nil { return nil, err }
	peers, err := parsePeers(peersStr)
	if err != nil { return nil, err }
	c.Peers = peers
	if _, ok := c.Peers[c.NodeID]; !ok {
		return nil, fmt.Errorf("PEERS não contém este NODE_ID=%d", c.NodeID)
	}

	if c.HeartbeatIntervalMs, err = envInt("HEARTBEAT_INTERVAL_MS", 200); err != nil { return nil, err }
	if c.HeartbeatTimeoutMs, err = envInt("HEARTBEAT_TIMEOUT_MS", 800); err != nil { return nil, err }
	if c.ElectionTimeoutMs, err = envInt("ELECTION_TIMEOUT_MS", 600); err != nil { return nil, err }
	if c.TxTimeoutMs, err = envInt("TX_TIMEOUT_MS", 1500); err != nil { return nil, err }
	if c.NetDelayMs, err = envInt("NET_DELAY_MS", 0); err != nil { return nil, err }
	if c.NetJitterMs, err = envInt("NET_JITTER_MS", 0); err != nil { return nil, err }

	if c.WalDir = getEnvDefault("WAL_DIR", "/data/wal"); c.WalDir == "" {
		return nil, fmt.Errorf("WAL_DIR vazio")
	}

	seedStr := getEnvDefault("FAULT_SEED", "42")
	c.FaultSeed, err = strconv.ParseInt(seedStr, 10, 64)
	if err != nil { return nil, fmt.Errorf("FAULT_SEED inválido: %q", seedStr) }

	dropStr := getEnvDefault("NET_DROP_RATE", "0.0")
	c.NetDropRate, err = strconv.ParseFloat(dropStr, 64)
	if err != nil { return nil, fmt.Errorf("NET_DROP_RATE inválido: %q", dropStr) }
	if c.NetDropRate < 0 || c.NetDropRate > 1 {
		return nil, fmt.Errorf("NET_DROP_RATE fora de [0,1]: %f", c.NetDropRate)
	}

	return c, nil
}

// PeerIDs retorna os IDs dos outros nós (exclui o próprio).
func (c *Config) PeerIDs() []int {
	out := make([]int, 0, len(c.Peers)-1)
	for id := range c.Peers {
		if id != c.NodeID { out = append(out, id) }
	}
	return out
}

func mustEnv(k string) (string, error) {
	v := os.Getenv(k)
	if v == "" { return "", fmt.Errorf("env var obrigatória ausente: %s", k) }
	return v, nil
}

func getEnvDefault(k, def string) string {
	if v := os.Getenv(k); v != "" { return v }
	return def
}

func envInt(k string, def int) (int, error) {
	v := getEnvDefault(k, "")
	if v == "" { return def, nil }
	n, err := strconv.Atoi(v)
	if err != nil { return 0, fmt.Errorf("%s inválido: %q", k, v) }
	return n, nil
}

// parsePeers aceita "0=node0:8000,1=node1:8000,..."
func parsePeers(s string) (map[int]string, error) {
	out := make(map[int]string)
	for _, chunk := range strings.Split(s, ",") {
		chunk = strings.TrimSpace(chunk)
		if chunk == "" { continue }
		eq := strings.IndexByte(chunk, '=')
		if eq <= 0 { return nil, fmt.Errorf("peer mal formado: %q", chunk) }
		idStr := chunk[:eq]
		addr := chunk[eq+1:]
		id, err := strconv.Atoi(idStr)
		if err != nil || id < 0 { return nil, fmt.Errorf("peer ID inválido: %q", idStr) }
		if addr == "" { return nil, fmt.Errorf("peer %d sem endereço", id) }
		if _, dup := out[id]; dup { return nil, fmt.Errorf("peer duplicado: %d", id) }
		out[id] = addr
	}
	if len(out) < 3 {
		return nil, fmt.Errorf("PEERS requer >= 3 nós (recebido %d)", len(out))
	}
	return out, nil
}
