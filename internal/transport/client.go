package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"
)

// Client mantém um pool 1-conexão-por-peer. Cada conexão é serializada por
// mutex (uma request-response por vez), o que basta para N ≤ 7 e evita
// correlação de req_id no protocolo.
type Client struct {
	selfID int
	peers  map[int]string
	fault  FaultConfig

	mu   sync.Mutex
	pool map[int]*connEntry
}

type connEntry struct {
	mu   sync.Mutex
	conn net.Conn
}

// NewClient cria o cliente apontando para os peers estáticos.
func NewClient(selfID int, peers map[int]string, fault FaultConfig) *Client {
	return &Client{
		selfID: selfID,
		peers:  peers,
		fault:  fault,
		pool:   make(map[int]*connEntry),
	}
}

// SetPartition permite atualizar em runtime a função de partição.
func (c *Client) SetPartition(p func(src, dst int) bool) {
	c.mu.Lock()
	c.fault.Partition = p
	c.mu.Unlock()
}

// Call executa uma request-response síncrona respeitando o deadline do ctx.
// Toda chamada define SetDeadline na conn antes de escrever/ler.
func (c *Client) Call(ctx context.Context, dst int, env *Envelope) (*Envelope, error) {
	if env.Src == 0 && c.selfID != 0 {
		env.Src = c.selfID
	}
	env.Dst = dst
	if env.V == 0 { env.V = protocolVersion }
	if env.SentAtMs == 0 { env.SentAtMs = nowMillis() }

	entry := c.acquire(dst)
	entry.mu.Lock()
	defer entry.mu.Unlock()

	if entry.conn == nil {
		conn, err := c.dial(dst)
		if err != nil { return nil, err }
		entry.conn = conn
	}

	if dl, ok := ctx.Deadline(); ok {
		_ = entry.conn.SetDeadline(dl)
	} else {
		_ = entry.conn.SetDeadline(time.Now().Add(10 * time.Second))
	}

	out, err := json.Marshal(env)
	if err != nil { return nil, err }
	if err := WriteFrame(entry.conn, out); err != nil {
		c.drop(dst, entry)
		return nil, fmt.Errorf("write frame: %w", err)
	}
	in, err := ReadFrame(entry.conn)
	if err != nil {
		c.drop(dst, entry)
		return nil, fmt.Errorf("read frame: %w", err)
	}
	var resp Envelope
	if err := json.Unmarshal(in, &resp); err != nil { return nil, err }
	return &resp, nil
}

// Send dispara uma mensagem one-way (fire-and-forget). O servidor identifica
// one-way quando o handler devolve resposta nil. Para o cliente, ele apenas
// escreve o frame e não espera retorno — usa a mesma conn do pool.
func (c *Client) Send(ctx context.Context, dst int, env *Envelope) error {
	if env.Src == 0 && c.selfID != 0 { env.Src = c.selfID }
	env.Dst = dst
	if env.V == 0 { env.V = protocolVersion }
	if env.SentAtMs == 0 { env.SentAtMs = nowMillis() }

	entry := c.acquire(dst)
	entry.mu.Lock()
	defer entry.mu.Unlock()

	if entry.conn == nil {
		conn, err := c.dial(dst)
		if err != nil { return err }
		entry.conn = conn
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = entry.conn.SetDeadline(dl)
	} else {
		_ = entry.conn.SetDeadline(time.Now().Add(5 * time.Second))
	}
	out, err := json.Marshal(env)
	if err != nil { return err }
	if err := WriteFrame(entry.conn, out); err != nil {
		c.drop(dst, entry)
		return err
	}
	return nil
}

// Close fecha todas as conexões do pool.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.pool {
		e.mu.Lock()
		if e.conn != nil { _ = e.conn.Close() }
		e.conn = nil
		e.mu.Unlock()
	}
}

func (c *Client) acquire(dst int) *connEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.pool[dst]; ok { return e }
	e := &connEntry{}
	c.pool[dst] = e
	return e
}

func (c *Client) drop(dst int, e *connEntry) {
	if e.conn != nil { _ = e.conn.Close() }
	e.conn = nil
}

func (c *Client) dial(dst int) (net.Conn, error) {
	addr, ok := c.peers[dst]
	if !ok { return nil, fmt.Errorf("peer desconhecido: %d", dst) }
	d := net.Dialer{Timeout: 3 * time.Second}
	raw, err := d.Dial("tcp", addr)
	if err != nil { return nil, err }
	return NewFaultConn(raw, c.selfID, dst, c.fault), nil
}
