package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"
)

// Handler recebe um envelope e devolve:
//   - (resp != nil, nil)  -> request/response: escreve resp de volta.
//   - (nil, nil)          -> one-way: nada é escrito.
//   - (nil, err)          -> erro: apenas loga.
type Handler func(ctx context.Context, env *Envelope) (*Envelope, error)

// Server escuta TCP e despacha envelopes para o Handler.
type Server struct {
	selfID  int
	addr    string
	handler Handler
	ln      net.Listener
}

// NewServer cria o servidor (não escuta ainda).
func NewServer(selfID int, addr string, h Handler) *Server {
	return &Server{selfID: selfID, addr: addr, handler: h}
}

// ListenAndServe aceita conexões e spawna uma goroutine por conexão.
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil { return fmt.Errorf("listen %s: %w", s.addr, err) }
	s.ln = ln
	slog.Info("transport: escutando", "node_id", s.selfID, "addr", s.addr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) { return nil }
			return err
		}
		go s.serveConn(conn)
	}
}

// Close encerra o listener.
func (s *Server) Close() error {
	if s.ln != nil { return s.ln.Close() }
	return nil
}

func (s *Server) serveConn(conn net.Conn) {
	defer conn.Close()
	for {
		_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		raw, err := ReadFrame(conn)
		if err != nil { return }

		var env Envelope
		if err := json.Unmarshal(raw, &env); err != nil {
			slog.Warn("transport: envelope inválido", "err", err, "remote", conn.RemoteAddr())
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := s.handler(ctx, &env)
		cancel()
		if err != nil {
			slog.Warn("transport: handler erro",
				"kind", env.Kind, "src", env.Src, "err", err)
			continue
		}
		if resp == nil { continue }
		out, err := json.Marshal(resp)
		if err != nil { continue }
		_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if err := WriteFrame(conn, out); err != nil { return }
	}
}
