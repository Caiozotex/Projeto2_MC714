// Command client é o gerador de carga do sistema. Ele fala HTTP com o
// endpoint /tx do nó líder e mede latência por transação. Duas saídas:
//
//   - resumo em stdout (p50/p95/p99, vazão, taxa de abort) — legível;
//   - um CSV em --out (se especificado) — usado por scripts/plot_results.py.
//
// Exemplos:
//
//	# 200 transações, concorrência 8, URL do líder:
//	go run ./cmd/client -url http://127.0.0.1:18080 -n 200 -c 8 \
//	    -out experiments/results/tx.csv
//
//	# Gera apenas 30 transações de 10% de abort esperado (por conta
//	# inexistente forçada) — usado nos testes de correção.
//	go run ./cmd/client -url http://127.0.0.1:18080 -n 30 -abort-rate 0.1
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "client erro:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		url           = flag.String("url", "http://127.0.0.1:8080", "URL base do nó (sem barra)")
		nTotal        = flag.Int("n", 100, "número de transações a enviar")
		concurrency   = flag.Int("c", 8, "número de clientes concorrentes")
		abortRate     = flag.Float64("abort-rate", 0.0, "fração de transações destinadas a abortar (op=negativa grande)")
		outPath       = flag.String("out", "", "arquivo CSV de saída (opcional)")
		warmup        = flag.Int("warmup", 10, "transações de aquecimento (não medidas)")
		timeout       = flag.Duration("timeout", 5*time.Second, "timeout por requisição")
		seed          = flag.Int64("seed", 42, "semente do gerador de valores")
	)
	flag.Parse()
	if *nTotal <= 0 || *concurrency <= 0 {
		return fmt.Errorf("valores inválidos: n=%d c=%d", *nTotal, *concurrency)
	}

	httpClient := &http.Client{Timeout: *timeout}

	// 1. Descoberta de líder: bate em cada nó do cluster (opcional —
	// aceita --url única para simplificar; aqui só usamos ela).
	leaderURL := *url
	leaderID, err := whoIsLeader(httpClient, leaderURL)
	if err != nil {
		return fmt.Errorf("descobrir líder: %w", err)
	}
	fmt.Fprintf(os.Stderr, "client: líder=%d url=%s\n", leaderID, leaderURL)

	// 2. Warm-up.
	for i := 0; i < *warmup; i++ {
		_ = sendOne(httpClient, leaderURL, makeTx(makeTxID(*seed+int64(i)), false))
	}

	// 3. Carga principal com N trabalhadores.
	var (
		total    = *nTotal
		jobs     = make(chan int, total)
		results  = make(chan sample, total)
		wg       sync.WaitGroup
		aborted  int32
		failed   int32
		startT   = time.Now()
	)

	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				txID := makeTxID(*seed+int64(i))
				forcedAbort := *abortRate > 0 && float64(i%100)/100.0 < *abortRate
				req := makeTx(txID, forcedAbort)
				res, err := sendTx(httpClient, leaderURL, req)
				if err != nil {
					atomic.AddInt32(&failed, 1)
					results <- sample{TxID: txID, Err: err.Error()}
					continue
				}
				if res.Decision == "ABORT" {
					atomic.AddInt32(&aborted, 1)
				}
				results <- sample{
					TxID: txID, Decision: res.Decision,
					LatencyMs: res.LatencyMs, Aborted: res.Decision == "ABORT",
				}
			}
		}()
	}
	for i := 0; i < total; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	close(results)
	elapsed := time.Since(startT)

	// 4. Coleta resultados.
	var samples []sample
	for s := range results {
		samples = append(samples, s)
	}

	// 5. Estatísticas.
	var lats []int64
	for _, s := range samples {
		if s.Err == "" {
			lats = append(lats, s.LatencyMs)
		}
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })

	throughput := float64(len(lats)) / elapsed.Seconds()
	abortPct := 0.0
	if len(samples) > 0 {
		abortPct = 100.0 * float64(atomic.LoadInt32(&aborted)) / float64(len(samples))
	}

	fmt.Printf("== client summary ==\n")
	fmt.Printf("  total      : %d\n", len(samples))
	fmt.Printf("  success    : %d\n", len(lats))
	fmt.Printf("  failed     : %d\n", atomic.LoadInt32(&failed))
	fmt.Printf("  aborted    : %d (%.2f%%)\n", atomic.LoadInt32(&aborted), abortPct)
	fmt.Printf("  elapsed    : %s\n", elapsed)
	fmt.Printf("  throughput : %.1f tx/s\n", throughput)
	if len(lats) > 0 {
		fmt.Printf("  p50        : %d ms\n", percentile(lats, 0.50))
		fmt.Printf("  p95        : %d ms\n", percentile(lats, 0.95))
		fmt.Printf("  p99        : %d ms\n", percentile(lats, 0.99))
		fmt.Printf("  max        : %d ms\n", lats[len(lats)-1])
	}

	// 6. CSV opcional.
	if *outPath != "" {
		if err := writeCSV(*outPath, samples); err != nil {
			return fmt.Errorf("CSV: %w", err)
		}
		fmt.Fprintf(os.Stderr, "client: CSV salvo em %s\n", *outPath)
	}
	return nil
}

// -- modelos espelhando internal/api --

type txRequest struct {
	TxID string `json:"tx_id"`
	Ops  []opReq `json:"ops"`
}
type opReq struct {
	Account string `json:"account"`
	Delta   int64  `json:"delta"`
}
type txResponse struct {
	TxID      string `json:"tx_id"`
	Decision  string `json:"decision"`
	LeaderID  int    `json:"leader_id"`
	IsLeader  bool   `json:"is_leader"`
	LatencyMs int64  `json:"latency_ms"`
}
// sample representa uma transacão enviada ao cluster.
type sample struct {
	TxID      string
	Decision  string
	LatencyMs int64
	Aborted   bool
	Err       string
}

type healthResponse struct {
	NodeID   int  `json:"node_id"`
	LeaderID int  `json:"leader_id"`
	IsLeader bool `json:"is_leader"`
}

// -- helpers --

func whoIsLeader(c *http.Client, base string) (int, error) {
	resp, err := c.Get(base + "/healthz")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("/healthz status %d", resp.StatusCode)
	}
	var h healthResponse
	if err := json.NewDecoder(resp.Body).Decode(&h); err != nil {
		return 0, err
	}
	if !h.IsLeader {
		return h.LeaderID, fmt.Errorf("nó %d não é líder (líder=%d); aponte --url para o líder", h.NodeID, h.LeaderID)
	}
	return h.NodeID, nil
}

// makeTx constrói uma transferência alice→bob (ou alice com delta grande
// negativo se forcedAbort).
func makeTx(txID string, forcedAbort bool) txRequest {
	if forcedAbort {
		return txRequest{
			TxID: txID,
			Ops: []opReq{
				{Account: "alice", Delta: -10_000_000_000}, // > saldo → ABORT
			},
		}
	}
	return txRequest{
		TxID: txID,
		Ops: []opReq{
			{Account: "alice", Delta: -1},
			{Account: "bob", Delta: +1},
		},
	}
}

func sendOne(c *http.Client, base string, req txRequest) error {
	_, err := sendTx(c, base, req)
	return err
}

func sendTx(c *http.Client, base string, req txRequest) (txResponse, error) {
	body, _ := json.Marshal(req)
	httpReq, err := http.NewRequestWithContext(context.Background(),
		http.MethodPost, base+"/tx", bytes.NewReader(body))
	if err != nil {
		return txResponse{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(httpReq)
	if err != nil {
		return txResponse{}, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return txResponse{}, fmt.Errorf("http %d: %s", resp.StatusCode, string(raw))
	}
	var out txResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return txResponse{}, err
	}
	return out, nil
}

func makeTxID(seed int64) string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("tx-%016x-%s", seed, hex.EncodeToString(b[:4]))
}

func percentile(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func writeCSV(path string, samples []sample) error {
	if err := os.MkdirAll(dirOf(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	_ = w.Write([]string{"tx_id", "decision", "latency_ms", "aborted", "err"})
	for _, s := range samples {
		_ = w.Write([]string{
			s.TxID, s.Decision,
			fmt.Sprintf("%d", s.LatencyMs),
			fmt.Sprintf("%t", s.Aborted),
			s.Err,
		})
	}
	return w.Error()
}

func dirOf(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[:i]
		}
	}
	return "."
}
