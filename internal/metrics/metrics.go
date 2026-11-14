package metrics

import (
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"time"
)

// Registry guarda contadores e histogramas por nome.
type Registry struct {
	mu         sync.Mutex
	counters   map[string]int64
	histograms map[string]*Histogram
	nodeID     int
}

// NewRegistry cria o registry para o nó.
func NewRegistry(nodeID int) *Registry {
	return &Registry{
		counters:   make(map[string]int64),
		histograms: make(map[string]*Histogram),
		nodeID:     nodeID,
	}
}

// Inc incrementa um contador.
func (r *Registry) Inc(name string) { r.Add(name, 1) }

// Add soma `v` a um contador.
func (r *Registry) Add(name string, v int64) {
	r.mu.Lock()
	r.counters[name] += v
	r.mu.Unlock()
}

// GetCounter devolve o valor atual de um contador.
func (r *Registry) GetCounter(name string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counters[name]
}

// Observe registra uma amostra em um histograma (cria se preciso).
func (r *Registry) Observe(name string, v float64) {
	r.mu.Lock()
	h, ok := r.histograms[name]
	if !ok {
		h = &Histogram{maxSize: 100_000}
		r.histograms[name] = h
	}
	r.mu.Unlock()
	h.Observe(v)
}

// Snapshot devolve um retrato JSON-serializável.
func (r *Registry) Snapshot() map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	counters := make(map[string]int64, len(r.counters))
	for k, v := range r.counters { counters[k] = v }
	hists := make(map[string]any, len(r.histograms))
	for k, h := range r.histograms {
		hists[k] = h.Stats()
	}
	return map[string]any{
		"node_id":    r.nodeID,
		"ts_ms":      time.Now().UnixNano() / int64(time.Millisecond),
		"counters":   counters,
		"histograms": hists,
	}
}

// HTTPHandler expõe /metrics (JSON) para scrapers externos.
func (r *Registry) HTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(r.Snapshot())
	})
}

// Histogram é um acumulador com percentis on-demand.
type Histogram struct {
	mu      sync.Mutex
	samples []float64
	maxSize int
}

// Observe adiciona uma amostra (truncando FIFO ao exceder maxSize).
func (h *Histogram) Observe(v float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.maxSize > 0 && len(h.samples) >= h.maxSize {
		copy(h.samples, h.samples[1:])
		h.samples[len(h.samples)-1] = v
		return
	}
	h.samples = append(h.samples, v)
}

// Stats devolve count/mean/min/max/p50/p95/p99 (ms).
func (h *Histogram) Stats() map[string]float64 {
	h.mu.Lock()
	cp := make([]float64, len(h.samples))
	copy(cp, h.samples)
	h.mu.Unlock()

	sort.Float64s(cp)
	out := map[string]float64{"count": float64(len(cp))}
	if len(cp) == 0 { return out }
	sum := 0.0
	for _, v := range cp { sum += v }
	out["mean"] = sum / float64(len(cp))
	out["min"] = cp[0]
	out["max"] = cp[len(cp)-1]
	out["p50"] = percentile(cp, 0.50)
	out["p95"] = percentile(cp, 0.95)
	out["p99"] = percentile(cp, 0.99)
	return out
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 { return 0 }
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}
