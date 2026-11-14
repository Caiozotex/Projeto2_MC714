package transport

import (
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"
)

// FaultConfig parametriza a injeção reproduzível de falhas de rede.
// Toda aleatoriedade é derivada de Seed (+ offsets estáveis por par),
// garantindo que a mesma execução com o mesmo FAULT_SEED produza a
// mesma sequência de drops/jitters.
type FaultConfig struct {
	DelayMs  int
	JitterMs int
	DropRate float64
	// Partition retorna true se a aresta (src -> dst) está particionada.
	// Nil == sem partição. Pode ser trocada em runtime via painel /admin.
	Partition func(src, dst int) bool
	Seed      int64
}

// FaultConn envolve um net.Conn aplicando atraso, jitter, descarte e
// partição sobre as escritas. Leituras seguem inalteradas (a latência é
// cobrada do lado do emissor, como em uma rede real fim-a-fim).
type FaultConn struct {
	net.Conn
	cfg FaultConfig
	rng *rand.Rand
	mu  sync.Mutex
	src int
	dst int
}

// NewFaultConn constrói um FaultConn com RNG determinístico por aresta.
func NewFaultConn(raw net.Conn, src, dst int, cfg FaultConfig) *FaultConn {
	seed := cfg.Seed + int64(src)*1_000_003 + int64(dst)
	return &FaultConn{
		Conn: raw,
		cfg:  cfg,
		rng:  rand.New(rand.NewSource(seed)),
		src:  src,
		dst:  dst,
	}
}

func (fc *FaultConn) Write(b []byte) (int, error) {
	fc.mu.Lock()
	partitioned := fc.cfg.Partition != nil && fc.cfg.Partition(fc.src, fc.dst)
	dropped := fc.cfg.DropRate > 0 && fc.rng.Float64() < fc.cfg.DropRate
	delay := time.Duration(0)
	if fc.cfg.DelayMs > 0 || fc.cfg.JitterMs > 0 {
		j := 0
		if fc.cfg.JitterMs > 0 {
			j = fc.rng.Intn(2*fc.cfg.JitterMs+1) - fc.cfg.JitterMs
		}
		total := fc.cfg.DelayMs + j
		if total < 0 { total = 0 }
		delay = time.Duration(total) * time.Millisecond
	}
	fc.mu.Unlock()

	if partitioned {
		return 0, fmt.Errorf("fault: aresta %d->%d particionada", fc.src, fc.dst)
	}
	if dropped {
		return 0, fmt.Errorf("fault: aresta %d->%d dropou mensagem", fc.src, fc.dst)
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	return fc.Conn.Write(b)
}
