# Serviço de Transações Distribuídas com Failover Automático (Bully + 2PC + Heartbeat)

> **MC714 — Sistemas Distribuídos (2º Semestre de 2026)**  
> Instituto de Computação — Universidade Estadual de Campinas (IC/UNICAMP)

Implementação em **Go** e orquestração via **Docker Compose v2** de um sistema distribuído tolerante a falhas que integra três algoritmos clássicos desenvolvidos do zero para resolver um problema prático coerente de processamento transacional distribuído com recuperação automática de coordenador.

---

## Equipe

- **Integrante 1:** Caio Raphael Teixeira — RA: 212661
- **Integrante 2:** Vitor Takahashi Miranda — RA: 231740

---

## Visão Geral e Arquitetura

O sistema implementa um **processador de transações distribuídas sobre dados particionados** com no mínimo **3 nós lógicos independentes** (escalável para 5, 7 ou mais nós via configuração). Todos os algoritmos foram implementados do zero, sem uso de bibliotecas que já implementem eleição, consenso ou detecção de falhas.

### Algoritmos Integrados

1. **Eleição de Líder — Algoritmo do Valentão (Bully)** *(Tema 1)*  
   Elege de forma determinística o nó ativo de maior identificador (`NODE_ID`) como coordenador do sistema utilizando mensagens `ELECTION`, `OK` e `COORDINATOR`.
2. **Compromisso Atômico — Two-Phase Commit (2PC)** *(Tema 2)*  
   Garante atomicidade ("tudo ou nada") nas transações distribuídas entre coordenador (`INIT -> WAIT -> COMMIT | ABORT`) e participantes (`INIT -> READY -> COMMIT | ABORT`), com persistência em **Write-Ahead Log (WAL)** local, operações idempotentes e **protocolo cooperativo de recuperação** entre pares.
3. **Detector de Falhas via Heartbeat com Timeout Adaptativo** *(Bônus: 3º algoritmo + integração prática)*  
   Monitora continuamente a vivacidade dos pares com mensagens periódicas de batimento e ajusta dinamicamente `timeout[peer]` ao detectar falsos positivos causados por atraso ou *jitter* de rede, disparando novas eleições Bully quando o coordenador atual se torna suspeito.

### Fluxo de Integração

```text
+-------------------------------------------------------------------+
|                         Nó Lógico (Go)                            |
|                                                                   |
|  [ Heartbeat Detector ] --(suspeita do líder)--> [ Bully ]        |
|           |                                          |            |
|   timeout adaptativo                        define novo líder     |
|           |                                          v            |
|           +-------------------------------> [ Módulo 2PC ]        |
|                                                |       |          |
|  [ Rede Docker / RPC com deadlines ] <---------+       +-> [WAL]  |
|  (atraso, jitter, perda e partição com seed)                      |
+-------------------------------------------------------------------+
```

> **Modelo Formal e Limitações:** As hipóteses temporais, propriedades dos canais, modelos de falha (`crash-stop`, `crash-recovery`, omissão/partição), propriedades formais de *Safety* e *Liveness* e a discussão sobre execução de múltiplos nós lógicos em uma única máquina física estão detalhadas em [`MODEL.md`](MODEL.md).

---

## Estrutura do Repositório

```text
.
├── cmd/
│   ├── node/                 # Ponto de entrada do nó distribuído (servidor + algoritmos)
│   └── client/               # Cliente gerador de carga e transações 2PC
├── internal/
│   ├── bully/                # Implementação do algoritmo de eleição Bully
│   ├── heartbeat/            # Detector de falhas baseado em heartbeat com timeout adaptativo
│   ├── twopc/                # Máquinas de estado 2PC (Coordenador/Participante) e recuperação
│   ├── storage/              # Persistência em Write-Ahead Log (WAL) e estado local
│   ├── transport/            # Comunicação em rede (timeouts/deadlines) e injetor de falhas
│   └── metrics/              # Coleta de latências, contadores de mensagens e logs JSON
├── scripts/
│   ├── up.sh                 # Sobe o cluster padrão via Docker Compose
│   ├── test_correctness.sh   # Valida invariantes em cenário normal nos logs
│   ├── test_faults.sh        # Injeta falhas (kill, atraso, perda, partição) e valida recuperação
│   ├── run_experiments.sh    # Executa toda a suíte experimental automatizada
│   ├── plot_results.py       # Gera gráficos (média, IC e p50/p95/p99) a partir dos CSVs
│   └── clean.sh              # Encerra contêineres e limpa volumes/artefatos temporários
├── experiments/
│   └── results/              # Dados brutos (.csv/.json), env_info.txt e gráficos gerados
├── docs/
│   └── relatorio.pdf         # Cópia do relatório final em formato IEEE (máx. 6 páginas)
├── Dockerfile                # Build multi-stage da imagem Go única para todos os nós
├── compose.yaml              # Orquestração Docker Compose v2 (3+ nós independentes)
├── go.mod                    # Dependências Go (comunicação/serialização/testes)
├── MODEL.md                  # Especificação formal do modelo de sistema e correção
└── README.md                 # Este arquivo
```

---

## Pré-requisitos

- **Docker Engine** e **Docker Compose v2** (`docker compose`)
- **Go** `1.22+` *(opcional no host para executar testes unitários locais com `go test`)*
- **Python 3** com `matplotlib` / `pandas` *(opcional para gerar gráficos dos experimentos)*
- **Bash**, `jq` e `curl` *(para os scripts de automação e verificação)*

Verifique as versões instaladas no ambiente:

```bash
docker --version
docker compose version
go version
```

---

## Configuração Externa (Variáveis de Ambiente)

Todos os contêineres executam a **mesma imagem Docker**, diferenciados exclusivamente por variáveis de ambiente no `compose.yaml`, permitindo portabilidade direta entre a execução local e a nuvem privada do IC sem alterações de código:

| Variável | Descrição | Exemplo |
| :--- | :--- | :--- |
| `NODE_ID` | Identificador numérico único do nó (`0..N-1`) | `2` |
| `LISTEN_ADDR` | Endereço e porta de escuta na rede | `:8000` |
| `PEERS` | Lista de pares (`id=host:porta`) separados por vírgula | `0=node0:8000,1=node1:8000` |
| `HEARTBEAT_INTERVAL_MS` | Intervalo de envio de heartbeats (ms) | `200` |
| `HEARTBEAT_TIMEOUT_MS` | Timeout inicial para suspeita de falha (ms) | `800` |
| `ELECTION_TIMEOUT_MS` | Prazo para resposta `OK` no Bully (ms) | `600` |
| `TX_TIMEOUT_MS` | Deadline/timeout para fases do 2PC e chamadas RPC (ms) | `1500` |
| `WAL_DIR` | Diretório persistente do Write-Ahead Log | `/data/wal` |
| `FAULT_SEED` | Semente fixa para reprodutibilidade da injeção de rede | `42` |
| `NET_DELAY_MS` | Atraso artificial médio de rede (ms) | `0` |
| `NET_JITTER_MS` | Variação (jitter) do atraso de rede (ms) | `0` |
| `NET_DROP_RATE` | Taxa probabilística de descarte de mensagens (`0.0` a `1.0`) | `0.0` |

*(Ajuste os nomes exatos caso utilize flags ou variáveis adicionais definidas na implementação.)*

---

## Como Construir, Iniciar, Testar e Remover o Ambiente

### 1. Construir e Iniciar o Sistema (Ambiente de Referência)

```bash
# Construir as imagens e iniciar os nós em background (Docker Compose v2)
docker compose build
docker compose up -d

# Acompanhar os logs estruturados em tempo real
docker compose logs -f
```

*(Ou execute diretamente o script auxiliar `./scripts/up.sh`, se preferir.)*

### 2. Executar Testes de Correção e Cenários de Falha

Os testes unitários verificam as máquinas de estados e regras locais, enquanto os scripts de integração validam as propriedades de *Safety* e *Liveness* diretamente nos logs estruturados gerados pelos contêineres:

```bash
# Testes unitários e de módulos em Go
go test -v ./...

# Validação end-to-end em cenário normal (sem falhas)
./scripts/test_correctness.sh

# Validação em cenários de falha:
# - Queda abrupta do coordenador (docker compose kill) -> Heartbeat -> Bully
# - Queda no meio do 2PC e recuperação via WAL após reinício
# - Perda/atraso de mensagens e partição lógica de rede
./scripts/test_faults.sh
```

### 3. Remover Todo o Ambiente

```bash
# Encerrar os contêineres e remover redes e volumes (incluindo WALs antigos)
docker compose down -v
```

---

## Reprodução dos Experimentos e Métricas

Os experimentos automatizados variam parâmetros em **3 ou mais níveis**, executam **múltiplas repetições independentes** com sementes registradas (descartando período de aquecimento) e salvam todos os resultados brutos na pasta `experiments/results/`, junto com os metadados do ambiente em `experiments/results/env_info.txt` (CPU, memória, SO, versões do Go e Docker).

### Executar Bateria Experimental e Gerar Gráficos

```bash
# Executa os ensaios para Heartbeat, Bully e 2PC e salva os CSVs/JSONs brutos
./scripts/run_experiments.sh

# Gera os gráficos de comparação e percentis (p50, p95, p99)
python3 scripts/plot_results.py
```

### Resumo dos Cenários Avaliados

| Algoritmo | Parâmetros Variados (3+ níveis) | Métricas Coletadas |
| :--- | :--- | :--- |
| **Heartbeat** | Atraso/jitter de rede (`0ms`, `50ms`, `150ms`, `300ms`) | Tempo de detecção de falha (ms), taxa de falsos positivos (%), overhead de mensagens/s |
| **Bully** | Número de nós (`N ∈ {3, 5, 7}`) e nó iniciador | Tempo de convergência da eleição (ms), total de mensagens (`ELECTION`, `OK`, `COORDINATOR`) |
| **2PC** | Número de nós (`N ∈ {3, 5, 7}`) e atraso de rede (`0ms`, `50ms`, `150ms`) | Latência de transação (**p50, p95, p99**), vazão (tx/s), taxa de abort |

---

## Citação de Código Reutilizado e Referências

- **Referência Teórica Principal:**  
  [1] M. van Steen and A. S. Tanenbaum, *Distributed Systems*, 4th ed., distributed-systems.net, 2023.
- **Referência Complementar:**  
  [2] A. D. Kshemkalyani and M. Singhal, *Distributed Computing: Principles, Algorithms, and Systems*, Cambridge University Press, 2008.
- **Bibliotecas e Ferramentas de Terceiros:**  
