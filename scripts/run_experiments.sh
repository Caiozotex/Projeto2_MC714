#!/usr/bin/env bash
# Bateria experimental completa (3 algoritmos × ≥3 níveis × N reps).
# Duração típica: ~40-60min com REPS=3. Ajuste REPS=5 para bateria completa.
set -euo pipefail
cd "$(dirname "$0")/.."

RESULTS="experiments/results"
REPS="${REPS:-3}"
mkdir -p "$RESULTS"

# 0. Metadados do ambiente
{
  echo "== Environment Info =="
  echo "date:      $(date -Iseconds)"
  echo "hostname:  $(hostname)"
  echo "cpu:       $(lscpu 2>/dev/null | grep 'Model name' | head -1 | cut -d: -f2 | xargs)"
  echo "cores:     $(nproc)"
  echo "mem_total: $(free -h | awk '/Mem:/{print $2}')"
  echo "kernel:    $(uname -r)"
  echo "go:        $(go version)"
  echo "docker:    $(docker --version)"
  echo "compose:   $(docker compose version --short)"
} > "$RESULTS/env_info.txt"
cat "$RESULTS/env_info.txt"

wait_healthy() {
  local files=("$@")
  local n=40
  for i in $(seq 1 "$n"); do
    sleep 1
    local not_healthy
    not_healthy=$(docker compose "${files[@]}" ps --format json 2>/dev/null \
      | jq -rs '[.[] | select(.Health != "healthy")] | length' 2>/dev/null || echo 99)
    if [[ "$not_healthy" == "0" ]]; then
      return 0
    fi
  done
  echo "  !! wait_healthy timeout (${n}s). Estado atual:"
  docker compose "${files[@]}" ps
  echo "  !! Logs dos últimos 5 containers:"
  docker compose "${files[@]}" logs --tail=10 2>&1 | tail -50
  return 1
}

find_leader_port() {
  local n=$1
  for p in $(seq 8080 $((8080+n-1))); do
    if curl -sf "http://127.0.0.1:$p/leader" 2>/dev/null | jq -e '.is_leader == true' >/dev/null 2>&1; then
      echo "$p"; return
    fi
  done
  echo ""
}

# ==== 2PC: variando N ∈ {3,5,7} ====
run_2pc_N() {
  local n=$1 out=$2
  case "$n" in
    3) FILES=(-f compose.yaml) ;;
    5) FILES=(-f compose.yaml -f compose.n5.yaml) ;;
    7) FILES=(-f compose.yaml -f compose.n7.yaml) ;;
  esac
  echo "n,tx_id,decision,latency_ms" > "$out"
  for r in $(seq 1 "$REPS"); do
    docker compose "${FILES[@]}" down -v >/dev/null 2>&1 || true
    docker compose "${FILES[@]}" up -d --build >/dev/null
    if ! wait_healthy "${FILES[@]}"; then
      echo "  !! rep=$r N=$n falhou — pulando"
      continue
    fi
    LEADER_PORT=$(find_leader_port "$n")
    if [[ -z "$LEADER_PORT" ]]; then
      echo "  !! sem líder N=$n rep=$r"
      continue
    fi
    go run ./cmd/client -url "http://127.0.0.1:$LEADER_PORT" -n 20 -c 4 -warmup 5 >/dev/null 2>&1 || true
    TMP="$RESULTS/.tmp_${n}_${r}.csv"
    go run ./cmd/client -url "http://127.0.0.1:$LEADER_PORT" -n 200 -c 8 -warmup 0 -out "$TMP" >/dev/null
    local commits
    commits=$(tail -n +2 "$TMP" | awk -F',' '$2=="COMMIT"' | wc -l)
    local total
    total=$(($(wc -l < "$TMP")-1))
    echo "  -> rep=$r N=$n: $commits/$total COMMIT (esperado 200/200)"
    tail -n +2 "$TMP" | awk -v n="$n" -F',' '{print n","$1","$2","$3}' >> "$out"
    rm -f "$TMP"
  done
  echo "  -> $out ($(($(wc -l < "$out")-1)) amostras)"
}

echo "==> [2PC] N ∈ {3,5,7}"
for n in 3 5 7; do run_2pc_N "$n" "$RESULTS/twopc_N${n}.csv"; done

# ==== Bully: N ∈ {3,5,7} (tempo de convergência pós-kill) ====
echo "==> [Bully] N ∈ {3,5,7}"
for n in 3 5 7; do
  case "$n" in
    3) FILES=(-f compose.yaml) ;;
    5) FILES=(-f compose.yaml -f compose.n5.yaml) ;;
    7) FILES=(-f compose.yaml -f compose.n7.yaml) ;;
  esac
  OUT="$RESULTS/bully_N${n}.csv"
  echo "n,rep,converge_ms,old_leader" > "$OUT"
  for r in $(seq 1 "$REPS"); do
    docker compose "${FILES[@]}" down -v >/dev/null 2>&1 || true
    docker compose "${FILES[@]}" up -d --build >/dev/null
    wait_healthy "${FILES[@]}" || continue
    OLD=$(find_leader_port "$n" | head -1)
    OLD_ID=$(curl -s "http://127.0.0.1:$OLD/leader" | jq -r '.leader_id')
    T0=$(date +%s%3N)
    docker compose "${FILES[@]}" kill "node${OLD_ID}" >/dev/null
    CONV=""
    for i in $(seq 1 50); do
      sleep 0.1
      for p in $(seq 8080 $((8080+n-1))); do
        [[ "$p" -eq "$OLD" ]] && continue
        IS=$(curl -sf "http://127.0.0.1:$p/leader" 2>/dev/null | jq -r '.is_leader' 2>/dev/null || echo "")
        if [[ "$IS" == "true" ]]; then CONV=$(( $(date +%s%3N) - T0 )); break 2; fi
      done
    done
    echo "$n,$r,${CONV:-9999},$OLD_ID" >> "$OUT"
  done
  echo "  -> $OUT"
done

# ==== Heartbeat: FP sob jitter ====
echo "==> [Heartbeat] jitter ∈ {0,50,150,300}ms"
OUT="$RESULTS/heartbeat_jitter.csv"
echo "jitter_ms,rep,fp_count" > "$OUT"
for j in 0 50 150 300; do
  for r in $(seq 1 "$REPS"); do
    NET_DELAY_MS="$j" NET_JITTER_MS="$j" docker compose down -v >/dev/null 2>&1 || true
    NET_DELAY_MS="$j" NET_JITTER_MS="$j" docker compose up -d --build >/dev/null
    wait_healthy -f compose.yaml || true
    sleep 5
    FP=$(docker compose logs --no-color node0 2>/dev/null | grep -c 'peer suspeito' || true)
    echo "$j,$r,$FP" >> "$OUT"
  done
done
echo "  -> $OUT"

docker compose down -v >/dev/null 2>&1 || true

echo
echo "==> Coleta concluída em $RESULTS/"
ls -1 "$RESULTS/"
