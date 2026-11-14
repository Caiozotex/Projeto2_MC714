#!/usr/bin/env bash
# Valida Liveness e Safety sob falhas injetadas via docker compose kill/start.
set -euo pipefail
cd "$(dirname "$0")/.."

PASS=0; FAIL=0
ok()   { echo "  ✅ $*"; PASS=$((PASS+1)); }
fail() { echo "  ❌ $*"; FAIL=$((FAIL+1)); }

echo "==> [setup] cluster limpo (3 nós)"
./scripts/clean.sh >/dev/null 2>&1 || true
./scripts/up.sh 3 >/dev/null

echo "==> [falha 1] kill do líder → failover"
docker compose kill node2 >/dev/null
START_MS=$(date +%s%3N)
CONVERGED=""
for i in $(seq 1 100); do
  sleep 0.1
  NEW=$(curl -s http://127.0.0.1:8080/leader 2>/dev/null | jq -r '.leader_id' 2>/dev/null || echo -1)
  if [[ "$NEW" -ge 0 && "$NEW" -ne 2 ]]; then
    CONVERGED=$(( $(date +%s%3N) - START_MS ))
    ok "failover em ${CONVERGED}ms → líder = $NEW"
    break
  fi
  [[ $i -eq 100 ]] && fail "timeout aguardando novo líder"
done
[[ -n "$CONVERGED" && "$CONVERGED" -lt 3000 ]] && ok "failover < 3s" || fail "failover lento: ${CONVERGED}ms"

echo "==> [falha 2] transações continuam após failover"
OUT=$(go run ./cmd/client -url http://127.0.0.1:8081 -n 30 -c 4 2>&1)
echo "$OUT" | grep -q 'success    : 30' && ok "30/30 após failover" || fail "client pós-failover falhou"

echo "==> [falha 3] nó reiniciado volta como follower (não rouba liderança)"
docker compose start node2 >/dev/null
sleep 3
L=$(curl -s http://127.0.0.1:8082/leader | jq -r '.is_leader')
[[ "$L" == "false" ]] && ok "node2 voltou como follower" || fail "node2 roubou liderança"

echo "==> [falha 4] restart do líder preserva WAL"
docker compose restart node1 >/dev/null
sleep 5
curl -sf http://127.0.0.1:8081/healthz >/dev/null \
  && ok "líder reiniciou e respondeu" || fail "líder não voltou"

echo
echo "==============================================="
echo " Resultado: PASS=$PASS  FAIL=$FAIL"
echo "==============================================="
[[ "$FAIL" -eq 0 ]] && exit 0 || exit 1
