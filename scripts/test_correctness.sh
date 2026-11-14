#!/usr/bin/env bash
# Valida invariantes de Safety em cenário normal (sem falhas).
set -euo pipefail
cd "$(dirname "$0")/.."

PASS=0; FAIL=0
ok()   { echo "  ✅ $*"; PASS=$((PASS+1)); }
fail() { echo "  ❌ $*"; FAIL=$((FAIL+1)); }

echo "==> [0] cluster limpo (3 nós)"
./scripts/clean.sh >/dev/null 2>&1 || true
./scripts/up.sh 3 >/dev/null

echo "==> [1] líder único e igual ao maior NODE_ID ativo"
L0=$(curl -s http://127.0.0.1:8080/leader)
L1=$(curl -s http://127.0.0.1:8081/leader)
L2=$(curl -s http://127.0.0.1:8082/leader)
NUM_LEADERS=$(echo "$L0 $L1 $L2" | jq -rs 'map(.leader_id) | unique | length')
[[ "$NUM_LEADERS" -eq 1 ]] && ok "todos concordam em 1 líder" || fail "líderes divergentes (veja $L0 $L1 $L2)"
LEADER=$(echo "$L0" | jq -r '.leader_id')
[[ "$LEADER" -eq 2 ]] && ok "líder = maior NODE_ID ativo (2)" || fail "líder deveria ser 2, veio $LEADER"

echo "==> [2] 2PC COMMIT com unanimidade"
OUT=$(go run ./cmd/client -url http://127.0.0.1:8082 -n 50 -c 4 2>/dev/null)
echo "$OUT" | grep -qE 'aborted    : 0 ' && ok "50/50 COMMIT" \
  || fail "abort inesperado: $(echo "$OUT" | grep aborted)"

echo "==> [3] 2PC ABORT com saldo insuficiente forçado"
OUT=$(go run ./cmd/client -url http://127.0.0.1:8082 -n 20 -c 4 -abort-rate 1.0 2>/dev/null)
echo "$OUT" | grep -q 'aborted    : 20' && ok "20/20 ABORT" \
  || fail "deveria abortar 100%: $(echo "$OUT" | grep aborted)"

echo "==> [4] heartbeat sem falsos positivos (rede estável)"
sleep 5
FP=$(docker compose logs --no-color node0 2>/dev/null | grep -c 'peer suspeito' || true)
[[ "$FP" -le 2 ]] && ok "falsos positivos heartbeat: $FP (aceitável ≤2 no boot)" \
  || fail "muitos falsos positivos: $FP"

echo
echo "==============================================="
echo " Resultado: PASS=$PASS  FAIL=$FAIL"
echo "==============================================="
[[ "$FAIL" -eq 0 ]] && exit 0 || exit 1
