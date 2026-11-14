#!/usr/bin/env bash
# Sobe o cluster padrão (3, 5 ou 7 nós) e espera ficar healthy.
# Uso: ./scripts/up.sh [n]      (n=3|5|7, default 3)
set -euo pipefail
cd "$(dirname "$0")/.."

N="${1:-3}"

case "$N" in
  3) FILES=(-f compose.yaml) ;;
  5) FILES=(-f compose.yaml -f compose.n5.yaml) ;;
  7) FILES=(-f compose.yaml -f compose.n7.yaml) ;;
  *) echo "N inválido: $N (use 3, 5 ou 7)"; exit 1 ;;
esac

echo "==> docker compose ${FILES[*]} up -d --build"
docker compose "${FILES[@]}" up -d --build

echo "==> aguardando healthchecks..."
for i in $(seq 1 40); do
  sleep 1
  TOTAL=$(docker compose "${FILES[@]}" ps --format json 2>/dev/null | jq -rs 'length' 2>/dev/null || echo 0)
  NOT_HEALTHY=$(docker compose "${FILES[@]}" ps --format json 2>/dev/null \
    | jq -rs '[.[] | select(.Health != "healthy")] | length' 2>/dev/null || echo 1)
  if [[ "$TOTAL" -eq "$N" && "$NOT_HEALTHY" -eq 0 ]]; then
    echo "==> todos os $N nós healthy"
    docker compose "${FILES[@]}" ps
    exit 0
  fi
done

echo "!! timeout aguardando healthchecks"
docker compose "${FILES[@]}" ps
docker compose "${FILES[@]}" logs --tail=20
exit 1
