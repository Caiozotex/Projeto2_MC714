#!/usr/bin/env bash
# Encerra todos os clusters (3, 5, 7), remove volumes e artefatos temporários.
set -euo pipefail
cd "$(dirname "$0")/.."

for f in "compose.yaml" "compose.yaml compose.n5.yaml" "compose.yaml compose.n7.yaml"; do
  echo "==> docker compose -f $f down -v (tolerante a erros)"
  docker compose -f $f down -v --remove-orphans 2>/dev/null || true
done

echo "==> limpando artefatos locais"
rm -f experiments/results/*.csv experiments/results/*.json experiments/results/*.png \
      experiments/results/env_info.txt \
      docs/relatorio.aux docs/relatorio.log docs/relatorio.out docs/relatorio.pdf

echo "==> volumes mc714 remanescentes:"
docker volume ls | grep mc714 || echo "  (nenhum)"
