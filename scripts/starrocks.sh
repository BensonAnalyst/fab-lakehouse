#!/usr/bin/env bash
# Open a StarRocks SQL prompt, or run a SQL file:
#   ./scripts/starrocks.sh                                   interactive prompt
#   ./scripts/starrocks.sh sql/starrocks/01_create_catalog.sql
set -euo pipefail
if [ $# -eq 0 ]; then
  docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot --prompt="StarRocks > "
else
  docker compose exec -T starrocks mysql -P9030 -h127.0.0.1 -uroot --table < "$1"
fi
