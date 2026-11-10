#!/usr/bin/env bash
# Load one generated batch folder (e.g. data/batch_01) into the source Postgres "MES".
# Usage: ./scripts/load_source.sh batch_01
set -euo pipefail
BATCH="${1:?usage: ./scripts/load_source.sh <batch folder name under data/, e.g. batch_01>}"
docker compose exec -T postgres \
  psql -U fab -d fab_source -v dir="/data/${BATCH}" -f /sql/load_batch.sql
