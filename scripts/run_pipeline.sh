#!/usr/bin/env bash
# Run the full pipeline once: bronze -> silver -> gold -> data quality.
# Each job is idempotent: re-running it does not create duplicates.
#   ./scripts/run_pipeline.sh                 normal run (2-hour overlap window)
#   OVERLAP_HOURS=0 ./scripts/run_pipeline.sh  "break it" experiment (see docs/STEP_BY_STEP.md, step 9)
#   BACKFILL_TABLES=lot_steps ./scripts/run_pipeline.sh   full re-extract of a table (incident recovery)
set -euo pipefail
PKG="org.postgresql:postgresql:42.7.4"
OVERLAP="${OVERLAP_HOURS:-2}"
BACKFILL="${BACKFILL_TABLES:-}"
for job in 01_bronze_ingest 02_silver 03_gold 04_data_quality; do
  echo "=================== ${job} ==================="
  docker compose exec -T -e OVERLAP_HOURS="${OVERLAP}" -e BACKFILL_TABLES="${BACKFILL}" spark \
    spark-submit --packages "${PKG}" "/home/iceberg/jobs/${job}.py" 2> >(grep -v -E "WARN|INFO" >&2)
done
echo "Pipeline finished."
