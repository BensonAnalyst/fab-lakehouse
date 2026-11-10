#!/usr/bin/env bash
# Run a single Spark job, e.g.  ./scripts/run_job.sh 05_iceberg_maintenance
set -euo pipefail
JOB="${1:?usage: ./scripts/run_job.sh <job name without .py>}"
docker compose exec -T spark spark-submit --packages org.postgresql:postgresql:42.7.4 \
  "/home/iceberg/jobs/${JOB}.py" 2> >(grep -v -E "WARN|INFO" >&2)
