# Incident report: late-committed MES row silently skipped (8D format)

**Date:** 2026-11-11 · **Severity:** data completeness (1 lot step missing from lakehouse) · **Status:** closed

## D1 – Team
Data platform (pipeline owner).

## D2 – Problem description
After the day-2 incremental load, `recon_lot_steps` failed: source had **1,789** distinct (lot_id, step_seq),
silver had **1,788**. Freshness check still **passed**, so dashboards looked healthy.
Missing row: lot `L261107001`, rework step 99 (step_id 9999), track-out 2026-11-07 23:15.

## D3 – Containment
Pipeline exited with an error after the DQ stage, so no downstream consumer was told the data was complete.
Affected scope identified as one table (`lot_steps`) via the reconciliation table `ops.dq_results`.

## D4 – Root cause (5-Why)
1. Row missing → it was never extracted into bronze.
2. Not extracted → its `updated_at` (11-07 23:15) was older than the run-1 watermark (11-07 23:56:45).
3. Older than the watermark → the MES stamps business time (track-out), but the transaction was **committed later**.
4. Skipped → the extract ran with **no overlap window** (`OVERLAP_HOURS=0`): only `updated_at > watermark` was read.
5. Not noticed by freshness → freshness checks the latest timestamp only, not completeness.

**Root cause:** high-watermark extraction without an overlap window cannot see late-committed transactions.

## D5 – Corrective action
Targeted backfill of the affected table: `BACKFILL_TABLES=lot_steps ./scripts/run_pipeline.sh`.
A normal re-run did **not** recover the row, because the watermark had already moved past it.

## D6 – Verification
Silver MERGE is idempotent: the full re-extract inserted only the missing row, no duplicates.
All 8 DQ checks PASS; `recon_lot_steps` 1,789 = 1,789.

## D7 – Prevention
- Default extract uses a **2-hour overlap window** (`OVERLAP_HOURS=2`), with de-duplication in silver.
- **Reconciliation against the source on every run**, not only freshness; the pipeline fails loudly on mismatch.
- Every run is logged (`ops.ingest_log`, `ops.dq_results`) for audit.
- Long-term option: CDC from the database log (Debezium → Kafka), which captures commits in commit order.

## D8 – Lessons learned
Freshness tells you the pipeline ran; reconciliation tells you it is complete.
Idempotent layers turn recovery into a simple, safe backfill instead of a rebuild.
