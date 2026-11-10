# Source-to-target mapping

How each source field travels from the MES into the lakehouse, and which rule is applied where.
This is the document a data engineer writes **before** building a pipeline, and keeps up to date after.

Layers: **Source** (Postgres `mes.*`, stands in for the MES/SPC Oracle databases) → **Bronze** (raw, append-only,
Iceberg on MinIO) → **Silver** (clean, de-duplicated, conformed) → **Gold** (data products).

## Common technical columns (bronze)

| Column | Rule |
|---|---|
| `_ingest_ts` | timestamp the row was written to bronze |
| `_run_id` | id of the ingestion run (links to `ops.ingest_log`) |
| `_source` | e.g. `postgres.mes.lot_steps` (lineage) |

Extract rule (all tables): `updated_at > high_watermark − OVERLAP (2h)`; watermark in `ops.etl_watermark`.

## mes.lot_steps → silver.lot_steps → gold.lot_genealogy

| Source column | Type | Silver column | Gold column | Transformation / rule |
|---|---|---|---|---|
| `txn_id` | BIGINT | `txn_id` | — | kept for audit; tie-breaker when de-duplicating (lowest = original log) |
| `lot_id` | VARCHAR | `lot_id` | `lot_id` | **business key part 1**; rows with NULL are dropped in silver |
| `step_seq` | INT | `step_seq` | `step_seq` | **business key part 2**; de-dup: one row per (lot_id, step_seq), latest `updated_at` |
| `step_id` | VARCHAR | `step_id` | `step_id` | as-is |
| `tool_id` | VARCHAR | `tool_id` | `tool_id` | DQ: must exist in `silver.tools` (check `ref_lot_steps_tool`) |
| `chamber_id` | VARCHAR | `chamber_id` | `chamber_id` | as-is |
| `recipe_id` | VARCHAR | `recipe_id` | `recipe_id` | lookup key into `dim_recipe_scd2` |
| — | — | — | `recipe_version_at_run` | **SCD2 join**: `track_in_time >= effective_from AND < effective_to`; DQ: never NULL |
| `wafer_qty` | INT | `wafer_qty` | `wafer_qty` | as-is |
| `track_in_time` | TIMESTAMP | `track_in_time` | `track_in_time` | silver partitioned by day of this column |
| `track_out_time` | TIMESTAMP | `track_out_time` | `track_out_time` | NULL = still in process → feeds `gold.wip_current` |
| — | — | — | `process_min` | `(track_out − track_in)` in minutes |
| — | — | — | `queue_min_before_step` | `track_in − previous step's track_out` (LAG over lot, ordered by step_seq) |
| — | — | — | `spc_parameter`, `spc_mean`, `spc_ooc_sites` | from `silver.spc_points` aggregated per lot + step |
| `updated_at` | TIMESTAMP | `updated_at` | — | watermark column; MERGE only updates when incoming is newer |

## mes.spc_points → silver.spc_points (+ quarantine) → gold.spc_ooc_daily

| Source column | Silver | Rule |
|---|---|---|
| `point_id` | `point_id` | business key; de-dup latest `updated_at` |
| `lot_id`, `tool_id` | same | **NULL in either → `silver.spc_points_quarantine`** with `quarantine_reason` (not dropped silently) |
| `tool_id`, `chamber_id` | same | = the **process** tool/chamber that made the feature (not the metrology tool) |
| `value`, `target`, `ucl`, `lcl` | same, as DOUBLE | cast from NUMERIC |
| `ooc_flag` | `ooc_flag` INT | 1 if outside ±3σ limits |
| `measure_time` | same | silver partitioned by day; gold groups by `CAST(measure_time AS DATE)` |

Gold `spc_ooc_daily`: grain = day × tool × chamber × parameter; `ooc_pct = 100 × SUM(ooc_flag) / COUNT(*)`.

## mes.recipe_master → silver.dim_recipe_scd2

| Source | Silver | Rule |
|---|---|---|
| `recipe_id`, `step_id`, `tool_id`, `recipe_version` | same | one row per distinct version ever captured in bronze |
| `updated_at` | `effective_from` | `MIN(updated_at)` of that version |
| — | `effective_to` | next version's `effective_from`, else `9999-12-31` |
| — | `is_current` | true for the open-ended row |

Known limitation: versions that live and die between two extracts are not captured → CDC for full fidelity.
First load must be taken when the source is in a known state (generator batch 1 uses no recipe changes).

## mes.tool_state_events → silver.tool_state_events → gold E10 products

| Source | Silver / Gold | Rule |
|---|---|---|
| `event_id` | key | de-dup latest |
| `e10_state`, `reason_code`, `event_time` | same | a state lasts from its `event_time` until the tool's next event (LEAD) |
| — | `gold.tool_downtime_episodes` | consecutive SCHEDULED/UNSCHEDULED_DOWN periods merged (gaps & islands) |
| — | `gold.tool_e10_daily` | hours per E10 state per day; availability = non-down / total; utilization = productive / total (period counted on its start day — simplification) |

## Data quality checks (ops.dq_results)

| Check | Rule | Catches |
|---|---|---|
| `recon_lot_steps` | source distinct (lot_id, step_seq) = silver rows | lost or doubled rows (e.g. late commits) |
| `recon_spc_points` | source valid points = silver rows | lost rows |
| `recon_spc_quarantine` | source invalid points = quarantine rows | bad rows silently dropped |
| `recon_tool_states` | source events = silver rows | lost rows |
| `unique_lot_steps` | no duplicate business keys in silver | broken de-dup |
| `ref_lot_steps_tool` | every lot_step tool in tool master | orphan references |
| `genealogy_recipe_version` | no NULL recipe version in genealogy | traceability gaps |
| `fresh_lot_steps` | silver max(updated_at) = source max(updated_at) | stale pipeline |
