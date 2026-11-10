# Step-by-step guide

Work through the steps in order. Each step lists **what to run**, **what you should see**, and
**what to learn / say in an interview**. Total time: about 4–6 hours for steps 0–10.

Commands are for macOS Terminal, run from the project folder unless stated.

---

## Step 0 — Prerequisites (15 min)

1. Install **Docker Desktop** for Mac: https://www.docker.com/products/docker-desktop/
2. Docker Desktop → Settings → Resources → set **Memory to at least 8 GB** (6 GB is the bare minimum).
   On a 16 GB MacBook Air this is fine. On an 8 GB Mac, close other apps and see Troubleshooting.
3. Check the tools:
   ```bash
   docker --version
   docker compose version
   python3 --version      # if missing: xcode-select --install
   git --version
   ```
4. (Optional) install **DBeaver** (free SQL client) to browse Postgres and StarRocks with a GUI.

---

## Step 1 — Put the project on GitHub (10 min)

1. Unzip `fab-lakehouse.zip` into e.g. `~/projects/fab-lakehouse`.
2. On github.com create an **empty** public repo called `fab-lakehouse` (no README, no .gitignore).
3. In Terminal:
   ```bash
   cd ~/projects/fab-lakehouse
   chmod +x scripts/*.sh
   git init
   git add .
   git commit -m "Initial scaffold: on-prem fab lakehouse (MinIO, Iceberg, Spark, StarRocks)"
   git branch -M main
   git remote add origin https://github.com/BensonAnalyst/fab-lakehouse.git
   git push -u origin main
   ```
**Learn:** commit early and often. Interviewers do look at commit history: small, clear commits read as "engineer".

---

## Step 2 — Generate the first week of fab data (5 min)

```bash
python3 generator/generate_fab_data.py --start 2026-11-01 --days 7 --out data/batch_01 --batch 1
```
You should see ~168 lots, ~1,700 lot steps, ~2,300 SPC points, ~600 tool state events.

Open `data/batch_01/lot_steps.csv` and `spc_points.csv` in Excel and look at them.

**Learn / say:** "I modelled the source the way an MES holds it: lot history at step level, SPC points
per site, SEMI E10 equipment states, and a recipe master that only keeps the *current* version."
(`data/` is in `.gitignore`, so generated data never goes to GitHub.)

---

## Step 3 — Start the platform (15–30 min the first time, images download)

```bash
docker compose up -d
docker compose ps
```
Wait until `minio`, `rest`, `postgres`, `spark`, `starrocks` show **running** (the `bucket-init` setup container
runs once, creates the buckets and exits; that is normal — check it with `docker compose logs bucket-init`). StarRocks needs ~1–2 minutes to finish starting.

Check each piece:
| What | How | Expect |
|---|---|---|
| MinIO console | http://localhost:9001 (admin / password) | buckets `warehouse` and `landing` |
| Iceberg catalog | http://localhost:8181/v1/config | a small JSON reply |
| Postgres | `docker compose exec postgres psql -U fab -d fab_source -c "\dt mes.*"` | 6 empty tables |
| StarRocks | `./scripts/starrocks.sh` then `SHOW BACKENDS\G` (`exit` to leave) | `Alive: true` |

**Learn:** MinIO = storage (S3 API), Iceberg REST = catalog (which files make up which table),
Spark = compute for transformations, StarRocks = compute for fast SQL. Storage and compute are separate
and can scale separately — the core idea of a lakehouse.

In the MinIO console, open bucket `landing` → *Lifecycle*: there is a 30-day expiry rule.
That is MinIO ILM for raw landing files. (Never put ILM rules on Iceberg table folders — step 11 explains.)

---

## Step 4 — Load the source "MES" (2 min)

```bash
./scripts/load_source.sh batch_01
```
The last output is a summary table: row counts and `max_updated_at` per source table.

**Learn:** master data is *upserted* (MES keeps current state), transactions are *appended*.
Look at `sql/source/load_batch.sql` — the `ON CONFLICT ... WHERE EXCLUDED.updated_at > ...` means an
older record can never overwrite a newer one.

---

## Step 5 — Run the pipeline for the first time (5–10 min)

```bash
./scripts/run_pipeline.sh
```
The first run downloads the Postgres JDBC driver (needs internet). You will see four sections:
- **01_bronze_ingest** — each table extracted from `1900-01-01` (no watermark yet = full initial load)
- **02_silver** — tables created; `quarantined 0 SPC rows`
- **03_gold** — five data products with row counts
- **04_data_quality** — 8 checks, all **PASS**

Now look at MinIO console → `warehouse` → `bronze/lot_steps/`:
- `data/` contains **Parquet** files, partitioned by day (`updated_at_day=2026-11-01/`, …)
- `metadata/` contains Iceberg **metadata JSON, manifest lists and manifests**

**Learn / say:** "A table in a lakehouse is just Parquet files plus metadata that says which files belong
to the current snapshot. That's what gives ACID writes, time travel and schema evolution on cheap object storage."

Commit: `git add . && git commit -m "First successful pipeline run" && git push`

---

## Step 6 — Query the lake from StarRocks (20 min)

```bash
./scripts/starrocks.sh sql/starrocks/01_create_catalog.sql
./scripts/starrocks.sh sql/starrocks/02_explore_data_products.sql
```
Read each result against the question in the SQL comment. Key finding to notice:
**ETCH01 chamber B** has by far the highest OOC rate, and Q2 shows the CD mean creeping up
day by day (~180 nm → ~190 nm): a **slow drift**, not a sudden shift. Q3 gives the list of lots
processed on that chamber = the **containment list**.

**Learn / say:** "StarRocks reads the Iceberg tables in place through an external catalog — no copy.
For the dashboards that must be very fast I'd materialise into native StarRocks tables (step 10)."

Optional: connect DBeaver → *MySQL* driver → host `localhost`, port `9030`, user `root`, no password.
Power BI Desktop can connect the same way (MySQL connector), which is how a BI layer would sit on top.

---

## Step 7 — The second day arrives: incremental load + messy data (10 min)

```bash
python3 generator/generate_fab_data.py --start 2026-11-08 --days 1 --out data/batch_02 \
        --batch 2 --recipe-changes 3 --inject-issues
./scripts/load_source.sh batch_02
./scripts/run_pipeline.sh
```
The generator prints the issues it injected: duplicate lot steps, SPC points without a tool,
one **late-commit** row, and 3 recipe version changes.

What to check in the pipeline output:
- **bronze:** `lot_steps extracted ~90 rows (updated_at > 2026-11-07 21:56…)` — only the new rows
  plus a 2-hour overlap, not the whole table.
- **silver:** `quarantined 4 SPC rows`
- **data quality:** all 8 checks PASS again — duplicates removed, bad rows quarantined, late row captured.

Look at the watermark log in StarRocks (`./scripts/starrocks.sh`):
```sql
SELECT table_name, extract_from, rows_extracted, new_watermark
FROM fab_lake.ops.ingest_log ORDER BY logged_ts;
```

**Learn / say:** "Incremental extract by high-watermark with an overlap window. Bronze is append-only and
may contain repeats from the overlap; silver de-duplicates on the business key with a MERGE, so every
job is idempotent — I can re-run any step safely after a failure."

---

## Step 8 — See the SCD Type 2 recipe history (10 min)

```sql
-- in ./scripts/starrocks.sh
SELECT * FROM fab_lake.silver.dim_recipe_scd2
WHERE recipe_id IN (SELECT recipe_id FROM fab_lake.silver.dim_recipe_scd2
                    GROUP BY recipe_id HAVING COUNT(*) > 1)
ORDER BY recipe_id, effective_from;
```
Each changed recipe now has two rows with `effective_from` / `effective_to` / `is_current`.
`gold.lot_genealogy.recipe_version_at_run` joins on *track-in time between effective_from and effective_to*,
so every lot step shows the version that was actually used.

**Learn / say (strong point):** "The MES only keeps the current recipe version, so history must be built
in the lakehouse. The limitation: history is only as fine as the extract frequency — if a recipe changes
twice between two extracts, the middle version is lost. For full fidelity I'd use CDC from the database log
(Debezium → Kafka). I also added a DQ check that every lot step resolves to a recipe version, because
for automotive customers traceability is a requirement, not a nice-to-have."

(Same story as your HP firmware SCD2 work — connect the two in the interview.)

---

## Step 9 — Break it on purpose: why the overlap window exists (15 min)

Reset everything and replay both batches **without** the overlap:
```bash
docker compose down -v          # -v deletes all data volumes = clean slate
docker compose up -d            # wait ~2 min for StarRocks/Postgres
./scripts/load_source.sh batch_01
OVERLAP_HOURS=0 ./scripts/run_pipeline.sh
./scripts/load_source.sh batch_02
OVERLAP_HOURS=0 ./scripts/run_pipeline.sh
```
Data quality now shows:
```
recon_lot_steps   expected 1789   actual 1788   FAIL
fresh_lot_steps   ...                           PASS
```
The late-commit row (timestamp *before* the previous watermark) was silently skipped. The
**freshness check still passes** — only **reconciliation against the source** catches it.

**Now fix the incident.** First try a normal run:
```bash
./scripts/run_pipeline.sh
```
Still FAIL. Why? The watermark has already moved past the late row, and a 2-hour overlap from the
*new* watermark doesn't reach back far enough. The fix is a **backfill** of the affected table:
```bash
BACKFILL_TABLES=lot_steps ./scripts/run_pipeline.sh
```
`recon_lot_steps` is back to PASS. The full re-extract is safe because the silver MERGE is idempotent:
rows that already exist change nothing, and only the missing row is inserted.

Treat it like a real incident and write a 5-line RCA (8D style) in `docs/incident_late_commit.md`:
*symptom → detection (reconciliation) → root cause (watermark without overlap vs late commits) →
fix (backfill) → prevention (overlap window + reconciliation on every run)*. Commit it — it shows
exactly the "root-cause analysis and continuous improvement" the JD asks for.

**Learn / say:** "Freshness tells you the pipeline ran; reconciliation tells you it's complete.
Every run reconciles business-key counts against the source and fails loudly, and because every
layer is idempotent, recovery is just a targeted backfill."

---

## Step 10 — StarRocks serving layer: table models + materialized view (20 min)

```bash
./scripts/starrocks.sh sql/starrocks/03_serving_layer.sql
```
Read the comments in the file while it runs:
| StarRocks model | Used for | Why |
|---|---|---|
| **Primary Key** | `wip_current` | state changes constantly → upsert by `lot_id`; inserting twice doesn't double the rows |
| **Duplicate Key** | `tool_state_events_raw` | append-only events, sorted by time for range scans, auto daily partitions |
| **Aggregate Key** | `spc_chamber_daily_agg` | pre-aggregates on load (SUM/MAX per key) |
| **Async materialized view** | `mv_chamber_ooc` | precomputed dashboard table over the lake, refreshed hourly |

**Learn / say:** "I pick the table model by access pattern: Primary Key for mutable state such as WIP,
Duplicate Key for raw events, Aggregate or an async MV for dashboards."

Commit and push.

---

## Step 11 — Lakehouse operations: time travel, compaction, retention (15 min)

```bash
./scripts/run_job.sh 05_iceberg_maintenance
```
You will see: the list of snapshots (one per write), a time-travel row count at the first snapshot,
the number of data files before/after **compaction** (`rewrite_data_files`), and **snapshot expiry**.

**Learn / say:**
- "Frequent small appends create the *small-files problem*; compaction rewrites them into bigger files."
- "Old snapshots keep old files alive on MinIO; `expire_snapshots` is how retention works for Iceberg."
- "I'd never put a MinIO lifecycle rule on Iceberg folders — the metadata still points at those files and the
  table breaks. ILM is for raw landing buckets."

---

## Step 12 — Finish the README for GitHub (30–45 min)

1. Run the queries in step 6 and paste 2–3 result tables (or screenshots) into `README.md` → *Results*.
2. Add a screenshot of the MinIO console showing the Iceberg folders.
3. Fill the *Before vs After* numbers with your own estimates (keep the word "estimated").
4. Commit and push. Pin the repo on your GitHub profile.

---

## Interview cheat sheet — what this project lets you say

- "I built a small version of an on-prem lakehouse: Postgres as the MES source, MinIO + Iceberg as the lake,
  Spark for ingestion and transforms, StarRocks for serving."
- "Incremental ingestion with a watermark and an overlap window; idempotent MERGE in silver."
- "I injected the problems a real MES has — double logging, missing tool IDs, late commits — and the pipeline
  de-duplicates, quarantines and reconciles them."
- "SCD2 for recipe versions so lot genealogy shows the recipe version actually used — traceability for automotive."
- "I chose StarRocks table models by access pattern and used an async MV for the dashboard layer."
- "And I learnt the operational side: compaction, snapshot expiry, why not to use ILM on Iceberg."
- Honest framing: "It's a personal build to learn your stack hands-on; my production experience is Databricks/Delta,
  and the concepts transferred directly."

---

## Stretch goals (after the interview)

1. **Airflow** — add an Airflow container and one DAG: load → bronze → silver → gold → DQ, with retries;
   DQ failure stops the run and alerts.
2. **CDC** — Debezium + Kafka on `mes.recipe_master` and `mes.lot_steps`; StarRocks *Routine Load* from Kafka
   for near-real-time WIP.
3. **Trino** — add Trino and query Postgres + Iceberg in one SQL (data discovery before building pipelines).
4. **Real sensor data** — load the public UCI **SECOM** dataset (fab sensor readings with pass/fail) as an FDC source,
   then build an anomaly / yield model on top of gold.
5. **Dashboard** — Superset container or Power BI (MySQL connector to StarRocks port 9030).
6. **Lineage** — OpenLineage + Marquez to show column-level lineage bronze → gold automatically.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `pull access denied for minio/minio` | MinIO removed its images from Docker Hub (Sep 2026). This project already uses `cgr.dev/chainguard/minio:latest`; make sure your `docker-compose.yml` is the updated one |
| MinIO log says `file access denied` | the Chainguard image runs as uid 65532; the compose file sets `user: "0:0"` to avoid this. If you changed it, put it back |
| A container keeps restarting / Mac very slow | Docker Desktop memory too low. Raise to 8 GB+, or stop StarRocks while running Spark: `docker compose stop starrocks` then `start` it later |
| `image ... not found` or `no matching manifest for linux/arm64` | Edit the `image:` line in `docker-compose.yml` to a tag that exists for Apple Silicon (check the image's Docker Hub page), or add `platform: linux/amd64` under that service (slower, uses emulation) |
| `run_pipeline.sh` fails downloading `org.postgresql` | Needs internet on first run; retry. Behind a corporate proxy, run from home network |
| `Connection refused` to `rest:8181` or `minio:9000` | `docker compose ps` — start the missing service: `docker compose up -d rest minio` |
| StarRocks `Access Denied` / `Backend not alive` | Wait 2 minutes after start; check `docker compose logs starrocks` |
| StarRocks query error on `fab_lake...` | Run step 6's catalog script again; check `SHOW CATALOGS;` |
| `load_source.sh` says file not found | Generator output must be under `data/` in the project folder (`--out data/batch_01`) |
| Start completely fresh | `docker compose down -v` (deletes all data), then step 3 onwards |
| Port already in use (5433, 9000, 9030 …) | Change the left-hand number in the `ports:` line of that service |
