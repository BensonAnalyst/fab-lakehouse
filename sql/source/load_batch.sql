-- Loads one generated batch into the source "MES" database.
-- Called by scripts/load_source.sh with:  psql -v dir=/data/batch_01 -f load_batch.sql
-- Master data is UPSERTED (MES keeps current state); transactional tables are APPENDED.

\set ON_ERROR_STOP on
BEGIN;

CREATE TEMP TABLE stg_tools (LIKE mes.tools) ON COMMIT DROP;
CREATE TEMP TABLE stg_recipe (LIKE mes.recipe_master) ON COMMIT DROP;
CREATE TEMP TABLE stg_lots (LIKE mes.lots) ON COMMIT DROP;

\set f :dir '/tools.csv'
COPY stg_tools FROM :'f' WITH (FORMAT csv, HEADER true);
\set f :dir '/recipe_master.csv'
COPY stg_recipe FROM :'f' WITH (FORMAT csv, HEADER true);
\set f :dir '/lots.csv'
COPY stg_lots FROM :'f' WITH (FORMAT csv, HEADER true);

INSERT INTO mes.tools SELECT * FROM stg_tools
ON CONFLICT (tool_id, chamber_id) DO UPDATE
   SET module = EXCLUDED.module, bay = EXCLUDED.bay, status = EXCLUDED.status, updated_at = EXCLUDED.updated_at
 WHERE EXCLUDED.updated_at > mes.tools.updated_at;

-- only a NEWER release overwrites the current version
INSERT INTO mes.recipe_master SELECT * FROM stg_recipe
ON CONFLICT (recipe_id) DO UPDATE
   SET recipe_version = EXCLUDED.recipe_version, updated_at = EXCLUDED.updated_at
 WHERE EXCLUDED.updated_at > mes.recipe_master.updated_at;

INSERT INTO mes.lots SELECT * FROM stg_lots
ON CONFLICT (lot_id) DO UPDATE
   SET lot_status = EXCLUDED.lot_status, updated_at = EXCLUDED.updated_at
 WHERE EXCLUDED.updated_at > mes.lots.updated_at;

\set f :dir '/lot_steps.csv'
COPY mes.lot_steps FROM :'f' WITH (FORMAT csv, HEADER true);
\set f :dir '/spc_points.csv'
COPY mes.spc_points FROM :'f' WITH (FORMAT csv, HEADER true);
\set f :dir '/tool_state_events.csv'
COPY mes.tool_state_events FROM :'f' WITH (FORMAT csv, HEADER true);

COMMIT;

SELECT 'lot_steps' AS table_name, COUNT(*) AS total_rows, MAX(updated_at) AS max_updated_at FROM mes.lot_steps
UNION ALL SELECT 'spc_points', COUNT(*), MAX(updated_at) FROM mes.spc_points
UNION ALL SELECT 'tool_state_events', COUNT(*), MAX(updated_at) FROM mes.tool_state_events
UNION ALL SELECT 'lots', COUNT(*), MAX(updated_at) FROM mes.lots
UNION ALL SELECT 'recipe_master', COUNT(*), MAX(updated_at) FROM mes.recipe_master;
