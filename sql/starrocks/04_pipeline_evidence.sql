-- Evidence that the pipeline behaves correctly (run after Step 7)
SELECT table_name, extract_from, rows_extracted, new_watermark
FROM fab_lake.ops.ingest_log ORDER BY logged_ts, table_name;

SELECT recipe_id, recipe_version, effective_from, effective_to, is_current
FROM fab_lake.silver.dim_recipe_scd2
WHERE recipe_id IN (SELECT recipe_id FROM fab_lake.silver.dim_recipe_scd2
                    GROUP BY recipe_id HAVING COUNT(*) > 1)
ORDER BY recipe_id, effective_from;

SELECT lot_id, step_seq, step_id, track_out_time
FROM fab_lake.silver.lot_steps WHERE step_id = '9999';

SELECT check_name, expected, actual, status
FROM fab_lake.ops.dq_results
WHERE run_id = (SELECT MAX(run_id) FROM fab_lake.ops.dq_results)
ORDER BY check_name;
