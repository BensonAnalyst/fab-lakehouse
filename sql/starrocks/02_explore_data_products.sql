-- The questions an engineer or manager would ask, answered from the gold data products.
-- Run:  ./scripts/starrocks.sh sql/starrocks/02_explore_data_products.sql
-- (or paste them one by one into ./scripts/starrocks.sh / DBeaver)

-- Q1. Which chamber is out of control most often?   (excursion detection)
SELECT tool_id, chamber_id, parameter,
       SUM(points) AS points, SUM(ooc_points) AS ooc_points,
       ROUND(100.0 * SUM(ooc_points) / SUM(points), 2) AS ooc_pct
FROM fab_lake.gold.spc_ooc_daily
GROUP BY tool_id, chamber_id, parameter
ORDER BY ooc_pct DESC
LIMIT 5;

-- Q2. Is it a sudden shift or a slow drift?   (trend for the worst chamber)
SELECT measure_date, mean_value, std_value, target, ooc_pct
FROM fab_lake.gold.spc_ooc_daily
WHERE tool_id = 'ETCH01' AND chamber_id = 'B' AND parameter = 'POLY_CD_NM'
ORDER BY measure_date;

-- Q3. Which lots were processed on that chamber, with which recipe version?   (containment list)
SELECT lot_id, priority, step_id, recipe_id, recipe_version_at_run, track_in_time, spc_mean, spc_ooc_sites
FROM fab_lake.gold.lot_genealogy
WHERE tool_id = 'ETCH01' AND chamber_id = 'B' AND step_id = '3000'
ORDER BY track_in_time DESC
LIMIT 20;

-- Q4. Full traceability for one lot   (what a customer audit asks for)
SELECT step_seq, step_id, tool_id, chamber_id, recipe_id, recipe_version_at_run,
       track_in_time, track_out_time, process_min, queue_min_before_step, spc_parameter, spc_mean
FROM fab_lake.gold.lot_genealogy
WHERE lot_id = 'L261103001'
ORDER BY step_seq;

-- Q5. Downtime Pareto: what is taking tools down?
SELECT unscheduled_reason, COUNT(*) AS episodes, ROUND(SUM(down_min) / 60, 1) AS down_hours
FROM fab_lake.gold.tool_downtime_episodes
WHERE unscheduled_reason IS NOT NULL
GROUP BY unscheduled_reason
ORDER BY down_hours DESC;

-- Q6. Tool availability and utilization (SEMI E10) over the period
SELECT tool_id,
       ROUND(AVG(availability_pct), 1) AS avg_availability_pct,
       ROUND(AVG(utilization_pct), 1)  AS avg_utilization_pct,
       ROUND(SUM(unscheduled_down_h), 1) AS unscheduled_down_h
FROM fab_lake.gold.tool_e10_daily
GROUP BY tool_id
ORDER BY avg_availability_pct;

-- Q7. Where is cycle time lost?   (queue vs process time by step)
SELECT step_id,
       ROUND(AVG(process_min), 1) AS avg_process_min,
       ROUND(AVG(queue_min_before_step), 1) AS avg_queue_min
FROM fab_lake.gold.lot_genealogy
WHERE step_id <> '9999'
GROUP BY step_id
ORDER BY avg_queue_min DESC;

-- Q8. Pipeline health: latest data quality run
SELECT check_name, expected, actual, status, checked_ts
FROM fab_lake.ops.dq_results
WHERE run_id = (SELECT MAX(run_id) FROM fab_lake.ops.dq_results)
ORDER BY check_name;
