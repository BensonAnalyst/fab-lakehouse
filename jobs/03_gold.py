"""
GOLD: business-ready data products (full refresh each run = simple and idempotent).

  gold.lot_genealogy          every lot x step: tool, chamber, recipe VERSION at run time (SCD2 join),
                              process/queue minutes, SPC result  -> automotive traceability
  gold.spc_ooc_daily          OOC rate per day x process tool x chamber x parameter -> excursion detection
  gold.tool_downtime_episodes consecutive down states merged into episodes (gaps & islands)
  gold.tool_e10_daily         SEMI E10 hours, availability % and utilization % per tool per day
  gold.wip_current            lots currently in process and how long they have been in the step
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import NS, ensure_namespaces, get_spark, replace_table  # noqa: E402

LOT_GENEALOGY = """
WITH spc AS (
    SELECT lot_id, step_id, parameter, ROUND(AVG(value), 3) AS spc_mean, SUM(ooc_flag) AS spc_ooc_sites
    FROM {silver}.spc_points
    GROUP BY lot_id, step_id, parameter
)
SELECT s.lot_id, l.product_id, l.priority, s.step_seq, s.step_id, s.tool_id, s.chamber_id,
       s.recipe_id, r.recipe_version AS recipe_version_at_run,
       s.wafer_qty, s.track_in_time, s.track_out_time,
       ROUND((unix_timestamp(s.track_out_time) - unix_timestamp(s.track_in_time)) / 60.0, 1) AS process_min,
       ROUND((unix_timestamp(s.track_in_time)
              - unix_timestamp(LAG(s.track_out_time) OVER (PARTITION BY s.lot_id ORDER BY s.step_seq))) / 60.0, 1)
           AS queue_min_before_step,
       spc.parameter AS spc_parameter, spc.spc_mean, spc.spc_ooc_sites
FROM {silver}.lot_steps s
LEFT JOIN {silver}.lots l
       ON s.lot_id = l.lot_id
LEFT JOIN {silver}.dim_recipe_scd2 r
       ON s.recipe_id = r.recipe_id
      AND s.track_in_time >= r.effective_from AND s.track_in_time < r.effective_to
LEFT JOIN spc
       ON s.lot_id = spc.lot_id AND s.step_id = spc.step_id
"""

SPC_OOC_DAILY = """
SELECT CAST(measure_time AS DATE) AS measure_date, tool_id, chamber_id, parameter,
       COUNT(*) AS points,
       SUM(ooc_flag) AS ooc_points,
       ROUND(100.0 * SUM(ooc_flag) / COUNT(*), 2) AS ooc_pct,
       ROUND(AVG(value), 3) AS mean_value,
       ROUND(STDDEV(value), 3) AS std_value,
       MAX(target) AS target
FROM {silver}.spc_points
GROUP BY CAST(measure_time AS DATE), tool_id, chamber_id, parameter
"""

# one row per state period: from this event until the next event of the same tool
STATE_PERIODS = """
SELECT tool_id, e10_state, reason_code, event_time AS start_time,
       LEAD(event_time) OVER (PARTITION BY tool_id ORDER BY event_time) AS end_time,
       CASE WHEN e10_state IN ('SCHEDULED_DOWN', 'UNSCHEDULED_DOWN') THEN 1 ELSE 0 END AS is_down
FROM {silver}.tool_state_events
"""

# gaps & islands: consecutive down periods (e.g. PM that turns into an unscheduled repair) = one episode
DOWNTIME_EPISODES = """
WITH p AS ({periods}),
g AS (
    SELECT p.*,
           ROW_NUMBER() OVER (PARTITION BY tool_id ORDER BY start_time)
         - ROW_NUMBER() OVER (PARTITION BY tool_id, is_down ORDER BY start_time) AS grp
    FROM p
    WHERE end_time IS NOT NULL
)
SELECT tool_id,
       MIN(start_time) AS down_start,
       MAX(end_time)   AS down_end,
       ROUND((unix_timestamp(MAX(end_time)) - unix_timestamp(MIN(start_time))) / 60.0, 1) AS down_min,
       array_join(transform(array_sort(collect_list(struct(start_time, e10_state))), x -> x.e10_state), ' > ')
           AS state_path,
       MAX(CASE WHEN e10_state = 'UNSCHEDULED_DOWN' THEN reason_code END) AS unscheduled_reason,
       COUNT(*) AS state_changes
FROM g
WHERE is_down = 1
GROUP BY tool_id, grp
"""

# simplification: a period is counted on the day it starts
TOOL_E10_DAILY = """
WITH p AS ({periods}),
h AS (
    SELECT tool_id, CAST(start_time AS DATE) AS state_date, e10_state, is_down,
           (unix_timestamp(end_time) - unix_timestamp(start_time)) / 3600.0 AS hours
    FROM p
    WHERE end_time IS NOT NULL
)
SELECT tool_id, state_date,
       ROUND(SUM(CASE WHEN e10_state = 'PRODUCTIVE' THEN hours ELSE 0 END), 2)       AS productive_h,
       ROUND(SUM(CASE WHEN e10_state = 'STANDBY' THEN hours ELSE 0 END), 2)          AS standby_h,
       ROUND(SUM(CASE WHEN e10_state = 'ENGINEERING' THEN hours ELSE 0 END), 2)      AS engineering_h,
       ROUND(SUM(CASE WHEN e10_state = 'SCHEDULED_DOWN' THEN hours ELSE 0 END), 2)   AS scheduled_down_h,
       ROUND(SUM(CASE WHEN e10_state = 'UNSCHEDULED_DOWN' THEN hours ELSE 0 END), 2) AS unscheduled_down_h,
       ROUND(100.0 * SUM(CASE WHEN is_down = 0 THEN hours ELSE 0 END) / SUM(hours), 1) AS availability_pct,
       ROUND(100.0 * SUM(CASE WHEN e10_state = 'PRODUCTIVE' THEN hours ELSE 0 END) / SUM(hours), 1)
           AS utilization_pct
FROM h
GROUP BY tool_id, state_date
"""

WIP_CURRENT = """
SELECT s.lot_id, l.priority, s.step_seq, s.step_id, s.tool_id, s.chamber_id, s.track_in_time,
       ROUND((unix_timestamp(a.as_of) - unix_timestamp(s.track_in_time)) / 3600.0, 2) AS hours_in_step,
       a.as_of
FROM {silver}.lot_steps s
JOIN {silver}.lots l ON s.lot_id = l.lot_id
CROSS JOIN (SELECT MAX(updated_at) AS as_of FROM {silver}.lot_steps) a
WHERE s.track_out_time IS NULL
"""


def build(spark, sql):
    return spark.sql(sql.format(periods=STATE_PERIODS.format(**NS), **NS))


def main():
    spark = get_spark("03_gold")
    ensure_namespaces(spark)
    G = NS["gold"]
    products = {
        "lot_genealogy": LOT_GENEALOGY,
        "spc_ooc_daily": SPC_OOC_DAILY,
        "tool_downtime_episodes": DOWNTIME_EPISODES,
        "tool_e10_daily": TOOL_E10_DAILY,
        "wip_current": WIP_CURRENT,
    }
    for name, sql in products.items():
        df = build(spark, sql)
        replace_table(df, f"{G}.{name}")
        print(f"  gold.{name:<24} {spark.table(f'{G}.{name}').count():>6,} rows")
    spark.stop()


if __name__ == "__main__":
    main()
