"""
SILVER: clean, de-duplicated, conformed tables + SCD Type 2 recipe history.

Rules applied (see docs/source_to_target_mapping.md):
  * de-duplicate on the BUSINESS key, keep the latest updated_at
      lot_steps: (lot_id, step_seq)  -> removes MES double-logging and overlap repeats
  * rows failing hard rules go to a QUARANTINE table instead of silently disappearing
      spc_points with no tool_id cannot be attributed to equipment -> quarantine
  * recipe_master only holds the CURRENT version in the MES; silver rebuilds the
    full version history from bronze snapshots (SCD Type 2)
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import NS, ensure_namespaces, get_spark, replace_table, upsert  # noqa: E402

LOTS = """
SELECT lot_id, product_id, priority, wafer_qty, lot_status, start_time, updated_at
FROM (SELECT b.*, ROW_NUMBER() OVER (PARTITION BY lot_id ORDER BY updated_at DESC) AS rn
      FROM {bronze}.lots b) x
WHERE rn = 1
"""

TOOLS = """
SELECT tool_id, chamber_id, module, bay, status, updated_at
FROM (SELECT b.*, ROW_NUMBER() OVER (PARTITION BY tool_id, chamber_id ORDER BY updated_at DESC) AS rn
      FROM {bronze}.tools b) x
WHERE rn = 1
"""

# tie-break on txn_id ASC = keep the ORIGINAL log line when MES double-logs the same step
LOT_STEPS = """
SELECT txn_id, lot_id, step_seq, step_id, tool_id, chamber_id, recipe_id, wafer_qty,
       track_in_time, track_out_time, updated_at
FROM (SELECT b.*, ROW_NUMBER() OVER (PARTITION BY lot_id, step_seq
                                     ORDER BY updated_at DESC, txn_id ASC) AS rn
      FROM {bronze}.lot_steps b
      WHERE lot_id IS NOT NULL AND step_seq IS NOT NULL) x
WHERE rn = 1
"""

SPC_DEDUP = """
SELECT point_id, lot_id, step_id, parameter, tool_id, chamber_id, site,
       CAST(value AS DOUBLE) AS value, CAST(target AS DOUBLE) AS target,
       CAST(ucl AS DOUBLE) AS ucl, CAST(lcl AS DOUBLE) AS lcl,
       CAST(ooc_flag AS INT) AS ooc_flag, measure_time, updated_at
FROM (SELECT b.*, ROW_NUMBER() OVER (PARTITION BY point_id ORDER BY updated_at DESC) AS rn
      FROM {bronze}.spc_points b) x
WHERE rn = 1
"""

TOOL_STATES = """
SELECT event_id, tool_id, e10_state, reason_code, event_time, updated_at
FROM (SELECT b.*, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY updated_at DESC) AS rn
      FROM {bronze}.tool_state_events b) x
WHERE rn = 1
"""

# SCD Type 2: each distinct version seen becomes a row with a validity interval.
# Limitation worth saying in an interview: history is only as fine-grained as the extract
# frequency. If a recipe changes twice between two extracts, the middle version is never seen
# -> for full fidelity use CDC (Debezium/Kafka) on the source table.
RECIPE_SCD2 = """
WITH versions AS (
    SELECT recipe_id, step_id, tool_id, recipe_version, MIN(updated_at) AS effective_from
    FROM {bronze}.recipe_master
    GROUP BY recipe_id, step_id, tool_id, recipe_version
)
SELECT recipe_id, step_id, tool_id, recipe_version, effective_from,
       COALESCE(LEAD(effective_from) OVER (PARTITION BY recipe_id ORDER BY effective_from),
                TIMESTAMP '9999-12-31 00:00:00') AS effective_to,
       LEAD(effective_from) OVER (PARTITION BY recipe_id ORDER BY effective_from) IS NULL AS is_current
FROM versions
"""


def main():
    spark = get_spark("02_silver")
    ensure_namespaces(spark)
    S = NS["silver"]

    def q(sql):
        return spark.sql(sql.format(**NS))

    print("lots          :", upsert(spark, q(LOTS), f"{S}.lots", ["lot_id"]))
    print("lot_steps     :", upsert(spark, q(LOT_STEPS), f"{S}.lot_steps", ["lot_id", "step_seq"],
                                   partition_day_col="track_in_time"))

    spc = q(SPC_DEDUP).cache()
    good = spc.where("tool_id IS NOT NULL AND lot_id IS NOT NULL")
    bad = spc.where("tool_id IS NULL OR lot_id IS NULL") \
             .selectExpr("*", "'missing tool_id or lot_id' AS quarantine_reason")
    print("spc_points    :", upsert(spark, good, f"{S}.spc_points", ["point_id"], partition_day_col="measure_time"))
    replace_table(bad, f"{S}.spc_points_quarantine")
    print(f"  quarantined {bad.count()} SPC rows")

    print("tool_states   :", upsert(spark, q(TOOL_STATES), f"{S}.tool_state_events", ["event_id"],
                                   partition_day_col="event_time"))
    replace_table(q(TOOLS), f"{S}.tools")
    replace_table(q(RECIPE_SCD2), f"{S}.dim_recipe_scd2")
    print("tools + dim_recipe_scd2 rebuilt")
    spark.stop()


if __name__ == "__main__":
    main()
