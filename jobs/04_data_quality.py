"""
DATA QUALITY + RECONCILIATION: prove the lakehouse matches the source.

Every run appends results to lake.ops.dq_results (an audit trail you can chart in StarRocks/BI).
Exit code 1 if any check FAILS -> an orchestrator (Airflow) would stop downstream tasks and alert.

Checks
  1. Reconciliation  source distinct business keys == silver rows          (nothing lost, nothing doubled)
  2. Quarantine       source rows with missing tool_id == quarantine rows   (bad rows accounted for)
  3. Uniqueness       no duplicate business keys in silver
  4. Referential      every lot_step tool exists in the tool master
  5. Traceability     every lot step resolves to a recipe version (SCD2 join) -> automotive audit need
  6. Freshness        silver max(updated_at) == source max(updated_at)
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pyspark.sql import functions as F  # noqa: E402
from common import NS, OPS, ensure_namespaces, get_spark, new_run_id, read_jdbc  # noqa: E402

S = NS["silver"]

# name, description, source SQL (Postgres) or None, lakehouse SQL (Spark), rule
CHECKS = [
    ("recon_lot_steps", "source distinct (lot_id, step_seq) vs silver rows",
     "SELECT COUNT(*) AS v FROM (SELECT DISTINCT lot_id, step_seq FROM mes.lot_steps WHERE lot_id IS NOT NULL) x",
     f"SELECT COUNT(*) AS v FROM {S}.lot_steps", "equal"),
    ("recon_spc_points", "source valid SPC points vs silver rows",
     "SELECT COUNT(*) AS v FROM mes.spc_points WHERE tool_id IS NOT NULL AND lot_id IS NOT NULL",
     f"SELECT COUNT(*) AS v FROM {S}.spc_points", "equal"),
    ("recon_spc_quarantine", "source invalid SPC points vs quarantine rows",
     "SELECT COUNT(*) AS v FROM mes.spc_points WHERE tool_id IS NULL OR lot_id IS NULL",
     f"SELECT COUNT(*) AS v FROM {S}.spc_points_quarantine", "equal"),
    ("recon_tool_states", "source events vs silver rows",
     "SELECT COUNT(*) AS v FROM mes.tool_state_events",
     f"SELECT COUNT(*) AS v FROM {S}.tool_state_events", "equal"),
    ("unique_lot_steps", "duplicate (lot_id, step_seq) in silver",
     None, f"SELECT COUNT(*) AS v FROM (SELECT lot_id, step_seq FROM {S}.lot_steps "
           f"GROUP BY lot_id, step_seq HAVING COUNT(*) > 1) x", "zero"),
    ("ref_lot_steps_tool", "lot_steps tools missing from tool master",
     None, f"SELECT COUNT(*) AS v FROM {S}.lot_steps s LEFT ANTI JOIN "
           f"(SELECT DISTINCT tool_id FROM {S}.tools) t ON s.tool_id = t.tool_id", "zero"),
    ("genealogy_recipe_version", "lot steps whose recipe version at run time is unknown",
     None, f"SELECT COUNT(*) AS v FROM {NS['gold']}.lot_genealogy WHERE recipe_version_at_run IS NULL", "zero"),
    ("fresh_lot_steps", "minutes silver lags source (max updated_at)",
     "SELECT CAST(EXTRACT(EPOCH FROM MAX(updated_at)) AS BIGINT) AS v FROM mes.lot_steps",
     f"SELECT CAST(unix_timestamp(MAX(updated_at)) AS BIGINT) AS v FROM {S}.lot_steps", "lag_minutes<=0"),
]


def scalar(df):
    return df.collect()[0][0]


def main():
    spark = get_spark("04_data_quality")
    ensure_namespaces(spark)
    run_id = new_run_id()
    results = []
    for name, desc, src_sql, lake_sql, rule in CHECKS:
        actual = scalar(spark.sql(lake_sql))
        expected = scalar(read_jdbc(spark, src_sql)) if src_sql else 0
        if rule == "equal":
            ok = int(actual) == int(expected)
        elif rule == "zero":
            ok = int(actual) == 0
        else:  # freshness: values are epoch seconds
            lag_min = (int(expected) - int(actual)) / 60.0
            expected, actual, ok = 0, round(lag_min, 1), lag_min <= 0
        results.append((run_id, name, desc, str(expected), str(actual), "PASS" if ok else "FAIL"))

    df = spark.createDataFrame(results, "run_id string, check_name string, description string, "
                                        "expected string, actual string, status string") \
              .withColumn("checked_ts", F.current_timestamp())
    if spark.catalog.tableExists(f"{OPS}.dq_results"):
        df.writeTo(f"{OPS}.dq_results").append()
    else:
        df.writeTo(f"{OPS}.dq_results").using("iceberg").create()

    print(f"\nData quality run {run_id}")
    print(f"{'check':<22}{'expected':>10}{'actual':>10}  status")
    for _, name, _, exp, act, status in results:
        print(f"{name:<22}{exp:>10}{act:>10}  {status}")
    failed = [r for r in results if r[5] == "FAIL"]
    spark.stop()
    if failed:
        print(f"\n{len(failed)} check(s) FAILED")
        sys.exit(1)
    print("\nAll checks passed")


if __name__ == "__main__":
    main()
