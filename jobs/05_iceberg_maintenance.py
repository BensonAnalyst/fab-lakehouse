"""
ICEBERG TABLE MAINTENANCE: the "operations" side of an on-prem lakehouse.

  1. Snapshots + time travel  -> every write is a snapshot; you can query the table as it was
  2. Small-files compaction    -> frequent small appends create many tiny files; rewrite them
  3. Snapshot expiry           -> old snapshots keep old files alive on MinIO; expire them to reclaim space

Important: never use a MinIO lifecycle (ILM) rule to delete files under an Iceberg table.
Iceberg's metadata still points at them and the table breaks. Retention for Iceberg is done
with expire_snapshots (this job); ILM is for plain landing/raw-file buckets.
"""
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import CATALOG, SILVER, get_spark  # noqa: E402

TABLE = f"{SILVER}.lot_steps"
SHORT = TABLE.split(".", 1)[1]  # silver.lot_steps (procedures take the name without the catalog)


def main():
    spark = get_spark("05_iceberg_maintenance")

    print("\n== 1. Snapshots (one per write) ==")
    snaps = spark.sql(f"SELECT committed_at, snapshot_id, operation, summary['added-records'] AS added "
                      f"FROM {TABLE}.snapshots ORDER BY committed_at")
    snaps.show(truncate=False)
    first = snaps.first()
    if first:
        n_then = spark.sql(f"SELECT COUNT(*) FROM {TABLE} VERSION AS OF {first['snapshot_id']}").first()[0]
        n_now = spark.table(TABLE).count()
        print(f"Time travel: rows at first snapshot = {n_then:,}; rows now = {n_now:,}")

    print("\n== 2. Data files before compaction ==")
    spark.sql(f"SELECT COUNT(*) AS files, ROUND(AVG(file_size_in_bytes)/1024, 1) AS avg_kb "
              f"FROM {TABLE}.files").show()
    spark.sql(f"CALL {CATALOG}.system.rewrite_data_files(table => '{SHORT}', "
              f"options => map('min-input-files', '2'))").show(truncate=False)
    print("Data files after compaction:")
    spark.sql(f"SELECT COUNT(*) AS files, ROUND(AVG(file_size_in_bytes)/1024, 1) AS avg_kb "
              f"FROM {TABLE}.files").show()

    print("\n== 3. Expire snapshots older than 1 minute, keep the last 2 ==")
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
    spark.sql(f"CALL {CATALOG}.system.expire_snapshots(table => '{SHORT}', "
              f"older_than => TIMESTAMP '{cutoff}', retain_last => 2)").show(truncate=False)
    spark.sql(f"SELECT COUNT(*) AS snapshots_remaining FROM {TABLE}.snapshots").show()
    spark.stop()


if __name__ == "__main__":
    main()
