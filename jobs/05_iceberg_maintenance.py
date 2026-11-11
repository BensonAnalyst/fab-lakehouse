"""
ICEBERG TABLE MAINTENANCE: the "operations" side of an on-prem lakehouse.

  1. Snapshots + time travel  -> every write is a snapshot; you can query the table as it was
  2. Small-files compaction    -> frequent small appends create many tiny files; rewrite them
  3. Snapshot expiry           -> old snapshots keep old files alive on MinIO; expire them to reclaim space

Snapshots and files are read through the Iceberg Java API (Spark3Util.loadIcebergTable) instead of
the SQL metadata tables (`table.snapshots`): newer Iceberg REST catalog servers reject the
namespace lookup Spark tries first for `db.table.snapshots` ("Suspicious Path Character").

Important: never use a MinIO lifecycle (ILM) rule to delete files under an Iceberg table.
Iceberg's metadata still points at them and the table breaks. Retention for Iceberg is done
with expire_snapshots (this job); ILM is for plain landing/raw-file buckets.
"""
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import CATALOG, SILVER, get_spark  # noqa: E402

TABLE = f"{SILVER}.lot_steps"          # lake.silver.lot_steps
SHORT = TABLE.split(".", 1)[1]         # silver.lot_steps (procedures take the name without the catalog)


def load_table(spark):
    """Iceberg Table object (Java) for TABLE, freshly loaded."""
    t = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, TABLE)
    t.refresh()
    return t


def list_snapshots(table):
    rows, it = [], table.snapshots().iterator()
    while it.hasNext():
        s = it.next()
        committed = datetime.fromtimestamp(s.timestampMillis() / 1000, tz=timezone.utc)
        rows.append((committed, s.snapshotId(), s.operation(), s.summary().get("added-records")))
    return sorted(rows)


def file_stats(table):
    """Number of live data files and their average size (KB) in the current snapshot."""
    n, total = 0, 0
    tasks = table.newScan().planFiles()
    it = tasks.iterator()
    while it.hasNext():
        f = it.next().file()
        n += 1
        total += f.fileSizeInBytes()
    tasks.close()
    return n, round(total / n / 1024, 1) if n else 0.0


def main():
    spark = get_spark("05_iceberg_maintenance")
    table = load_table(spark)

    print("\n== 1. Snapshots (one per write) ==")
    snaps = list_snapshots(table)
    print(f"{'committed_at (UTC)':<22}{'snapshot_id':>22}  {'operation':<10}{'added_records':>14}")
    for committed, sid, op, added in snaps:
        print(f"{committed:%Y-%m-%d %H:%M:%S}   {sid:>22}  {op:<10}{str(added):>14}")
    if snaps:
        first_id = snaps[0][1]
        n_then = spark.sql(f"SELECT COUNT(*) FROM {TABLE} VERSION AS OF {first_id}").first()[0]
        n_now = spark.table(TABLE).count()
        print(f"\nTime travel: rows at first snapshot = {n_then:,}; rows now = {n_now:,}")

    print("\n== 2. Small-files compaction ==")
    files_before, kb_before = file_stats(table)
    print(f"Before: {files_before} data files, avg {kb_before} KB")
    spark.sql(f"CALL {CATALOG}.system.rewrite_data_files(table => '{SHORT}', "
              f"options => map('min-input-files', '2'))").show(truncate=False)
    table = load_table(spark)
    files_after, kb_after = file_stats(table)
    print(f"After:  {files_after} data files, avg {kb_after} KB")

    n_before_expiry = len(list_snapshots(table))
    print("\n== 3. Expire snapshots older than 1 minute, keep the last 2 ==")
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
    spark.sql(f"CALL {CATALOG}.system.expire_snapshots(table => '{SHORT}', "
              f"older_than => TIMESTAMP '{cutoff}', retain_last => 2)").show(truncate=False)
    table = load_table(spark)
    print(f"Snapshots before expiry: {n_before_expiry}; remaining now: {len(list_snapshots(table))}")
    spark.stop()


if __name__ == "__main__":
    main()
