"""Shared helpers for the Spark jobs: Spark session, namespaces, JDBC reads, idempotent upserts."""
import os
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F

CATALOG = "lake"
BRONZE, SILVER, GOLD, OPS = (f"{CATALOG}.bronze", f"{CATALOG}.silver", f"{CATALOG}.gold", f"{CATALOG}.ops")
NS = {"bronze": BRONZE, "silver": SILVER, "gold": GOLD, "ops": OPS}

JDBC_URL = os.getenv("SOURCE_JDBC_URL", "jdbc:postgresql://postgres:5432/fab_source")
JDBC_USER = os.getenv("SOURCE_USER", "fab")
JDBC_PASSWORD = os.getenv("SOURCE_PASSWORD", "fab")


def get_spark(app_name: str) -> SparkSession:
    """Spark session wired to the Iceberg REST catalog, with data files on MinIO (S3 API)."""
    c = f"spark.sql.catalog.{CATALOG}"
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(c, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{c}.type", "rest")
        .config(f"{c}.uri", os.getenv("ICEBERG_REST_URI", "http://rest:8181"))
        .config(f"{c}.warehouse", "s3://warehouse/")
        .config(f"{c}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{c}.s3.endpoint", os.getenv("S3_ENDPOINT", "http://minio:9000"))
        .config(f"{c}.s3.path-style-access", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")  # small data, laptop-sized
        .getOrCreate()
    )


def ensure_namespaces(spark):
    for ns in NS.values():
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]


def read_jdbc(spark, query: str):
    """Read a SQL query from the source DB. For big tables you would add
    partitionColumn / lowerBound / upperBound / numPartitions for parallel reads."""
    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("user", JDBC_USER)
        .option("password", JDBC_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("query", query)
        .load()
    )


def table_exists(spark, name: str) -> bool:
    return spark.catalog.tableExists(name)


def upsert(spark, df, target: str, keys, order_col="updated_at", partition_day_col=None):
    """Idempotent MERGE: insert new keys, update existing keys only if the incoming row is newer.
    Re-running with the same input changes nothing (safe for retries and overlap windows)."""
    if not table_exists(spark, target):
        writer = df.writeTo(target).using("iceberg")
        if partition_day_col:
            writer = writer.partitionedBy(F.days(partition_day_col))
        writer.create()
        return "created"
    view = "_upsert_src"
    df.createOrReplaceTempView(view)
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    spark.sql(f"""
        MERGE INTO {target} t
        USING {view} s
        ON {on}
        WHEN MATCHED AND s.{order_col} > t.{order_col} THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    return "merged"


def replace_table(df, target: str):
    """Full refresh (used for gold data products and small dimensions)."""
    df.writeTo(target).using("iceberg").createOrReplace()
