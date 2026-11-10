-- Connect StarRocks to the Iceberg tables on MinIO WITHOUT copying data (external catalog).
-- Run:  ./scripts/starrocks.sh sql/starrocks/01_create_catalog.sql

CREATE EXTERNAL CATALOG IF NOT EXISTS fab_lake
COMMENT "Iceberg lakehouse on MinIO (bronze / silver / gold)"
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "rest",
    "iceberg.catalog.uri" = "http://rest:8181",
    "iceberg.catalog.warehouse" = "warehouse",
    "aws.s3.access_key" = "admin",
    "aws.s3.secret_key" = "password",
    "aws.s3.endpoint" = "http://minio:9000",
    "aws.s3.enable_path_style_access" = "true",
    "aws.s3.region" = "us-east-1"
);

SHOW CATALOGS;
SHOW DATABASES FROM fab_lake;
SHOW TABLES FROM fab_lake.gold;
