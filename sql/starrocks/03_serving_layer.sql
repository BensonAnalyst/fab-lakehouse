-- Serving layer INSIDE StarRocks: the three table models + an async materialized view.
-- Run:  ./scripts/starrocks.sh sql/starrocks/03_serving_layer.sql
-- replication_num = 1 because this demo has a single backend node (production: 3).

CREATE DATABASE IF NOT EXISTS fab_serving;
USE fab_serving;

-- 1) PRIMARY KEY model: rows are UPSERTED by key -> right for state that keeps changing (live WIP).
CREATE TABLE IF NOT EXISTS wip_current (
    lot_id         VARCHAR(20) NOT NULL,
    priority       VARCHAR(10),
    step_seq       INT,
    step_id        VARCHAR(10),
    tool_id        VARCHAR(20),
    chamber_id     VARCHAR(5),
    track_in_time  DATETIME,
    hours_in_step  DOUBLE,
    as_of          DATETIME
)
PRIMARY KEY (lot_id)
DISTRIBUTED BY HASH (lot_id)
PROPERTIES ("replication_num" = "1");

INSERT INTO wip_current
SELECT lot_id, priority, step_seq, step_id, tool_id, chamber_id, track_in_time, hours_in_step, as_of
FROM fab_lake.gold.wip_current;

-- run the same INSERT again: row count does NOT double, because the Primary Key model upserts
INSERT INTO wip_current
SELECT lot_id, priority, step_seq, step_id, tool_id, chamber_id, track_in_time, hours_in_step, as_of
FROM fab_lake.gold.wip_current;
SELECT COUNT(*) AS wip_lots_after_two_inserts FROM wip_current;

-- 2) DUPLICATE KEY model: append-only raw events, sorted by (event_time, tool_id) for fast range scans.
--    Expression partitioning creates one partition per day automatically.
CREATE TABLE IF NOT EXISTS tool_state_events_raw (
    event_time   DATETIME NOT NULL,
    tool_id      VARCHAR(20) NOT NULL,
    event_id     VARCHAR(20),
    e10_state    VARCHAR(20),
    reason_code  VARCHAR(30)
)
DUPLICATE KEY (event_time, tool_id)
PARTITION BY date_trunc('day', event_time)
DISTRIBUTED BY HASH (tool_id)
PROPERTIES ("replication_num" = "1");

TRUNCATE TABLE tool_state_events_raw;
INSERT INTO tool_state_events_raw
SELECT event_time, tool_id, event_id, e10_state, reason_code
FROM fab_lake.silver.tool_state_events;
SHOW PARTITIONS FROM tool_state_events_raw;

-- 3) AGGREGATE KEY model: StarRocks pre-aggregates on load (SUM / MAX per key).
CREATE TABLE IF NOT EXISTS spc_chamber_daily_agg (
    measure_date  DATE        NOT NULL,
    tool_id       VARCHAR(20) NOT NULL,
    chamber_id    VARCHAR(5)  NOT NULL,
    parameter     VARCHAR(30) NOT NULL,
    points        BIGINT SUM DEFAULT "0",
    ooc_points    BIGINT SUM DEFAULT "0",
    max_value     DOUBLE MAX
)
AGGREGATE KEY (measure_date, tool_id, chamber_id, parameter)
DISTRIBUTED BY HASH (tool_id)
PROPERTIES ("replication_num" = "1");

TRUNCATE TABLE spc_chamber_daily_agg;
INSERT INTO spc_chamber_daily_agg
SELECT CAST(measure_time AS DATE), tool_id, chamber_id, parameter, 1, ooc_flag, value
FROM fab_lake.silver.spc_points;   -- one row per point goes in, aggregated rows come out
SELECT COUNT(*) AS aggregated_rows FROM spc_chamber_daily_agg;

-- 4) ASYNC MATERIALIZED VIEW over the lake: precomputed dashboard table, refreshed on a schedule.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_chamber_ooc
DISTRIBUTED BY HASH (tool_id)
REFRESH ASYNC EVERY (INTERVAL 1 HOUR)
PROPERTIES ("replication_num" = "1")
AS
SELECT tool_id, chamber_id, parameter,
       SUM(points) AS points, SUM(ooc_points) AS ooc_points,
       ROUND(100.0 * SUM(ooc_points) / SUM(points), 2) AS ooc_pct,
       MAX(measure_date) AS last_date
FROM fab_lake.gold.spc_ooc_daily
GROUP BY tool_id, chamber_id, parameter;

REFRESH MATERIALIZED VIEW mv_chamber_ooc WITH SYNC MODE;
SELECT * FROM mv_chamber_ooc ORDER BY ooc_pct DESC LIMIT 5;
