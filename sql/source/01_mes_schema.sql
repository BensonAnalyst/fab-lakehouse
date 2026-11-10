-- Source system: plays the role of the fab's MES / SPC / equipment-automation databases
-- (at SSMC this would typically be Oracle). Runs automatically when the Postgres container starts.

CREATE SCHEMA IF NOT EXISTS mes;

CREATE TABLE IF NOT EXISTS mes.tools (
    tool_id      VARCHAR(20) NOT NULL,
    chamber_id   VARCHAR(5)  NOT NULL,
    module       VARCHAR(20) NOT NULL,
    bay          VARCHAR(20),
    status       VARCHAR(20),
    updated_at   TIMESTAMP   NOT NULL,
    PRIMARY KEY (tool_id, chamber_id)
);

-- Current state only: the MES overwrites the version when engineering releases a new one.
-- History is NOT kept here -> the lakehouse must build it (SCD Type 2).
CREATE TABLE IF NOT EXISTS mes.recipe_master (
    recipe_id       VARCHAR(40) PRIMARY KEY,
    step_id         VARCHAR(10) NOT NULL,
    tool_id         VARCHAR(20) NOT NULL,
    recipe_version  INT         NOT NULL,
    updated_at      TIMESTAMP   NOT NULL
);

CREATE TABLE IF NOT EXISTS mes.lots (
    lot_id      VARCHAR(20) PRIMARY KEY,
    product_id  VARCHAR(30),
    priority    VARCHAR(10),
    wafer_qty   INT,
    lot_status  VARCHAR(15),
    start_time  TIMESTAMP,
    updated_at  TIMESTAMP NOT NULL
);

-- Lot history. Deliberately no unique key on (lot_id, step_seq): real MES history tables
-- can double-log, and the pipeline has to deal with it.
CREATE TABLE IF NOT EXISTS mes.lot_steps (
    txn_id          BIGINT PRIMARY KEY,
    lot_id          VARCHAR(20),
    step_seq        INT,
    step_id         VARCHAR(10),
    tool_id         VARCHAR(20),
    chamber_id      VARCHAR(5),
    recipe_id       VARCHAR(40),
    wafer_qty       INT,
    track_in_time   TIMESTAMP,
    track_out_time  TIMESTAMP,
    updated_at      TIMESTAMP NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_lot_steps_upd ON mes.lot_steps (updated_at);

CREATE TABLE IF NOT EXISTS mes.spc_points (
    point_id             VARCHAR(40) PRIMARY KEY,
    lot_id               VARCHAR(20),
    step_id              VARCHAR(10),
    parameter            VARCHAR(30),
    tool_id              VARCHAR(20),
    chamber_id           VARCHAR(5),
    measure_tool_prefix  VARCHAR(10),
    site                 INT,
    value                NUMERIC(12,3),
    target               NUMERIC(12,3),
    ucl                  NUMERIC(12,3),
    lcl                  NUMERIC(12,3),
    ooc_flag             SMALLINT,
    measure_time         TIMESTAMP,
    updated_at           TIMESTAMP NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_spc_upd ON mes.spc_points (updated_at);

CREATE TABLE IF NOT EXISTS mes.tool_state_events (
    event_id     VARCHAR(20) PRIMARY KEY,
    tool_id      VARCHAR(20) NOT NULL,
    e10_state    VARCHAR(20) NOT NULL,
    reason_code  VARCHAR(30),
    event_time   TIMESTAMP   NOT NULL,
    updated_at   TIMESTAMP   NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_tse_upd ON mes.tool_state_events (updated_at);
