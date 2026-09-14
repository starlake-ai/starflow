-- Canonical schema for the starlake run store held in starlake-api's PostgreSQL.
-- starlake-api owns these tables and their migrations: the CLI never issues DDL against them.
-- The typed columns are for the api to query. `payload` is the event exactly as the CLI wrote it
-- and is what the CLI reads back, so the two can never disagree about what happened.
CREATE TABLE IF NOT EXISTS sl_run (
  run_id         VARCHAR(64) PRIMARY KEY,
  created_at     TIMESTAMPTZ NOT NULL,
  schema_version INT         NOT NULL,
  fingerprint    VARCHAR(64) NOT NULL,
  selection      JSONB,
  options        JSONB,
  env            VARCHAR(255),
  correlation_id VARCHAR(255),
  status         VARCHAR(32) NOT NULL,
  exit_code      INT,
  finished_at    TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS sl_run_event (
  run_id          VARCHAR(64) NOT NULL REFERENCES sl_run(run_id),
  attempt         INT         NOT NULL,
  seq             INT         NOT NULL,
  ts              TIMESTAMPTZ NOT NULL,
  type            VARCHAR(32) NOT NULL,
  task_id         VARCHAR(512),
  task_name       VARCHAR(512),
  node_type       VARCHAR(32),
  duration_millis BIGINT,
  error_type      VARCHAR(255),
  message         TEXT,
  reason          VARCHAR(32),
  payload         JSONB       NOT NULL,
  PRIMARY KEY (run_id, attempt, seq)
);

CREATE INDEX IF NOT EXISTS sl_run_event_type_idx ON sl_run_event (run_id, type);
