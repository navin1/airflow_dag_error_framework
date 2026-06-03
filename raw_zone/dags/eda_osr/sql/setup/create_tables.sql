-- Run once per environment to create the BQ infrastructure tables.
-- Substitute {project1} and {error_log_dataset} before executing,
-- or use the config_loader to render this file first.
--
-- Both tables are:
--   • Partitioned by DATE(inserted_at)   → cost-efficient partition pruning
--   • Clustered by (dag_id, task_id)     → fast per-pipeline queries

-- ── dag_error_log ─────────────────────────────────────────────────────────
-- One row per offending source row, written inline by each BigQuery SQL task.
CREATE TABLE IF NOT EXISTS `{project1}.{error_log_dataset}.dag_error_log`
(
  dag_id         STRING    NOT NULL OPTIONS(description = 'Airflow DAG identifier'),
  task_id        STRING    NOT NULL OPTIONS(description = 'Airflow task identifier'),
  dag_run_id     STRING    NOT NULL OPTIONS(description = 'Airflow DAG run identifier'),
  execution_date DATE      NOT NULL OPTIONS(description = 'Logical execution date ({{ ds }})'),
  inserted_at    TIMESTAMP NOT NULL OPTIONS(description = 'Wall-clock insert time; partition key'),
  source_table   STRING             OPTIONS(description = 'Staging table the row originated from'),
  row_data       STRING             OPTIONS(description = 'TO_JSON_STRING of the offending row'),
  error_reason   STRING    NOT NULL OPTIONS(description = 'Short classification code from CASE block')
)
PARTITION BY DATE(inserted_at)
CLUSTER BY dag_id, task_id
OPTIONS(
  partition_expiration_days = 365,
  require_partition_filter  = false,
  description = 'Per-row data-quality error log written by EDA OSR DAG tasks'
);


-- ── dag_audit_log ─────────────────────────────────────────────────────────
-- One row per task run, summarising how many rows were clean vs. errored.
CREATE TABLE IF NOT EXISTS `{project1}.{error_log_dataset}.dag_audit_log`
(
  dag_id         STRING    NOT NULL OPTIONS(description = 'Airflow DAG identifier'),
  task_id        STRING    NOT NULL OPTIONS(description = 'Airflow task identifier'),
  dag_run_id     STRING    NOT NULL OPTIONS(description = 'Airflow DAG run identifier'),
  execution_date DATE      NOT NULL OPTIONS(description = 'Logical execution date ({{ ds }})'),
  inserted_at    TIMESTAMP NOT NULL OPTIONS(description = 'Wall-clock insert time; partition key'),
  total_rows     INT64     NOT NULL OPTIONS(description = 'Total rows scanned in staging'),
  clean_rows     INT64     NOT NULL OPTIONS(description = 'Rows successfully inserted into target'),
  error_rows     INT64     NOT NULL OPTIONS(description = 'Rows routed to dag_error_log'),
  status         STRING    NOT NULL OPTIONS(description = 'SUCCESS or FAILED: <message>')
)
PARTITION BY DATE(inserted_at)
CLUSTER BY dag_id, task_id
OPTIONS(
  partition_expiration_days = 730,
  require_partition_filter  = false,
  description = 'Per-task audit counts written at the end of each EDA OSR DAG task'
);
