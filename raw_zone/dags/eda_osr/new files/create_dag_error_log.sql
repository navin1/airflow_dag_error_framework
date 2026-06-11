-- =============================================================================
-- dag_error_log : Row-level error detail. Every failed record is captured here
-- with the full record as JSON. Coexists with dag_audit_log (counts only).
--
-- If the table already exists from the earlier framework, run ONLY the
-- ALTER statements at the bottom to bring it up to date.
-- =============================================================================

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.dag_error_log`
(
  dag_id          STRING    NOT NULL,
  task_id         STRING    NOT NULL,
  dag_run_id      STRING    NOT NULL,
  table_name      STRING,             -- target table the record was bound for
  execution_date  TIMESTAMP,

  error_type      STRING,             -- e.g. ValidationError, BQInsertError
  error_message   STRING,
  failed_record   JSON,               -- the FULL failed record, queryable

  -- Optional convenience columns: key business fields extracted from the
  -- record at write time so analysts can filter without JSON functions.
  record_key_1    STRING,             -- e.g. txn id
  record_key_2    STRING,             -- e.g. reg / store

  inserted_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(inserted_at)
CLUSTER BY dag_id, dag_run_id, task_id
OPTIONS (
  description = 'Row-level failed records (full record stored as JSON). Counts roll up into dag_audit_log / vw_task_audit.'
);

-- ---------------------------------------------------------------------------
-- If the table ALREADY exists, only run what is missing:
-- ---------------------------------------------------------------------------
-- ALTER TABLE `your_project.your_dataset.dag_error_log` ADD COLUMN dag_run_id STRING;
-- ALTER TABLE `your_project.your_dataset.dag_error_log` ADD COLUMN failed_record JSON;
-- ALTER TABLE `your_project.your_dataset.dag_error_log` ADD COLUMN record_key_1 STRING;
-- ALTER TABLE `your_project.your_dataset.dag_error_log` ADD COLUMN record_key_2 STRING;