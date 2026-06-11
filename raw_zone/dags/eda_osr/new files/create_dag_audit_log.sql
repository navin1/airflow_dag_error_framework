-- =============================================================================
-- dag_audit_log : Two rows per task (START + END), aggregated by views
-- Run ONCE before deploying. Replace your_project.your_dataset.
-- =============================================================================

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.dag_audit_log`
(
  dag_id              STRING    NOT NULL,
  task_id             STRING    NOT NULL,
  dag_run_id          STRING    NOT NULL,
  media_date          DATE,               -- business/data date being processed
  table_name          STRING,             -- impacted table(s); '\n' separated if multiple

  event               STRING    NOT NULL, -- 'START' | 'END'
  status              STRING    NOT NULL, -- 'RUNNING' | 'SUCCESS' | 'FAILED'

  start_time          TIMESTAMP,          -- populated on START row only
  end_time            TIMESTAMP,          -- populated on END row only
  task_duration_secs  FLOAT64,            -- populated on END row only

  -- Record counts (END row only). NULL = task did not report;
  -- views fall back to dag_error_log for failed counts.
  records_processed   INT64,              -- total records the task attempted
  records_success     INT64,              -- records successfully processed/inserted
  records_failed      INT64,              -- records that failed (detail in dag_error_log)

  -- Failure details (END row only, when status = 'FAILED')
  error_type          STRING,
  error_message       STRING,
  traceback           STRING,

  inserted_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(inserted_at)
CLUSTER BY dag_id, dag_run_id, task_id
OPTIONS (
  description = 'Task-level audit trail: one START and one END row per task attempt. Joined/aggregated by vw_task_audit and vw_dag_run_audit.'
);

-- ---------------------------------------------------------------------------
-- If dag_audit_log ALREADY exists, just add the new column:
-- ---------------------------------------------------------------------------
-- ALTER TABLE `your_project.your_dataset.dag_audit_log` ADD COLUMN media_date DATE;