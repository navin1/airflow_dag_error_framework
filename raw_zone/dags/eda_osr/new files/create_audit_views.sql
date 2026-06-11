-- =============================================================================
-- Audit views : run AFTER both tables exist.
--   vw_task_audit     -> one row per task attempt (START + END joined)
--   vw_dag_run_audit  -> one row per DAG run (aggregated)
--   vw_audit_reconciliation -> flags count mismatches between audit & error log
-- Replace your_project.your_dataset.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. TASK-LEVEL VIEW : join START and END rows
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW `your_project.your_dataset.vw_task_audit` AS
WITH starts AS (
  SELECT dag_id, task_id, dag_run_id, media_date, table_name, start_time
  FROM `your_project.your_dataset.dag_audit_log`
  WHERE event = 'START'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY dag_id, dag_run_id, task_id ORDER BY inserted_at DESC) = 1
),
ends AS (
  SELECT dag_id, task_id, dag_run_id, media_date, status, end_time, task_duration_secs,
         records_processed, records_success, records_failed,
         error_type, error_message, traceback
  FROM `your_project.your_dataset.dag_audit_log`
  WHERE event = 'END'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY dag_id, dag_run_id, task_id ORDER BY inserted_at DESC) = 1
),
-- Failed-record counts derived from the error log (source of truth fallback)
err AS (
  SELECT dag_id, task_id, dag_run_id, COUNT(*) AS error_log_failed_cnt
  FROM `your_project.your_dataset.dag_error_log`
  GROUP BY dag_id, task_id, dag_run_id
)
SELECT
  s.dag_id,
  s.dag_run_id,
  -- END row wins (task may compute the real media_date mid-run), START is fallback
  COALESCE(e.media_date, s.media_date)                   AS media_date,
  s.task_id,
  s.table_name,
  COALESCE(e.status, 'RUNNING')                          AS status,
  s.start_time,
  e.end_time,
  COALESCE(e.task_duration_secs,
           TIMESTAMP_DIFF(e.end_time, s.start_time, MILLISECOND) / 1000.0)
                                                         AS task_duration_secs,
  e.records_processed,
  e.records_success,
  -- If the task did not report a failed count, derive it from the error log
  COALESCE(e.records_failed, err.error_log_failed_cnt, 0) AS records_failed,
  err.error_log_failed_cnt,
  e.error_type,
  e.error_message,
  e.traceback
FROM starts s
LEFT JOIN ends e
  ON  s.dag_id     = e.dag_id
  AND s.dag_run_id = e.dag_run_id
  AND s.task_id    = e.task_id
LEFT JOIN err
  ON  s.dag_id     = err.dag_id
  AND s.dag_run_id = err.dag_run_id
  AND s.task_id    = err.task_id;


-- ---------------------------------------------------------------------------
-- 2. DAG-LEVEL VIEW : one row per dag_run_id
--    impacted_tables = ALL tables across ALL tasks (comma joined)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW `your_project.your_dataset.vw_dag_run_audit` AS
SELECT
  dag_id,
  dag_run_id,
  -- Business/data date the run processed. MAX handles the rare case where
  -- tasks within one run report different dates (latest wins); use
  -- STRING_AGG(DISTINCT CAST(media_date AS STRING), ', ') instead if you
  -- want to see all of them.
  MAX(media_date)                                        AS media_date,
  CASE
    WHEN COUNTIF(status = 'FAILED')  > 0 THEN 'FAILED'
    WHEN COUNTIF(status = 'RUNNING') > 0 THEN 'RUNNING'
    ELSE 'SUCCESS'
  END                                                    AS dag_status,
  MIN(start_time)                                        AS dag_start_time,
  MAX(end_time)                                          AS dag_end_time,
  ROUND(TIMESTAMP_DIFF(MAX(end_time), MIN(start_time), MILLISECOND) / 60000.0, 2)
                                                         AS total_runtime_mins,
  COUNT(*)                                               AS total_tasks,
  COUNTIF(status = 'SUCCESS')                            AS tasks_success,
  COUNTIF(status = 'FAILED')                             AS tasks_failed,
  COUNTIF(status = 'RUNNING')                            AS tasks_running,

  -- Record-count rollup across the whole run
  SUM(records_processed)                                 AS total_records_processed,
  SUM(records_success)                                   AS total_records_success,
  SUM(records_failed)                                    AS total_records_failed,

  -- All impacted tables across all tasks, comma joined (tables stored
  -- '\n'-separated per task are split first, then deduplicated)
  (SELECT STRING_AGG(DISTINCT t, ', ' ORDER BY t)
   FROM UNNEST(SPLIT(STRING_AGG(table_name, '\n'), '\n')) AS t
   WHERE t != '')                                        AS impacted_tables,

  STRING_AGG(IF(status = 'FAILED', task_id, NULL), ', ') AS failed_tasks
FROM `your_project.your_dataset.vw_task_audit`
GROUP BY dag_id, dag_run_id;

-- ---------------------------------------------------------------------------
-- ALTERNATIVE: if media_date should come from eda_osr_rps_process_log instead
-- of being captured in dag_audit_log, replace MAX(media_date) above with a
-- post-aggregation join, e.g.:
--
--   CREATE OR REPLACE VIEW `your_project.your_dataset.vw_dag_run_audit` AS
--   WITH agg AS ( ...the SELECT above without media_date... )
--   SELECT agg.*, pl.media_date
--   FROM agg
--   LEFT JOIN (
--     SELECT dag_run_id, MAX(media_date) AS media_date
--     FROM `your_project.your_dataset.eda_osr_rps_process_log`
--     GROUP BY dag_run_id
--   ) pl USING (dag_run_id);
-- ---------------------------------------------------------------------------


-- ---------------------------------------------------------------------------
-- 3. RECONCILIATION VIEW : audit counts vs error-log counts must match
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW `your_project.your_dataset.vw_audit_reconciliation` AS
SELECT
  dag_id,
  dag_run_id,
  task_id,
  records_processed,
  records_success,
  records_failed,
  error_log_failed_cnt,
  -- processed should equal success + failed when the task reported counts
  (records_processed IS NOT NULL
     AND records_processed != COALESCE(records_success, 0) + COALESCE(records_failed, 0))
                                                         AS count_math_mismatch,
  -- audit failed count should match rows actually written to the error log
  (records_failed IS NOT NULL
     AND records_failed != COALESCE(error_log_failed_cnt, 0))
                                                         AS error_log_mismatch
FROM `your_project.your_dataset.vw_task_audit`
WHERE
  (records_processed IS NOT NULL
     AND records_processed != COALESCE(records_success, 0) + COALESCE(records_failed, 0))
  OR
  (records_failed IS NOT NULL
     AND records_failed != COALESCE(error_log_failed_cnt, 0));