-- DAG:  eda_osr_dag_folder2_dag1
-- Task: insert_aas
-- Target: {project1}.{aas_table}
-- SQL sub-folder: abcd/
-- Pattern: single-scan TEMP TABLE → clean insert + error log + audit log

CREATE TEMP TABLE staged AS
SELECT
  a.*,
  CASE
    WHEN a.aas_id      IS NULL                        THEN 'aas_id_missing'
    WHEN a.account_id  IS NULL                        THEN 'account_id_missing'
    WHEN a.segment     IS NULL OR a.segment = ''      THEN 'segment_missing'
    WHEN a.score       IS NULL OR a.score < 0         THEN 'invalid_score'
    WHEN a.scored_at   IS NULL                        THEN 'scored_at_missing'
    ELSE NULL
  END AS _error_reason
FROM `{project1}.{staging_dataset}.{staging_aas_table}` a
WHERE DATE(a.scored_at) = '{{ ds }}';

-- ── 1. Insert clean rows into target ──────────────────────────────────────
INSERT INTO `{project1}.{aas_table}`
  (aas_id, account_id, segment, score, scored_at)
SELECT
  aas_id,
  account_id,
  segment,
  score,
  scored_at
FROM staged
WHERE _error_reason IS NULL;

-- ── 2. Route bad rows to error log ────────────────────────────────────────
INSERT INTO `{project1}.{error_log_dataset}.{error_log_table}`
  (dag_id, task_id, dag_run_id, execution_date, inserted_at,
   source_table, row_data, error_reason)
SELECT
  '{{ dag.dag_id }}'                                        AS dag_id,
  'insert_aas'                                              AS task_id,
  '{{ run_id }}'                                            AS dag_run_id,
  DATE('{{ ds }}')                                          AS execution_date,
  CURRENT_TIMESTAMP()                                       AS inserted_at,
  '{staging_dataset}.{staging_aas_table}'                   AS source_table,
  TO_JSON_STRING(t)                                         AS row_data,
  _error_reason                                             AS error_reason
FROM staged t
WHERE _error_reason IS NOT NULL;

-- ── 3. Write audit counts ─────────────────────────────────────────────────
INSERT INTO `{project1}.{error_log_dataset}.{audit_log_table}`
  (dag_id, task_id, dag_run_id, execution_date, inserted_at,
   total_rows, clean_rows, error_rows, status)
SELECT
  '{{ dag.dag_id }}'                 AS dag_id,
  'insert_aas'                       AS task_id,
  '{{ run_id }}'                     AS dag_run_id,
  DATE('{{ ds }}')                   AS execution_date,
  CURRENT_TIMESTAMP()                AS inserted_at,
  COUNT(*)                           AS total_rows,
  COUNTIF(_error_reason IS NULL)     AS clean_rows,
  COUNTIF(_error_reason IS NOT NULL) AS error_rows,
  'SUCCESS'                          AS status
FROM staged;
