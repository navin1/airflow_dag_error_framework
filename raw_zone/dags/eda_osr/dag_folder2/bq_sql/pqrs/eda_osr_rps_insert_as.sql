-- DAG:  eda_osr_dag_folder2_dag2
-- Task: insert_as
-- Target: {project1}.{as_table}
-- SQL sub-folder: pqrs/
-- Pattern: single-scan TEMP TABLE → clean insert + error log + audit log

CREATE TEMP TABLE staged AS
SELECT
  s.*,
  CASE
    WHEN s.as_id       IS NULL                        THEN 'as_id_missing'
    WHEN s.entity_id   IS NULL                        THEN 'entity_id_missing'
    WHEN s.status_code IS NULL OR s.status_code = ''  THEN 'status_code_missing'
    WHEN s.effective_dt IS NULL                       THEN 'effective_dt_missing'
    WHEN s.created_at   IS NULL                       THEN 'created_at_missing'
    ELSE NULL
  END AS _error_reason
FROM `{project1}.{staging_dataset}.{staging_as_table}` s
WHERE DATE(s.created_at) = '{{ ds }}';

-- ── 1. Insert clean rows into target ──────────────────────────────────────
INSERT INTO `{project1}.{as_table}`
  (as_id, entity_id, status_code, effective_dt, created_at)
SELECT
  as_id,
  entity_id,
  status_code,
  effective_dt,
  created_at
FROM staged
WHERE _error_reason IS NULL;

-- ── 2. Route bad rows to error log ────────────────────────────────────────
INSERT INTO `{project1}.{error_log_dataset}.{error_log_table}`
  (dag_id, task_id, dag_run_id, execution_date, inserted_at,
   source_table, row_data, error_reason)
SELECT
  '{{ dag.dag_id }}'                                        AS dag_id,
  'insert_as'                                               AS task_id,
  '{{ run_id }}'                                            AS dag_run_id,
  DATE('{{ ds }}')                                          AS execution_date,
  CURRENT_TIMESTAMP()                                       AS inserted_at,
  '{staging_dataset}.{staging_as_table}'                    AS source_table,
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
  'insert_as'                        AS task_id,
  '{{ run_id }}'                     AS dag_run_id,
  DATE('{{ ds }}')                   AS execution_date,
  CURRENT_TIMESTAMP()                AS inserted_at,
  COUNT(*)                           AS total_rows,
  COUNTIF(_error_reason IS NULL)     AS clean_rows,
  COUNTIF(_error_reason IS NOT NULL) AS error_rows,
  'SUCCESS'                          AS status
FROM staged;
