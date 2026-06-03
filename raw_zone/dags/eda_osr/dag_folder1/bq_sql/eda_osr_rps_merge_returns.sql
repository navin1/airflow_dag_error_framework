-- DAG:  eda_osr_dag_folder1_dag1
-- Task: merge_returns
-- Target: {project1}.{returns_table}
-- Pattern: single-scan TEMP TABLE → MERGE (upsert) + error log + audit log

CREATE TEMP TABLE staged AS
SELECT
  r.*,
  CASE
    WHEN r.return_id   IS NULL                        THEN 'return_id_missing'
    WHEN r.sale_id     IS NULL                        THEN 'sale_id_missing'
    WHEN r.return_date IS NULL                        THEN 'return_date_missing'
    WHEN r.reason_code IS NULL OR r.reason_code = ''  THEN 'reason_code_missing'
    WHEN r.refund_amt  IS NULL OR r.refund_amt < 0    THEN 'invalid_refund_amount'
    ELSE NULL
  END AS _error_reason
FROM `{project1}.{staging_dataset}.{staging_returns_table}` r
WHERE DATE(r.loaded_at) = '{{ ds }}';

-- ── 1. Upsert clean rows into target ──────────────────────────────────────
MERGE `{project1}.{returns_table}` AS tgt
USING (
  SELECT return_id, sale_id, return_date, reason_code, refund_amt, loaded_at
  FROM staged
  WHERE _error_reason IS NULL
) AS src
ON tgt.return_id = src.return_id
WHEN MATCHED THEN
  UPDATE SET
    sale_id     = src.sale_id,
    return_date = src.return_date,
    reason_code = src.reason_code,
    refund_amt  = src.refund_amt,
    loaded_at   = src.loaded_at
WHEN NOT MATCHED THEN
  INSERT (return_id, sale_id, return_date, reason_code, refund_amt, loaded_at)
  VALUES (src.return_id, src.sale_id, src.return_date, src.reason_code, src.refund_amt, src.loaded_at);

-- ── 2. Route bad rows to error log ────────────────────────────────────────
INSERT INTO `{project1}.{error_log_dataset}.{error_log_table}`
  (dag_id, task_id, dag_run_id, execution_date, inserted_at,
   source_table, row_data, error_reason)
SELECT
  '{{ dag.dag_id }}'                                        AS dag_id,
  'merge_returns'                                           AS task_id,
  '{{ run_id }}'                                            AS dag_run_id,
  DATE('{{ ds }}')                                          AS execution_date,
  CURRENT_TIMESTAMP()                                       AS inserted_at,
  '{staging_dataset}.{staging_returns_table}'               AS source_table,
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
  'merge_returns'                    AS task_id,
  '{{ run_id }}'                     AS dag_run_id,
  DATE('{{ ds }}')                   AS execution_date,
  CURRENT_TIMESTAMP()                AS inserted_at,
  COUNT(*)                           AS total_rows,
  COUNTIF(_error_reason IS NULL)     AS clean_rows,
  COUNTIF(_error_reason IS NOT NULL) AS error_rows,
  'SUCCESS'                          AS status
FROM staged;
