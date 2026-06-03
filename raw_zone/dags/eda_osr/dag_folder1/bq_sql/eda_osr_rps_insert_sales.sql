-- DAG:  eda_osr_dag_folder1_dag1
-- Task: insert_sales
-- Target: {project1}.{key1}
-- Pattern: single-scan TEMP TABLE → clean insert + error log + audit log

CREATE TEMP TABLE staged AS
SELECT
  s.*,
  CASE
    WHEN s.sale_id     IS NULL                  THEN 'sale_id_missing'
    WHEN s.customer_id IS NULL                  THEN 'customer_id_missing'
    WHEN s.amount      IS NULL OR s.amount <= 0 THEN 'invalid_amount'
    WHEN s.sale_date   IS NULL                  THEN 'sale_date_missing'
    WHEN s.region      IS NULL                  THEN 'region_missing'
    ELSE NULL
  END AS _error_reason
FROM `{project1}.{staging_dataset}.{staging_sales_table}` s
WHERE DATE(s.created_at) = '{{ ds }}';

-- ── 1. Insert clean rows into target ──────────────────────────────────────
INSERT INTO `{project1}.{key1}`
  (sale_id, customer_id, amount, sale_date, region, created_at)
SELECT
  sale_id,
  customer_id,
  amount,
  sale_date,
  region,
  created_at
FROM staged
WHERE _error_reason IS NULL;

-- ── 2. Route bad rows to error log ────────────────────────────────────────
INSERT INTO `{project1}.{error_log_dataset}.{error_log_table}`
  (dag_id, task_id, dag_run_id, execution_date, inserted_at,
   source_table, row_data, error_reason)
SELECT
  '{{ dag.dag_id }}'                                        AS dag_id,
  'insert_sales'                                            AS task_id,
  '{{ run_id }}'                                            AS dag_run_id,
  DATE('{{ ds }}')                                          AS execution_date,
  CURRENT_TIMESTAMP()                                       AS inserted_at,
  '{staging_dataset}.{staging_sales_table}'                 AS source_table,
  TO_JSON_STRING(t)                                         AS row_data,
  _error_reason                                             AS error_reason
FROM staged t
WHERE _error_reason IS NOT NULL;

-- ── 3. Write audit counts ─────────────────────────────────────────────────
INSERT INTO `{project1}.{error_log_dataset}.{audit_log_table}`
  (dag_id, task_id, dag_run_id, execution_date, inserted_at,
   total_rows, clean_rows, error_rows, status)
SELECT
  '{{ dag.dag_id }}'                AS dag_id,
  'insert_sales'                    AS task_id,
  '{{ run_id }}'                    AS dag_run_id,
  DATE('{{ ds }}')                  AS execution_date,
  CURRENT_TIMESTAMP()               AS inserted_at,
  COUNT(*)                          AS total_rows,
  COUNTIF(_error_reason IS NULL)    AS clean_rows,
  COUNTIF(_error_reason IS NOT NULL) AS error_rows,
  'SUCCESS'                         AS status
FROM staged;
