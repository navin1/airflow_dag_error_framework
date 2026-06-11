─────────────────────────────────────────────────────────────────────────────

I am adding a task-level AUDIT logging mechanism to my existing Airflow DAG
error framework. The audit log COEXISTS with the current error log:

  - dag_audit_log  (NEW)      -> 2 rows per task (START + END): status, timing,
                                 media_date, records_processed / records_success
                                 / records_failed
  - dag_error_log  (EXISTING) -> 1 row per failed record, full record as JSON

A reference implementation is provided in the folder `audit_framework/`
(files: utils/audit_logger.py, utils/dag_patch.py,
sql/setup/create_dag_audit_log.sql, sql/setup/create_dag_error_log.sql,
sql/setup/create_audit_views.sql, README.md). Use these files as the source of
truth for logic; adapt paths/imports to my repo.

## My repo context
- Repo: airflow_dag_error_framework
- DAGs live under raw-zone/dags/eda_osr/ (and similar module folders)
- Shared utilities live in utils/ (dag_patch.py, email_notifier.py,
  airflow_config.py with render_template() and resolve(), lru_cache'd
  get_config()) — some config also under common/config/
- Every DAG already calls: patch_dag(dag, TASKS, config=paramf)
- TASKS entries are (task_suffix, table_template) where table_template is a
  string OR a list of strings, resolved against merged global+project config
- Existing on_failure_callback sends failure emails — it MUST keep working
- BigQuery project.dataset for log tables: <PROJECT.DATASET>
- A process log table eda_osr_rps_process_log captures media_date; the audit
  log must capture media_date too (same YYYY-MM-DD convention)

## Tasks — do these in order

1. ADD utils/audit_logger.py from the reference folder. Then:
   - Set AUDIT_TABLE = "<PROJECT.DATASET>.dag_audit_log"
   - Set ERROR_TABLE = "<PROJECT.DATASET>.dag_error_log"
   - Verify the BigQuery client import works in our environment
     (google-cloud-bigquery is already a Composer dependency).

2. REPLACE utils/dag_patch.py with the reference version, but MERGE carefully:
   - Keep our existing table-template resolution behavior exactly: supports a
     single template or a list (list resolves each and joins with "\n"), uses
     render_template(template, config) when config is passed, else resolve().
   - Fix the import line `from utils.airflow_config import render_template,
     resolve` to match our actual module path if different.
   - The new patch_dag must attach to EVERY task:
       a) pre_execute  -> audit_logger.log_task_start (chain any existing
          pre_execute, handling both bound methods and plain functions)
       b) on_success_callback -> log_task_end(status="SUCCESS") APPENDED to
          any existing success callbacks (normalize single callback vs list)
       c) on_failure_callback -> log_task_end(status="FAILED") APPENDED to
          existing failure callbacks (the email notifier must still fire)
   - Task matching: exact task_id OR task_id.endswith(suffix), as today.

3. DO NOT modify any DAG files or task .sql files. patch_dag wiring covers
   everything. Confirm by grepping that all DAGs call patch_dag.

4. ADD the three SQL setup scripts under sql/setup/ with <PROJECT.DATASET>
   substituted:
   - create_dag_audit_log.sql   (table partitioned by DATE(inserted_at),
     clustered by dag_id, dag_run_id, task_id; includes media_date DATE)
   - create_dag_error_log.sql   (dag_error_log already EXISTS in BQ — keep the
     CREATE IF NOT EXISTS but list the ALTER statements I must run instead:
     ADD COLUMN dag_run_id STRING, failed_record JSON, record_key_1 STRING,
     record_key_2 STRING — only the ones missing)
   - create_audit_views.sql     (vw_task_audit joins START/END rows with
     QUALIFY dedup and falls back to dag_error_log counts for records_failed;
     vw_dag_run_audit aggregates per dag_run_id with dag_status logic
     [any FAILED -> FAILED, any RUNNING -> RUNNING, else SUCCESS],
     MAX(media_date), summed counts, impacted_tables = all tables across all
     tasks split on "\n", deduplicated, comma joined, and failed_tasks list;
     vw_audit_reconciliation flags processed != success+failed or
     records_failed != error-log row count)

5. media_date resolution in audit_logger must follow this priority:
   XCom key 'audit_media_date' -> dag_run.conf['media_date'] ->
   logical_date as YYYY-MM-DD. Written on BOTH START and END rows.

6. Record-count sources for the END row, in priority order:
   - XComs audit_records_processed / _success / _failed (pushed by
     push_record_counts helper)
   - else BigQueryInsertJobOperator fallback: pull the job_id XCom, fetch the
     job, use num_dml_affected_rows (processed = success = affected, failed=0)
   - else leave NULL (views derive failed count from dag_error_log)

7. SAFETY REQUIREMENTS (must hold everywhere):
   - Audit/error inserts are wrapped in try/except and logged — a logging
     failure must NEVER fail or mask a task.
   - insert_rows_json is used (streaming insert); no DML in callbacks.
   - error_message truncated to 4000 chars, traceback to 8000.

8. VERIFY:
   - python -m py_compile on both utils files
   - Parse one DAG locally (python the DAG file) to confirm no import errors
   - Print the deployment checklist for me:
     (a) run the 3 SQL scripts in BQ in order (table DDLs first, views last;
         for dag_error_log run only the missing ALTERs),
     (b) confirm the google_cloud_default service account has insert rights
         on <PROJECT.DATASET>,
     (c) deploy utils/ to the Composer DAGs bucket,
     (d) trigger one DAG and check:
         SELECT * FROM <PROJECT.DATASET>.vw_dag_run_audit
         ORDER BY dag_start_time DESC LIMIT 5;
         and confirm vw_audit_reconciliation returns 0 rows.

Do NOT invent additional features. Ask me before changing anything in
email_notifier.py or airflow_config.py.