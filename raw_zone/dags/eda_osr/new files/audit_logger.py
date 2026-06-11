"""
utils/audit_logger.py
─────────────────────
Writes the two-row-per-task audit trail to BigQuery (dag_audit_log) and the
row-level failed records to dag_error_log.

Both tables COEXIST:
    dag_audit_log  -> counts + timing + status   (this module, START/END rows)
    dag_error_log  -> full failed records as JSON (log_failed_records helper)

How record counts flow into the END row
---------------------------------------
1. PythonOperator tasks call `push_record_counts(context, processed, success,
   failed)` at the end of their callable. The END callback reads these XComs.
2. BigQueryInsertJobOperator tasks: if no XCom counts exist, the END callback
   falls back to the BQ job's num_dml_affected_rows (success = processed).
3. If neither exists, counts stay NULL and vw_task_audit derives the failed
   count from dag_error_log.

Nothing in this module ever raises into the task — audit failures are logged
and swallowed so they never mask the real task error.
"""

from __future__ import annotations

import json
import logging
import traceback as tb
from datetime import datetime, timezone

log = logging.getLogger(__name__)

# ── CONFIG — adjust to your environment ─────────────────────────────────────
AUDIT_TABLE = "your_project.your_dataset.dag_audit_log"
ERROR_TABLE = "your_project.your_dataset.dag_error_log"

# XCom keys used to hand record counts from the task to the END callback
XCOM_PROCESSED = "audit_records_processed"
XCOM_SUCCESS   = "audit_records_success"
XCOM_FAILED    = "audit_records_failed"
XCOM_MEDIA_DT  = "audit_media_date"

_bq_client = None


def _bq():
    """Lazy, cached BigQuery client (created once per worker process)."""
    global _bq_client
    if _bq_client is None:
        from google.cloud import bigquery
        _bq_client = bigquery.Client()
    return _bq_client


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _insert(table: str, rows: list[dict]) -> None:
    """Insert rows; never let audit failures break the task."""
    try:
        errors = _bq().insert_rows_json(table, rows)
        if errors:
            log.error("Audit insert errors for %s: %s", table, errors)
    except Exception:
        log.exception("Audit insert failed for %s (swallowed)", table)


# ─────────────────────────────────────────────────────────────────────────────
# Public helper for tasks: report counts via XCom
# ─────────────────────────────────────────────────────────────────────────────
def push_record_counts(context: dict, processed: int, success: int, failed: int,
                       media_date: str | None = None) -> None:
    """Call from inside a PythonOperator callable to report record counts.

    media_date: optional 'YYYY-MM-DD' string (or date object) — the business
    date the task processed. Overrides dag_run.conf / logical_date fallback.

    Example:
        def load(**context):
            good, bad = validate_and_split(rows)
            insert(good)
            log_failed_records(context, bad, table_name=TARGET)
            push_record_counts(context, len(rows), len(good), len(bad),
                               media_date=str(batch_media_date))
    """
    ti = context["ti"]
    ti.xcom_push(key=XCOM_PROCESSED, value=int(processed))
    ti.xcom_push(key=XCOM_SUCCESS,   value=int(success))
    ti.xcom_push(key=XCOM_FAILED,    value=int(failed))
    if media_date is not None:
        ti.xcom_push(key=XCOM_MEDIA_DT, value=str(media_date))


def _get_media_date(context: dict) -> str | None:
    """Resolve media_date as 'YYYY-MM-DD' (same convention as
    eda_osr_rps_process_log).  Priority:
       1. XCom 'audit_media_date' pushed by the task
       2. dag_run.conf['media_date']
       3. the run's logical_date (typical data date for daily pipelines)
    """
    ti = context["ti"]
    try:
        val = ti.xcom_pull(task_ids=ti.task_id, key=XCOM_MEDIA_DT)
        if val:
            return str(val)[:10]
    except Exception:
        pass
    try:
        dag_run = context.get("dag_run")
        conf = getattr(dag_run, "conf", None) or {}
        if conf.get("media_date"):
            return str(conf["media_date"])[:10]
    except Exception:
        pass
    try:
        ld = context.get("logical_date") or context.get("execution_date")
        if ld:
            return ld.strftime("%Y-%m-%d")
    except Exception:
        pass
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Error log: capture the actual failed records (full JSON)
# ─────────────────────────────────────────────────────────────────────────────
def log_failed_records(
    context: dict,
    failed_records: list[dict],
    table_name: str = "",
    error_type: str = "ValidationError",
    error_message: str = "",
    key_field_1: str | None = None,
    key_field_2: str | None = None,
) -> None:
    """Write every failed record to dag_error_log with the full record as JSON.

    key_field_1/2: optional field names inside each record to copy into the
    convenience columns record_key_1/record_key_2 (e.g. 'txn', 'reg').
    """
    if not failed_records:
        return
    ti = context["ti"]
    rows = [
        {
            "dag_id":         ti.dag_id,
            "task_id":        ti.task_id,
            "dag_run_id":     context["run_id"],
            "table_name":     table_name,
            "execution_date": context["logical_date"].isoformat()
                              if context.get("logical_date") else _now_iso(),
            "error_type":     error_type,
            "error_message":  error_message or rec.get("_error", ""),
            "failed_record":  json.dumps(rec, default=str),
            "record_key_1":   str(rec.get(key_field_1, "")) if key_field_1 else None,
            "record_key_2":   str(rec.get(key_field_2, "")) if key_field_2 else None,
            "inserted_at":    _now_iso(),
        }
        for rec in failed_records
    ]
    _insert(ERROR_TABLE, rows)


# ─────────────────────────────────────────────────────────────────────────────
# Internal: pull counts for the END row
# ─────────────────────────────────────────────────────────────────────────────
def _get_counts(context: dict) -> tuple[int | None, int | None, int | None]:
    ti = context["ti"]

    # 1. Counts explicitly reported by the task
    processed = ti.xcom_pull(task_ids=ti.task_id, key=XCOM_PROCESSED)
    success   = ti.xcom_pull(task_ids=ti.task_id, key=XCOM_SUCCESS)
    failed    = ti.xcom_pull(task_ids=ti.task_id, key=XCOM_FAILED)
    if processed is not None:
        return processed, success, failed

    # 2. Fallback for BigQueryInsertJobOperator: read job statistics
    try:
        job_id = ti.xcom_pull(task_ids=ti.task_id, key="job_id") \
              or ti.xcom_pull(task_ids=ti.task_id)  # operator's return value
        if isinstance(job_id, str) and job_id:
            job = _bq().get_job(job_id)
            affected = getattr(job, "num_dml_affected_rows", None)
            if affected is not None:
                return int(affected), int(affected), 0
    except Exception:
        log.debug("BQ job-stats fallback unavailable", exc_info=True)

    # 3. Unknown — leave NULL; vw_task_audit derives failed cnt from error log
    return None, None, None


# ─────────────────────────────────────────────────────────────────────────────
# START / END row writers (wired up by dag_patch.py)
# ─────────────────────────────────────────────────────────────────────────────
def log_task_start(context: dict, table_name: str = "") -> None:
    """Insert the START row. Attached via pre_execute."""
    ti = context["ti"]
    _insert(AUDIT_TABLE, [{
        "dag_id":      ti.dag_id,
        "task_id":     ti.task_id,
        "dag_run_id":  context["run_id"],
        "media_date":  _get_media_date(context),
        "table_name":  table_name,
        "event":       "START",
        "status":      "RUNNING",
        "start_time":  _now_iso(),
        "inserted_at": _now_iso(),
    }])


def log_task_end(context: dict, status: str, table_name: str = "") -> None:
    """Insert the END row. Attached via on_success/on_failure callbacks."""
    ti = context["ti"]
    processed, success, failed = _get_counts(context)

    duration = None
    try:
        if ti.start_date:
            end = ti.end_date or datetime.now(timezone.utc)
            duration = (end - ti.start_date).total_seconds()
    except Exception:
        pass

    row = {
        "dag_id":             ti.dag_id,
        "task_id":            ti.task_id,
        "dag_run_id":         context["run_id"],
        "media_date":         _get_media_date(context),
        "table_name":         table_name,
        "event":              "END",
        "status":             status,
        "end_time":           _now_iso(),
        "task_duration_secs": duration,
        "records_processed":  processed,
        "records_success":    success,
        "records_failed":     failed,
        "inserted_at":        _now_iso(),
    }

    if status == "FAILED":
        exc = context.get("exception")
        row["error_type"]    = type(exc).__name__ if exc else "UnknownError"
        row["error_message"] = str(exc)[:4000] if exc else ""
        try:
            row["traceback"] = "".join(
                tb.format_exception(type(exc), exc, exc.__traceback__))[:8000] if exc else ""
        except Exception:
            row["traceback"] = ""

    _insert(AUDIT_TABLE, [row])