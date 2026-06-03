import json
import logging
from datetime import datetime, timezone
from typing import Any

from google.cloud import bigquery

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _log_tables() -> tuple[str, str]:
    from .airflow_config import get_config
    cfg = get_config()
    project = cfg["project1"]
    dataset = cfg["error_log_dataset"]
    return (
        f"{project}.{dataset}.{cfg['error_log_table']}",
        f"{project}.{dataset}.{cfg['audit_log_table']}",
    )


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _bq_insert(table_ref: str, rows: list[dict[str, Any]]) -> None:
    client = bigquery.Client()
    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        log.error("BigQuery streaming insert errors into %s: %s", table_ref, errors)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def make_task_failure_callback(task_id: str, target_table: str = ""):
    """Return an on_failure_callback that logs to BQ and pushes an XCom summary.

    Args:
        task_id:      Airflow task ID (used as the XCom key suffix).
        target_table: Resolved BQ target table for this task (informational).
    """

    def _on_task_failure(context: dict) -> None:
        dag_id = context["dag"].dag_id
        run_id = context.get("run_id", "")
        execution_date = str(context.get("ds", ""))
        exception = context.get("exception")
        error_msg = str(exception) if exception else "Unknown error"

        # Push summary XCom so the DAG-level callback can build the email
        ti = context["task_instance"]
        ti.xcom_push(
            key=f"failure_{task_id}",
            value={
                "task_id": task_id,
                "dag_id": dag_id,
                "run_id": run_id,
                "execution_date": execution_date,
                "error": error_msg,
                "target_table": target_table,
            },
        )

        # Write a runtime-failure row to the error + audit tables
        try:
            error_table, audit_table = _log_tables()
            _bq_insert(error_table, [{
                "dag_id": dag_id,
                "task_id": task_id,
                "dag_run_id": run_id,
                "execution_date": execution_date,
                "inserted_at": _now_iso(),
                "source_table": target_table,
                "row_data": json.dumps({"exception": error_msg}),
                "error_reason": "TASK_RUNTIME_FAILURE",
            }])
            _bq_insert(audit_table, [{
                "dag_id": dag_id,
                "task_id": task_id,
                "dag_run_id": run_id,
                "execution_date": execution_date,
                "inserted_at": _now_iso(),
                "total_rows": 0,
                "clean_rows": 0,
                "error_rows": 0,
                "status": f"FAILED: {error_msg[:200]}",
            }])
        except Exception as exc:
            log.error("Failed to write failure record to BigQuery: %s", exc)

    return _on_task_failure
