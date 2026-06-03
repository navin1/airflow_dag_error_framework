import json
import logging
import os
import urllib.error
import urllib.request
from pathlib import Path

from airflow.models import Variable
from airflow.models.xcom import XCom
from jinja2 import Environment, FileSystemLoader

log = logging.getLogger(__name__)

_TEMPLATE_DIR = Path(__file__).parent / "templates"


# ---------------------------------------------------------------------------
# DAG-level callback (registered by patch_dag)
# ---------------------------------------------------------------------------

def dag_failure_email_callback(context: dict) -> None:
    """Collect per-task XCom failure summaries and send a consolidated alert."""
    dag_id         = context["dag"].dag_id
    run_id         = context.get("run_id", "")
    execution_date = str(context.get("ds", ""))
    airflow_ui_url = Variable.get("AIRFLOW_UI_URL", default_var="http://localhost:8080")

    failed_tasks = _collect_failure_xcoms(context)
    failed_tasks = _enrich_row_counts(failed_tasks, dag_id, run_id)

    recipients_raw = Variable.get("ALERT_EMAIL_RECIPIENTS", default_var="")
    recipients = [r.strip() for r in recipients_raw.split(",") if r.strip()]
    if not recipients:
        log.warning("ALERT_EMAIL_RECIPIENTS Airflow Variable is empty — skipping failure email")
        return

    dag_url = f"{airflow_ui_url.rstrip('/')}/dags/{dag_id}/grid"
    html = _render_template(
        dag_id              = dag_id,
        run_id              = run_id,
        execution_date      = execution_date,
        failed_count        = len(failed_tasks),
        failed_task_details = failed_tasks,
        dag_url             = dag_url,
    )
    send_via_sendgrid(
        to           = recipients,
        subject      = f"[AIRFLOW FAILURE] {dag_id} | {execution_date}",
        html_content = html,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _collect_failure_xcoms(context: dict) -> list[dict]:
    dag_run = context["dag_run"]
    dag_id  = context["dag"].dag_id
    run_id  = context.get("run_id", "")

    failed = []
    for ti in dag_run.get_task_instances(state=["failed"]):
        val = XCom.get_one(
            run_id             = run_id,
            key                = f"failure_{ti.task_id}",
            task_id            = ti.task_id,
            dag_id             = dag_id,
            include_prior_dates= False,
        )
        if val and isinstance(val, dict):
            failed.append(val)
        else:
            failed.append({
                "task_id":       ti.task_id,
                "table_name":    "",
                "error_type":    "UnknownError",
                "error_message": str(ti.state),
                "traceback":     "",
            })

    for t in failed:
        t.setdefault("rows_processed", "N/A")
        t.setdefault("rows_inserted",  "N/A")
        t.setdefault("rows_failed",    "N/A")
        t.setdefault("duration",       "N/A")

    return failed


def _enrich_row_counts(failed_tasks: list[dict], dag_id: str, run_id: str) -> list[dict]:
    """Join failed tasks with dag_audit_log to fill in row counts."""
    try:
        from google.cloud import bigquery
        from utils.airflow_config import get_config

        cfg          = get_config()
        project      = cfg["project1"]
        dataset      = cfg["error_log_dataset"]
        audit_table  = f"{project}.{dataset}.{cfg['audit_log_table']}"

        client = bigquery.Client()
        query  = f"""
            SELECT task_id, total_rows, clean_rows, error_rows
            FROM `{audit_table}`
            WHERE dag_id     = @dag_id
              AND dag_run_id = @run_id
        """
        job_cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("dag_id",  "STRING", dag_id),
            bigquery.ScalarQueryParameter("run_id",  "STRING", run_id),
        ])
        audit_map = {row.task_id: row for row in client.query(query, job_config=job_cfg).result()}

        for task in failed_tasks:
            a = audit_map.get(task["task_id"])
            if a:
                task["rows_processed"] = f"{a.total_rows or 0:,}"
                task["rows_inserted"]  = f"{a.clean_rows  or 0:,}"
                task["rows_failed"]    = f"{a.error_rows  or 0:,}"

    except Exception as exc:
        log.warning("Could not enrich row counts from audit log: %s", exc)

    return failed_tasks


def _render_template(**kwargs) -> str:
    env = Environment(loader=FileSystemLoader(str(_TEMPLATE_DIR)), autoescape=True)
    return env.get_template("dag_failure_email.html").render(**kwargs)


# ---------------------------------------------------------------------------
# SendGrid HTTP delivery
# ---------------------------------------------------------------------------

def send_via_sendgrid(to: list[str], subject: str, html_content: str) -> None:
    """Send an email via the SendGrid v3 Mail Send API (no SMTP, no SDK)."""
    api_key    = os.environ["SENDGRID_API_KEY"]
    from_email = os.environ.get("EMAIL_FROM", "noreply@example.com")

    payload = {
        "personalizations": [{"to": [{"email": addr} for addr in to]}],
        "from":    {"email": from_email},
        "subject": subject,
        "content": [{"type": "text/html", "value": html_content}],
    }

    req = urllib.request.Request(
        "https://api.sendgrid.com/v3/mail/send",
        data    = json.dumps(payload).encode("utf-8"),
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type":  "application/json",
        },
        method  = "POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            log.info("SendGrid accepted email (status=%s) subject=%r to=%s", resp.status, subject, to)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        log.error("SendGrid HTTP %s — %s", exc.code, body)
    except Exception as exc:
        log.error("SendGrid request failed: %s", exc)
