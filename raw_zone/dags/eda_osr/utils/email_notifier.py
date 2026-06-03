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

    recipients_raw = Variable.get("ALERT_EMAIL_RECIPIENTS", default_var="")
    recipients = [r.strip() for r in recipients_raw.split(",") if r.strip()]
    if not recipients:
        log.warning("ALERT_EMAIL_RECIPIENTS Airflow Variable is empty — skipping failure email")
        return

    trace_uuid = context["task_instance"].xcom_pull(task_ids="make_uuid_task") or "N/A"

    dag_url = f"{airflow_ui_url.rstrip('/')}/dags/{dag_id}/grid"
    html = _render_template(
        dag_id              = dag_id,
        run_id              = run_id,
        execution_date      = execution_date,
        failed_count        = len(failed_tasks),
        failed_task_details = failed_tasks,
        trace_uuid          = trace_uuid,
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

    return failed


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
