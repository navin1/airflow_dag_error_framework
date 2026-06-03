import json
import logging
import os
import urllib.error
import urllib.request
from pathlib import Path

from airflow.models import Variable
from airflow.models.xcom import XCom

log = logging.getLogger(__name__)

_TEMPLATE_PATH = Path(__file__).parent / "templates" / "dag_failure_email.html"


# ---------------------------------------------------------------------------
# DAG-level callback (registered by patch_dag)
# ---------------------------------------------------------------------------

def dag_failure_email_callback(context: dict) -> None:
    """Collect per-task XCom failure summaries and send a consolidated alert."""
    dag_id = context["dag"].dag_id
    run_id = context.get("run_id", "")
    execution_date = str(context.get("ds", ""))
    airflow_ui_url = Variable.get("AIRFLOW_UI_URL", default_var="http://localhost:8080")

    failed_tasks = _collect_failure_xcoms(context)

    recipients_raw = Variable.get("ALERT_EMAIL_RECIPIENTS", default_var="")
    recipients = [r.strip() for r in recipients_raw.split(",") if r.strip()]
    if not recipients:
        log.warning("ALERT_EMAIL_RECIPIENTS Airflow Variable is empty — skipping failure email")
        return

    html = _render_template(dag_id, run_id, execution_date, failed_tasks, airflow_ui_url)
    send_via_sendgrid(
        to=recipients,
        subject=f"[AIRFLOW FAILURE] {dag_id} | {execution_date}",
        html_content=html,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _collect_failure_xcoms(context: dict) -> list[dict]:
    """Pull failure XComs pushed by make_task_failure_callback for each failed task."""
    dag_run = context["dag_run"]
    dag_id = context["dag"].dag_id
    run_id = context.get("run_id", "")

    failed = []
    for ti in dag_run.get_task_instances(state=["failed"]):
        xcom_key = f"failure_{ti.task_id}"
        val = XCom.get_one(
            run_id=run_id,
            key=xcom_key,
            task_id=ti.task_id,
            dag_id=dag_id,
            include_prior_dates=False,
        )
        if val and isinstance(val, dict):
            failed.append(val)
        else:
            # Fallback: build a minimal entry from task instance metadata
            failed.append({
                "task_id": ti.task_id,
                "dag_id": dag_id,
                "run_id": run_id,
                "execution_date": str(context.get("ds", "")),
                "error": str(ti.state),
                "target_table": "",
            })
    return failed


def _render_template(
    dag_id: str,
    run_id: str,
    execution_date: str,
    failed_tasks: list[dict],
    airflow_ui_url: str,
) -> str:
    template = _TEMPLATE_PATH.read_text()

    task_rows = ""
    for t in failed_tasks:
        error_text = str(t.get("error", "N/A")).replace("<", "&lt;").replace(">", "&gt;")
        task_rows += (
            f"<tr>"
            f"<td>{t.get('task_id', 'N/A')}</td>"
            f"<td>{t.get('target_table', '') or '—'}</td>"
            f'<td style="color:#dc3545;word-break:break-all;">{error_text}</td>'
            f"</tr>\n"
        )

    dag_url = f"{airflow_ui_url.rstrip('/')}/dags/{dag_id}/grid"

    return (
        template
        .replace("{{dag_id}}", dag_id)
        .replace("{{run_id}}", run_id)
        .replace("{{execution_date}}", execution_date)
        .replace("{{task_rows}}", task_rows)
        .replace("{{dag_url}}", dag_url)
        .replace("{{failed_count}}", str(len(failed_tasks)))
    )


# ---------------------------------------------------------------------------
# SendGrid HTTP delivery
# ---------------------------------------------------------------------------

def send_via_sendgrid(to: list[str], subject: str, html_content: str) -> None:
    """Send an email via the SendGrid v3 Mail Send API (no SMTP, no SDK)."""
    api_key = os.environ["SENDGRID_API_KEY"]
    from_email = os.environ.get("EMAIL_FROM", "noreply@example.com")

    payload = {
        "personalizations": [{"to": [{"email": addr} for addr in to]}],
        "from": {"email": from_email},
        "subject": subject,
        "content": [{"type": "text/html", "value": html_content}],
    }

    req = urllib.request.Request(
        "https://api.sendgrid.com/v3/mail/send",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            log.info("SendGrid accepted email (status=%s) subject=%r to=%s", resp.status, subject, to)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        log.error("SendGrid HTTP %s — %s", exc.code, body)
    except Exception as exc:
        log.error("SendGrid request failed: %s", exc)
