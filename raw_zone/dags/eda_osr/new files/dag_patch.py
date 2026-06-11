"""
utils/dag_patch.py  (UPDATED)
─────────────────────────────
Attaches the full audit + error mechanism to every task in a DAG with ZERO
changes to the DAGs themselves:

    pre_execute          -> START row in dag_audit_log   (status=RUNNING)
    on_success_callback  -> END row   in dag_audit_log   (status=SUCCESS)
    on_failure_callback  -> END row   in dag_audit_log   (status=FAILED)
                            + your existing error/email callbacks still fire

Existing callbacks already set on a task (e.g. the email notifier from the
error framework) are PRESERVED — the audit callback is chained around them.

Usage (unchanged from before):
    from utils.dag_patch import patch_dag
    patch_dag(dag, TASKS, config=paramf)

TASKS entries support a single table template OR a list of templates:
    ("rpe_fee_type08",  "{project_transact_pdata}.{rpe_fee_type08}"),
    ("rpe_tendertype_type31", [
        "{project_transact_pdata}.{rps_rpe_tendertype_type31}",
        "{project_transact_pdata}.{rps_rpe_merchandise_type05}",
    ]),
"""

from __future__ import annotations

import logging

from utils.audit_logger import log_task_start, log_task_end

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Table-template resolution (single string OR list, merged config aware)
# ─────────────────────────────────────────────────────────────────────────────
def _resolve_tables(table_template, config: dict | None) -> str:
    """Resolve one template or a list of templates to a '\n'-joined string."""
    from utils.airflow_config import render_template, resolve  # local import

    def _one(t: str) -> str:
        if not t:
            return ""
        return render_template(t, config) if config else resolve(t)

    if isinstance(table_template, list):
        return "\n".join(_one(t) for t in table_template if t)
    return _one(table_template)


# ─────────────────────────────────────────────────────────────────────────────
# Callback factories (capture table + previous callbacks per task)
# ─────────────────────────────────────────────────────────────────────────────
def _as_list(cb):
    """Normalise an existing callback attribute to a list (Airflow accepts both)."""
    if cb is None:
        return []
    return list(cb) if isinstance(cb, (list, tuple)) else [cb]


def _make_pre_execute(prev_pre_execute, table: str):
    def _pre_execute(context):
        log_task_start(context, table_name=table)
        # Chain the operator's original pre_execute so nothing is lost
        if prev_pre_execute is not None:
            try:
                prev_pre_execute(context)
            except TypeError:
                # bound method on BaseOperator: signature (self, context)
                prev_pre_execute.__func__(prev_pre_execute.__self__, context)
    return _pre_execute


def _make_success_cb(table: str):
    def _on_success(context):
        log_task_end(context, status="SUCCESS", table_name=table)
    return _on_success


def _make_failure_cb(table: str):
    def _on_failure(context):
        log_task_end(context, status="FAILED", table_name=table)
    return _on_failure


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────
def patch_dag(dag, tasks: list[tuple], config: dict | None = None) -> None:
    """Attach audit START/END logging to every task in *dag*.

    Args:
        dag:    The Airflow DAG object.
        tasks:  List of (task_id_suffix, table_template_or_list).
        config: Merged config dict (global + project) used to resolve
                table templates. If None, utils.airflow_config.resolve()
                is used (global config only).
    """
    # Build task_id -> resolved table(s) map. Matches on suffix so prefixed
    # task ids (e.g. 'eda_osr_rps_rpe_fee_type08') still resolve.
    table_map: dict[str, str] = {}
    for task_suffix, template in tasks:
        try:
            table_map[task_suffix] = _resolve_tables(template, config)
        except Exception:
            log.exception("Could not resolve table for task %s", task_suffix)
            table_map[task_suffix] = ""

    for task in dag.tasks:
        table = ""
        for suffix, resolved in table_map.items():
            if task.task_id == suffix or task.task_id.endswith(suffix):
                table = resolved
                break

        # 1. START row — chain around any existing pre_execute
        task.pre_execute = _make_pre_execute(task.pre_execute, table)

        # 2. END row (SUCCESS) — preserve existing success callbacks
        task.on_success_callback = _as_list(task.on_success_callback) + [
            _make_success_cb(table)
        ]

        # 3. END row (FAILED) — preserve existing failure callbacks
        #    (e.g. the email notifier / error framework callback)
        task.on_failure_callback = _as_list(task.on_failure_callback) + [
            _make_failure_cb(table)
        ]

    log.info("patch_dag: audit logging attached to %d tasks in %s",
             len(dag.tasks), dag.dag_id)