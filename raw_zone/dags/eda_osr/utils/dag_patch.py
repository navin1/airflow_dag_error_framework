"""patch_dag — two-line DAG integration.

Add at the bottom of any DAG file:

    from utils.dag_patch import patch_dag
    patch_dag(dag, TASKS)

TASKS format:
    [("task_id", "{project1}.{key1}"), ...]

Each task gets an on_failure_callback that logs to BigQuery and pushes an XCom.
The DAG itself gets an on_failure_callback that sends a consolidated SendGrid email.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure eda_osr/ is on sys.path so sibling utils imports resolve correctly
# when patch_dag is called from a sub-DAG folder two levels down.
_eda_osr_root = str(Path(__file__).parents[1])
if _eda_osr_root not in sys.path:
    sys.path.insert(0, _eda_osr_root)


def patch_dag(dag, tasks: list[tuple[str, str]]) -> None:
    """Attach error-logging and email callbacks to *dag*.

    Args:
        dag:   The Airflow DAG object (must already have all tasks registered).
        tasks: List of (task_id, target_table_template) where target_table_template
               may contain {key} placeholders resolved from the merged config.
    """
    from utils.airflow_config import resolve
    from utils.email_notifier import dag_failure_email_callback
    from utils.error_logger import make_task_failure_callback

    for task_id, table_template in tasks:
        try:
            task = dag.get_task(task_id)
        except Exception:
            import logging
            logging.getLogger(__name__).warning(
                "patch_dag: task %r not found in DAG %r — skipping callback", task_id, dag.dag_id
            )
            continue

        resolved_table = resolve(table_template)
        task.on_failure_callback = make_task_failure_callback(task_id, resolved_table)

    dag.on_failure_callback = dag_failure_email_callback
