from pathlib import Path

from .airflow_config import get_config
from .config_loader import render_template


def build_sql_path(sql_base: Path, task_id: str, sql_sub: str | None = None) -> Path:
    """Return the path to a SQL file following the eda_osr_rps_{task_id}.sql convention.

    Args:
        sql_base: Base bq_sql/ directory for the calling DAG folder.
        task_id:  Airflow task ID (e.g. "insert_sales").
        sql_sub:  Optional subdirectory inside bq_sql/ (e.g. "abcd").
                  Pass None for a flat bq_sql/ layout.
    """
    filename = f"eda_osr_rps_{task_id}.sql"
    if sql_sub:
        return sql_base / sql_sub / filename
    return sql_base / filename


def load_sql(sql_base: Path, task_id: str, sql_sub: str | None = None) -> str:
    """Load a SQL file and substitute {key} config placeholders.

    Airflow Jinja vars ({{ ds }}, {{ run_id }}, etc.) are preserved verbatim
    for the BigQueryInsertJobOperator template engine.
    """
    path = build_sql_path(sql_base, task_id, sql_sub)
    if not path.exists():
        raise FileNotFoundError(
            f"SQL file not found: {path}\n"
            f"  task_id={task_id!r}, sql_sub={sql_sub!r}, sql_base={sql_base}"
        )
    content = path.read_text()
    return render_template(content, get_config())
