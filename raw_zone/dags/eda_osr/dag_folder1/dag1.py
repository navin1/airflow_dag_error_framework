import sys
from datetime import datetime
from pathlib import Path

# Make utils/ importable: add eda_osr/ to sys.path
sys.path.insert(0, str(Path(__file__).parents[1]))

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from utils.airflow_config import get_config
from utils.sql_loader import load_sql
from utils.dag_patch import patch_dag

# ── Config ────────────────────────────────────────────────────────────────
cfg = get_config()
SQL_BASE = Path(__file__).parent / "bq_sql"
SQL_SUB = None  # flat bq_sql/ layout — no sub-folder

# task_id → resolved target table template
TASKS = [
    ("insert_sales",  "{project1}.{key1}"),
    ("merge_returns", "{project1}.{returns_table}"),
]

# ── DAG definition ────────────────────────────────────────────────────────
default_args = {
    "owner":           "eda_osr",
    "retries":         1,
    "start_date":      datetime(2024, 1, 1),
    "depends_on_past": False,
}

with DAG(
    dag_id="eda_osr_dag_folder1_dag1",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["eda_osr", "raw_zone", "dag_folder1"],
    doc_md="""
### eda_osr_dag_folder1_dag1
Daily load of sales data and returns into the EDA OSR raw zone.

Tasks run sequentially: `insert_sales` → `merge_returns`.
Error handling is injected by `patch_dag` at the bottom of this file.
    """,
) as dag:

    t_insert_sales = BigQueryInsertJobOperator(
        task_id="insert_sales",
        configuration={
            "query": {
                "query":        load_sql(SQL_BASE, "insert_sales", SQL_SUB),
                "useLegacySql": False,
            }
        },
        location="US",
        gcp_conn_id="google_cloud_default",
    )

    t_merge_returns = BigQueryInsertJobOperator(
        task_id="merge_returns",
        configuration={
            "query": {
                "query":        load_sql(SQL_BASE, "merge_returns", SQL_SUB),
                "useLegacySql": False,
            }
        },
        location="US",
        gcp_conn_id="google_cloud_default",
    )

    t_insert_sales >> t_merge_returns

# ── Attach error-handling callbacks (2 lines) ─────────────────────────────
patch_dag(dag, TASKS)
