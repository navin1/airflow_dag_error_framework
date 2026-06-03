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
SQL_SUB = "abcd"  # SQL lives in bq_sql/abcd/

TASKS = [
    ("insert_aas", "{project1}.{aas_table}"),
]

# ── DAG definition ────────────────────────────────────────────────────────
default_args = {
    "owner":           "eda_osr",
    "retries":         1,
    "start_date":      datetime(2024, 1, 1),
    "depends_on_past": False,
}

with DAG(
    dag_id="eda_osr_dag_folder2_dag1",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["eda_osr", "raw_zone", "dag_folder2"],
    doc_md="""
### eda_osr_dag_folder2_dag1
Daily load of AAS data into the EDA OSR raw zone.

SQL is located in `bq_sql/abcd/` (SQL_SUB="abcd").
Error handling is injected by `patch_dag` at the bottom of this file.
    """,
) as dag:

    t_insert_aas = BigQueryInsertJobOperator(
        task_id="insert_aas",
        configuration={
            "query": {
                "query":        load_sql(SQL_BASE, "insert_aas", SQL_SUB),
                "useLegacySql": False,
            }
        },
        location="US",
        gcp_conn_id="google_cloud_default",
    )

# ── Attach error-handling callbacks (2 lines) ─────────────────────────────
patch_dag(dag, TASKS)
