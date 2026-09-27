"""Daily sales ETL: extract -> transform -> load -> quality check -> notify.

Runs once per day for the previous day's data. Retries transient failures,
enforces an SLA, and never backfills (catchup=False) — late data is handled
by the partitioned backfill DAG instead.
"""
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

SCRIPTS_DIR = os.environ.get(
    "ETL_SCRIPTS_DIR", os.path.join(os.path.dirname(__file__), "..", "scripts")
)
sys.path.insert(0, os.path.abspath(SCRIPTS_DIR))

from etl_tasks import (  # noqa: E402
    data_quality_check,
    extract_orders,
    load_daily_revenue,
    transform_orders,
)

DATA_DIR = os.environ.get("ETL_DATA_DIR", "/opt/airflow/data")

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(hours=1),
}


def _paths(**context):
    ds = context["ds"]
    base = os.path.join(DATA_DIR, ds)
    return {
        "raw": os.path.join(DATA_DIR, "raw", "orders.csv"),
        "staging": os.path.join(base, "staging", "orders.csv"),
        "clean": os.path.join(base, "clean", "orders.csv"),
        "fact": os.path.join(base, "fact", "daily_revenue.csv"),
    }


def extract(**context):
    p = _paths(**context)
    n = extract_orders(p["raw"], p["staging"])
    print(f"extracted {n} rows for {context['ds']}")


def transform(**context):
    p = _paths(**context)
    n = transform_orders(p["staging"], p["clean"])
    print(f"cleaned {n} rows for {context['ds']}")


def load(**context):
    p = _paths(**context)
    n = load_daily_revenue(p["clean"], p["fact"])
    print(f"loaded {n} fact rows for {context['ds']}")


def quality(**context):
    p = _paths(**context)
    n = data_quality_check(p["fact"])
    print(f"quality checks passed on {n} fact rows for {context['ds']}")


with DAG(
    dag_id="daily_sales_etl",
    default_args=default_args,
    description="Daily sales ETL with retries, SLA and quality gates",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sales", "etl"],
) as dag:

    t_extract = PythonOperator(task_id="extract", python_callable=extract)
    t_transform = PythonOperator(task_id="transform", python_callable=transform)
    t_load = PythonOperator(task_id="load", python_callable=load)
    t_quality = PythonOperator(task_id="quality_check", python_callable=quality)
    t_notify = BashOperator(
        task_id="notify",
        bash_command='echo "daily_sales_etl succeeded for {{ ds }}"',
    )

    t_extract >> t_transform >> t_load >> t_quality >> t_notify
