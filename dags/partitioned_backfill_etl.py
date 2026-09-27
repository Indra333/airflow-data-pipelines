"""Backfill-friendly partitioned pipeline.

Each DAG run owns exactly one logical-date partition: it reads the cleaned
orders, filters to its own `ds`, and overwrites
`output/daily_revenue/date=<ds>/revenue.csv`. Because writes are
partition-scoped and idempotent, any date range can be safely re-run:

    airflow dags backfill partitioned_backfill_etl \\
        --start-date 2024-08-01 --end-date 2024-08-09

catchup=True plus max_active_runs keeps backfills parallel but bounded.
"""
import csv
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

SCRIPTS_DIR = os.environ.get(
    "ETL_SCRIPTS_DIR", os.path.join(os.path.dirname(__file__), "..", "scripts")
)
sys.path.insert(0, os.path.abspath(SCRIPTS_DIR))

from etl_tasks import data_quality_check, transform_orders  # noqa: E402

DATA_DIR = os.environ.get("ETL_DATA_DIR", "/opt/airflow/data")


def process_partition(**context):
    ds = context["ds"]
    staging = os.path.join(DATA_DIR, "raw", "orders.csv")
    clean = os.path.join(DATA_DIR, "tmp", ds, "orders_clean.csv")

    n = transform_orders(staging, clean)
    print(f"cleaned {n} rows (all dates) for partition run {ds}")

    # Aggregate only this run's logical date, then overwrite the partition.
    totals = {}
    with open(clean, newline="") as f:
        for r in csv.DictReader(f):
            if r["order_date"] != ds:
                continue
            key = r["product_category"]
            totals.setdefault(key, {"num_orders": 0, "revenue_cents": 0})
            totals[key]["num_orders"] += 1
            totals[key]["revenue_cents"] += int(r["line_total_cents"])

    partition_dir = os.path.join(DATA_DIR, "output", "daily_revenue", f"date={ds}")
    os.makedirs(partition_dir, exist_ok=True)
    fact_path = os.path.join(partition_dir, "revenue.csv")
    with open(fact_path, "w", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["order_date", "product_category", "num_orders", "revenue_dollars"]
        )
        writer.writeheader()
        for category in sorted(totals):
            agg = totals[category]
            writer.writerow(
                {
                    "order_date": ds,
                    "product_category": category,
                    "num_orders": agg["num_orders"],
                    "revenue_dollars": round(agg["revenue_cents"] / 100.0, 2),
                }
            )

    checked = data_quality_check(fact_path)
    print(f"partition date={ds}: wrote and validated {checked} rows")


with DAG(
    dag_id="partitioned_backfill_etl",
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    description="Idempotent per-date partitions, safe to backfill any range",
    schedule_interval="@daily",
    start_date=datetime(2024, 8, 1),
    catchup=True,
    max_active_runs=3,
    tags=["sales", "etl", "backfill"],
) as dag:

    process = PythonOperator(
        task_id="process_partition",
        python_callable=process_partition,
    )
