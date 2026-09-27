"""Tests for the shared ETL logic (stdlib only) and DAG file syntax.

Run with: pytest tests/
(The DAG syntax test parses the DAG files with ast — no Airflow install needed.)
"""
import ast
import csv
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from etl_tasks import (  # noqa: E402
    data_quality_check,
    extract_orders,
    load_daily_revenue,
    transform_orders,
)

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
SAMPLE_ORDERS = os.path.join(REPO_ROOT, "data", "raw", "orders.csv")


def _write_csv(path, fieldnames, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_extract_validates_header(tmp_path):
    bad = tmp_path / "bad.csv"
    _write_csv(str(bad), ["wrong", "columns"], [{"wrong": "1", "columns": "2"}])
    try:
        extract_orders(str(bad), str(tmp_path / "out.csv"))
    except ValueError as e:
        assert "missing columns" in str(e)
    else:
        raise AssertionError("expected ValueError for bad header")


def test_transform_dedupes_and_computes_totals(tmp_path):
    raw = tmp_path / "raw.csv"
    _write_csv(
        str(raw),
        ["order_id", "customer_id", "order_date", "product_category", "quantity", "unit_price_cents"],
        [
            {"order_id": "1", "customer_id": "1", "order_date": "2024-08-01",
             "product_category": "Books", "quantity": "2", "unit_price_cents": "1500"},
            {"order_id": "1", "customer_id": "1", "order_date": "2024-08-05",
             "product_category": "Books", "quantity": "1", "unit_price_cents": "1500"},
            {"order_id": "", "customer_id": "1", "order_date": "2024-08-01",
             "product_category": "Books", "quantity": "1", "unit_price_cents": "1500"},
        ],
    )
    clean = str(tmp_path / "clean.csv")
    assert transform_orders(str(raw), clean) == 1  # null dropped, dupe deduped
    with open(clean, newline="") as f:
        row = list(csv.DictReader(f))[0]
    assert row["order_date"] == "2024-08-05"  # latest wins
    assert row["line_total_cents"] == "1500"


def test_load_daily_revenue_math(tmp_path):
    clean = tmp_path / "clean.csv"
    _write_csv(
        str(clean),
        ["order_id", "customer_id", "order_date", "product_category", "quantity",
         "unit_price_cents", "line_total_cents"],
        [
            {"order_id": "1", "customer_id": "1", "order_date": "2024-08-01",
             "product_category": "Books", "quantity": "2", "unit_price_cents": "1500",
             "line_total_cents": "3000"},
            {"order_id": "2", "customer_id": "2", "order_date": "2024-08-01",
             "product_category": "Books", "quantity": "1", "unit_price_cents": "2000",
             "line_total_cents": "2000"},
        ],
    )
    fact = str(tmp_path / "fact.csv")
    assert load_daily_revenue(str(clean), fact) == 1
    with open(fact, newline="") as f:
        row = list(csv.DictReader(f))[0]
    assert row["num_orders"] == "2"
    assert row["revenue_dollars"] == "50.0"


def test_quality_check_rejects_negative_revenue(tmp_path):
    fact = tmp_path / "fact.csv"
    _write_csv(
        str(fact),
        ["order_date", "product_category", "num_orders", "revenue_dollars"],
        [{"order_date": "2024-08-01", "product_category": "Books",
          "num_orders": "1", "revenue_dollars": "-5.0"}],
    )
    try:
        data_quality_check(str(fact))
    except ValueError as e:
        assert "negative revenue" in str(e)
    else:
        raise AssertionError("expected ValueError for negative revenue")


def test_quality_check_rejects_empty_fact(tmp_path):
    fact = tmp_path / "fact.csv"
    _write_csv(str(fact), ["order_date", "product_category", "num_orders", "revenue_dollars"], [])
    try:
        data_quality_check(str(fact))
    except ValueError as e:
        assert "empty" in str(e)
    else:
        raise AssertionError("expected ValueError for empty fact")


def test_dag_files_define_expected_dags_and_tasks():
    dags_dir = os.path.join(REPO_ROOT, "dags")
    expectations = {
        "daily_sales_etl.py": ("daily_sales_etl",
                               ["extract", "transform", "load", "quality_check", "notify"]),
        "partitioned_backfill_etl.py": ("partitioned_backfill_etl", ["process_partition"]),
    }
    for filename, (dag_id, task_ids) in expectations.items():
        with open(os.path.join(dags_dir, filename)) as f:
            source = f.read()
        tree = ast.parse(source)  # raises on syntax errors
        assert f'dag_id="{dag_id}"' in source, f"{filename} missing dag_id"
        for task_id in task_ids:
            assert f'task_id="{task_id}"' in source, f"{filename} missing task {task_id}"


def test_sample_data_end_to_end(tmp_path):
    """Full extract -> transform -> load -> quality on the bundled sample data."""
    staging = str(tmp_path / "staging.csv")
    clean = str(tmp_path / "clean.csv")
    fact = str(tmp_path / "fact.csv")
    assert extract_orders(SAMPLE_ORDERS, staging) == 17
    assert transform_orders(staging, clean) == 16
    assert load_daily_revenue(clean, fact) == 16
    assert data_quality_check(fact) == 16
