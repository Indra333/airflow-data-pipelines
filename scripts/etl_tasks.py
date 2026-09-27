"""Plain-Python ETL steps shared by the DAGs.

No Airflow imports here on purpose: this module is importable and runnable
anywhere, which makes the pipeline logic unit-testable without Airflow.
In production these steps would read/write S3/Snowflake; here they work on
local CSVs so the whole thing runs with docker compose.
"""
import csv
import os

REQUIRED_ORDER_COLUMNS = [
    "order_id",
    "customer_id",
    "order_date",
    "product_category",
    "quantity",
    "unit_price_cents",
]


def _ensure_parent(path: str) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    os.makedirs(parent, exist_ok=True)


def extract_orders(raw_path: str, staging_path: str) -> int:
    """Copy raw orders to staging after validating the header. Returns row count."""
    with open(raw_path, newline="") as src:
        reader = csv.DictReader(src)
        missing = [c for c in REQUIRED_ORDER_COLUMNS if c not in (reader.fieldnames or [])]
        if missing:
            raise ValueError(f"extract_orders: missing columns {missing}")
        rows = list(reader)
    _ensure_parent(staging_path)
    with open(staging_path, "w", newline="") as dst:
        writer = csv.DictWriter(dst, fieldnames=reader.fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def transform_orders(staging_path: str, clean_path: str) -> int:
    """Cast types, compute line totals, drop null order_ids, dedupe on
    order_id keeping the latest order_date. Returns cleaned row count."""
    with open(staging_path, newline="") as f:
        rows = [r for r in csv.DictReader(f) if r.get("order_id")]
    latest = {}
    for r in rows:
        key = r["order_id"]
        if key not in latest or r["order_date"] > latest[key]["order_date"]:
            latest[key] = r
    cleaned = []
    for r in latest.values():
        quantity = int(r["quantity"])
        unit_price_cents = int(r["unit_price_cents"])
        cleaned.append(
            {
                "order_id": r["order_id"],
                "customer_id": r["customer_id"],
                "order_date": r["order_date"],
                "product_category": r["product_category"].strip(),
                "quantity": quantity,
                "unit_price_cents": unit_price_cents,
                "line_total_cents": quantity * unit_price_cents,
            }
        )
    _ensure_parent(clean_path)
    with open(clean_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(cleaned[0].keys()))
        writer.writeheader()
        writer.writerows(cleaned)
    return len(cleaned)


def load_daily_revenue(clean_path: str, fact_path: str) -> int:
    """Aggregate cleaned orders to daily revenue per product category."""
    totals = {}
    with open(clean_path, newline="") as f:
        for r in csv.DictReader(f):
            key = (r["order_date"], r["product_category"])
            totals.setdefault(key, {"num_orders": 0, "revenue_cents": 0})
            totals[key]["num_orders"] += 1
            totals[key]["revenue_cents"] += int(r["line_total_cents"])
    rows = [
        {
            "order_date": date,
            "product_category": category,
            "num_orders": agg["num_orders"],
            "revenue_dollars": round(agg["revenue_cents"] / 100.0, 2),
        }
        for (date, category), agg in sorted(totals.items())
    ]
    _ensure_parent(fact_path)
    with open(fact_path, "w", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["order_date", "product_category", "num_orders", "revenue_dollars"]
        )
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def data_quality_check(fact_path: str) -> int:
    """Fail the task if the fact table is empty or any revenue is negative."""
    with open(fact_path, newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        raise ValueError("data_quality_check: fact table is empty")
    bad = [r for r in rows if float(r["revenue_dollars"]) < 0]
    if bad:
        raise ValueError(f"data_quality_check: negative revenue rows: {bad}")
    return len(rows)
