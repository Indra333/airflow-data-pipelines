# Airflow Data Pipelines

Production-style Apache Airflow DAGs for a daily sales ETL, with the shared pipeline logic kept in plain Python (`scripts/etl_tasks.py`) so it stays testable without Airflow.

## DAGs

**daily_sales_etl** (`dags/daily_sales_etl.py`) — the everyday workhorse. Runs `@daily` with `catchup=False`:

```
extract → transform → load → quality_check → notify
```

Each run processes its logical date into an isolated `data/<ds>/` directory. It retries transient failures 3 times with 5-minute delays, enforces a 1-hour SLA, and fails the run if any quality gate trips.

**partitioned_backfill_etl** (`dags/partitioned_backfill_etl.py`) — built for history. Each run owns exactly one logical-date partition: it filters the cleaned orders to its own `ds` and overwrites `data/output/daily_revenue/date=<ds>/revenue.csv`. Because writes are partition-scoped and idempotent, any range can be re-run safely:

```bash
airflow dags backfill partitioned_backfill_etl \
  --start-date 2024-08-01 --end-date 2024-08-09
```

`catchup=True` with `max_active_runs=3` keeps backfills parallel but bounded.

## Scheduling, retries, backfill strategy

New data flows through `daily_sales_etl` every day and never backfills on its own. When source data arrives late or a bug is fixed, `partitioned_backfill_etl` replays exactly the affected dates — each partition overwrites itself, so re-runs are safe and rerunning the whole range converges to the same result. Retries handle transient failures; anything structural fails fast at the quality gate.

## Run it locally

```bash
export FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
docker compose up -d        # webserver on http://localhost:8080 (admin/admin)
```

Sample data lives in `data/raw/orders.csv`. The pipeline logic itself needs no Airflow to exercise:

```bash
pytest tests/               # 7 tests: ETL logic, quality gates, DAG syntax, end-to-end
```
