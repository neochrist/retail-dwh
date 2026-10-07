# pyright: reportMissingImports=false
# (Airflow is installed only in the Docker image.)
"""Daily retail pipeline: download the Kaggle CSV, load it into ClickHouse, build the dbt models."""

import os
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain

DATA_DIR = Path(os.environ.get("DATA_DIR", "/opt/airflow/data"))
DBT_BUILD = (
    "/opt/dbt-venv/bin/dbt build --project-dir /opt/airflow/dbt --indirect-selection cautious"
)


@dag(
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Tbilisi"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    tags=["retail"],
)
def retail_daily() -> None:
    # Our module is imported inside each task, not at the top: the scheduler re-parses this file
    # every 30 seconds, and pandas/clickhouse-connect imports are too heavy for that.

    @task
    def download_dataset() -> str:
        from ingestion import raw_sales

        return str(raw_sales.download_dataset(DATA_DIR))

    @task
    def load_raw_sales(csv_path: str) -> None:
        from ingestion import raw_sales

        raw_sales.load_from_csv(Path(csv_path))

    @task.bash
    def dbt_staging() -> str:
        return f"{DBT_BUILD} --select path:models/staging"

    chain(load_raw_sales(download_dataset()), dbt_staging())


retail_daily()
