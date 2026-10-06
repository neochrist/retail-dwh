# pyright: reportMissingImports=false
# (Airflow is installed only in the Docker image.)
"""DAG integrity test. Skipped locally; run inside the container:

docker compose exec airflow-scheduler pytest tests/test_dags.py
"""

from pathlib import Path

import pytest

pytest.importorskip("airflow")

from airflow.models import DAG, DagBag  # noqa: E402

DAGS_DIR = Path(__file__).parents[1] / "dags"


@pytest.fixture(scope="module")
def dag() -> DAG:
    dagbag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    assert dagbag.import_errors == {}
    return dagbag.get_dag("retail_daily")


def test_tasks_run_in_order(dag: DAG) -> None:
    assert set(dag.task_ids) == {"download_dataset", "load_raw_sales"}
    assert dag.get_task("download_dataset").downstream_task_ids == {"load_raw_sales"}


def test_runs_daily_at_midnight_tbilisi(dag: DAG) -> None:
    assert dag.schedule_interval == "0 0 * * *"
    assert dag.timezone.name == "Asia/Tbilisi"
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_tasks_retry(dag: DAG) -> None:
    assert all(task.retries == 2 for task in dag.tasks)
