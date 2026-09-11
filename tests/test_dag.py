"""The DAG must parse and keep its contract: dependencies, retries, catchup."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def dag():
    os.environ.setdefault("AIRFLOW_HOME", str(PROJECT_ROOT / "airflow_home"))
    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
    from airflow.models.dagbag import DagBag

    bag = DagBag(str(PROJECT_ROOT / "dags"), include_examples=False)
    assert bag.import_errors == {}, bag.import_errors
    return bag.dags["weather_pipeline"]


def test_tasks_run_in_order(dag):
    order = {task.task_id: sorted(task.downstream_task_ids) for task in dag.tasks}
    assert order == {
        "extract": ["load"],
        "load": ["transform"],
        "transform": ["test"],
        "test": [],
    }


def test_schedule_is_daily_with_catchup_for_backfills(dag):
    # "@daily" normalises to a midnight cron in Airflow 3.
    assert dag.timetable.summary == "0 0 * * *"
    assert dag.catchup is True
    assert dag.max_active_runs == 1


def test_tasks_retry(dag):
    assert all(task.retries >= 1 for task in dag.tasks)
