"""Daily weather pipeline: extract -> load -> dbt run -> dbt test.

Everything the DAG does lives in the ``pipelines`` package, so the tasks here
are four thin wrappers. That is deliberate: the notebook imports the same
functions, so the walkthrough cannot drift from what Airflow executes.

Idempotency / backfill
----------------------
Each run is pinned to its logical date. The extract writes a file named after
the weather date it covers, and the load replaces that date's rows in the raw
table. Re-running a date -- by hand, by retry, or by ``airflow backfill`` --
converges on the same state instead of duplicating rows, so catchup is safe.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

import pendulum

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from airflow.sdk import dag, task  # noqa: E402

from pipelines.dbt_runner import dbt_run, dbt_test, run_or_raise  # noqa: E402
from pipelines.extract import extract_to_landing  # noqa: E402
from pipelines.load import load_landing_file  # noqa: E402

DEFAULT_ARGS = {
    # The API is the flaky part; a couple of spaced retries absorb a 429 or a
    # blip without waking anyone up.
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "depends_on_past": False,
}


@dag(
    dag_id="weather_pipeline",
    description="Daily Open-Meteo archive weather for a handful of cities.",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=True,
    # One run at a time: every run writes the same dbt models, so serialising
    # keeps the warehouse consistent during a backfill.
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["weather", "elt", "dbt"],
    doc_md=__doc__,
)
def weather_pipeline():
    @task
    def extract(logical_date: datetime | None = None) -> str:
        """Call the Open-Meteo archive API for this run's date; land the JSON."""
        return str(extract_to_landing(logical_date.date()))

    @task
    def load(landing_file: str) -> str:
        """Replace this date's rows in the raw table with the landed records."""
        return str(load_landing_file(landing_file))

    @task
    def transform(load_summary: str) -> str:
        """Build staging and marts with dbt."""
        return run_or_raise(dbt_run()).summary()

    @task
    def test(run_summary: str) -> str:
        """Fail the run if any dbt schema or data test fails."""
        return run_or_raise(dbt_test()).summary()

    test(transform(load(extract())))


dag_instance = weather_pipeline()
