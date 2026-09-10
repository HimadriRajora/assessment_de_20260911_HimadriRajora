# Daily weather pipeline

Pulls daily weather for five cities (Berlin, London, New York, Tokyo, Sydney) from the free
[Open-Meteo archive API](https://open-meteo.com/en/docs/historical-weather-api), loads it,
models it with dbt and runs the whole thing on a schedule in Airflow.

```
Open-Meteo archive API
        |  one call per city, for one logical date
        v
  data/landing/weather_<date>.json          pipelines/extract.py
        |  delete-then-insert that date, in one transaction
        v
  raw.weather_daily      (API fields kept as-is, in JSON)   pipelines/load.py
        |
        v
  staging.stg_weather_daily          view    typed + renamed
        v
  marts.mart_city_weather_daily      table   daily report per city
  marts.mart_city_weather_summary    table   one row per city
```

Airflow runs it as `extract -> load -> dbt run -> dbt test`, daily, driven by the logical date
(`dags/weather_pipeline.py`).

The walkthrough is in [`notebooks/walkthrough.ipynb`](notebooks/walkthrough.ipynb), committed
with its outputs. It runs each stage by importing the same functions the DAG calls, shows the
row counts and the dbt output at each step, loads the same date twice to show nothing
duplicates, breaks a test on purpose to show it catches the problem, and finishes on the mart.

## Running it

With Docker:

```bash
cp .env.example .env      # make up does this for you if you forget
make up                   # builds the image, starts Airflow + JupyterLab
make reproduce            # runs the whole pipeline by executing the notebook
```

Airflow ends up on http://localhost:8080 (user `admin`, password in
`airflow_home/simple_auth_manager_passwords.json.generated`) and JupyterLab on
http://localhost:8888 with no token.

The DAG arrives paused, which is Airflow's default. Unpause it to watch it catch up from its
start date, or push it yourself:

```bash
make airflow-backfill     # backfill the last 30 logical dates through Airflow
```

If you would rather not use Docker, `make up-local` builds a `.venv` on Python 3.12 (it
installs [uv](https://docs.astral.sh/uv/) first if you don't have it) and everything works the
same way:

```bash
make up-local
make reproduce-local
make jupyter-local        # JupyterLab on :8888
make airflow-local        # Airflow standalone on :8080
```

## Checking it works

`make reproduce` is the short answer: it re-executes the notebook top to bottom, so if it
finishes, the pipeline ran. Beyond that:

* `make dag-test DATE=2026-09-08` runs the real DAG for one logical date.
* `make backfill BACKFILL_DAYS=30` loads 30 dates through the CLI.
* `make dbt-run` and `make dbt-test` build the models and run the 29 tests.
* `make dbt-docs` serves the model and column documentation on :8081.

To convince yourself re-running is safe, run a backfill twice and compare the counts:

```bash
make backfill BACKFILL_DAYS=3 && make backfill BACKFILL_DAYS=3
```

Section 3 of the notebook does this with the counts printed either side, and section 6 inserts
a duplicate row so you can watch the grain test fail and then pass again after a reload.

## What's where

```
pipelines/            the pipeline itself
  config.py           cities, paths and knobs, all overridable by env vars
  extract.py          Open-Meteo -> landed JSON, one logical date per run
  load.py             delete-then-insert into raw.weather_daily
  warehouse.py        DuckDB connection and the raw DDL
  dbt_runner.py       runs dbt in-process, returns a result object
  cli.py              python -m pipelines.cli extract|load|run|backfill|dbt-run|dbt-test
dags/                 weather_pipeline.py, the one DAG
dbt/weather/          source, staging model, two marts, tests and docs
notebooks/            walkthrough.ipynb
scripts/              reproduce.sh, run_notebook.py, bootstrap_local.sh
data/                 landing/ for raw JSON, warehouse/ for the DuckDB file (both gitignored)
```

## A few decisions worth explaining

**One run owns one logical date.** A run for date D loads weather for D minus
`ARCHIVE_LAG_DAYS` (5 by default), because the archive endpoint trails real time by a few
days. That mapping lives in `target_date_for()` and nowhere else, so the DAG, the CLI and the
notebook can't disagree about which day a run is responsible for.

**Loading is delete-then-insert on that date**, in one transaction. Re-run it however you like
(retry, by hand, backfill) and it deletes that date's rows and puts them back, leaving
everything else alone. The `unique` test on `city_id|weather_date` is the alarm if that ever
stops being true.

**Raw stays raw.** `raw.weather_daily` keeps the API's `daily` fields and the response
metadata as JSON, exactly as they arrived. Only `stg_weather_daily` knows those field names,
so if Open-Meteo renames something it's a one-file change. Anything we add about our own
processing is prefixed with `_`.

**The warehouse is DuckDB**, a single file in `data/warehouse/`. No database service to wait
on, which is why the notebook runs the same on a laptop, in the container and in CI. The SQL
and the load pattern move to Postgres without much thought if this ever needed to.

**The versions are pinned hard**, FastAPI and Starlette included. Airflow 3.0.4 fails every
task start with a 422 if pip resolves a newer FastAPI than it expects, and that is not a fun
afternoon. There's a comment at the top of `requirements.txt` saying so.

Configuration lives in `.env` (copy `.env.example`). Paths default to repo-relative locations
locally and are set to their container paths by `docker-compose.yml`.
