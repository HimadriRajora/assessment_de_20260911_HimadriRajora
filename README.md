# Daily weather pipeline

One small, end-to-end pipeline: daily weather for five cities from the free
[Open-Meteo archive API](https://open-meteo.com/en/docs/historical-weather-api), loaded into a
warehouse, modelled with dbt, orchestrated by Airflow, and explained in a notebook.

```
Open-Meteo API  ──extract──▶  raw.weather_daily (DuckDB)
                              │
                              └──dbt──▶ staging.stg_weather_daily ──▶ marts.mart_city_weather_daily
                                                                 ──▶ marts.mart_city_weather_summary
                                                                     (tests + docs)
                 Airflow DAG:  extract → load → dbt run → dbt test   (daily, backfillable)
                 Notebook:     runs every stage, shows the results, explains the choices
```

Cities: Berlin, London, New York, Tokyo, Sydney. Window: the last 30 days.

## Reproducibility

```bash
cp .env.example .env
make up          # airflow (standalone) + jupyter
make reproduce   # executes notebooks/walkthrough.ipynb headlessly
```

`make reproduce` runs the whole pipeline — extract, load, a 30-day backfill, `dbt run`,
`dbt test` — by executing the notebook, and fails loudly if any cell raises.

No Docker? The same thing runs in a local venv:

```bash
make up-local        # builds .venv on Python 3.12 (installs uv if needed)
make reproduce-local
```

## Getting started

```bash
make up          # airflow (standalone), jupyter
make airflow-ui  # http://localhost:8080  (admin / generated password)
make notebook    # http://localhost:8888  (JupyterLab, no token)
make dbt         # dbt run inside the container
make reproduce   # execute the notebook headlessly (what reviewers run)
make down
```

The DAG arrives paused, which is Airflow's default. Unpause it in the UI to watch it catch up
from its start date, or drive a backfill yourself with `make airflow-backfill`.

## 1. Extract & load (Python)

`pipelines/extract.py`, `pipelines/load.py`

One run loads **one logical date**. A run for logical date *D* loads weather for
*D − ARCHIVE_LAG_DAYS* (5 by default), because the archive endpoint trails real time by a few
days; that mapping lives in `target_date_for()` and nowhere else. `make backfill
BACKFILL_DAYS=30` loads a range.

**Re-running a date does not duplicate rows.** The mechanism is **delete-then-insert on the
run's date, inside one transaction**: the run owns exactly one weather date, so it deletes that
date's rows and re-inserts them, leaving every other date untouched. Chosen over upsert because
the run's unit of work *is* the partition — it needs no conflict target, it self-heals if the
API changes its mind about a day, and a failed insert rolls back rather than leaving a
half-written date.

**Raw stays raw.** `raw.weather_daily` stores the API's `daily` fields and response metadata as
verbatim JSON. Nothing is renamed, cast or converted before it lands. Columns describing our own
processing are prefixed with `_` (`_batch_id`, `_extracted_at`, `_loaded_at`).

**Timeouts and retries.** Every HTTP call carries a timeout (`REQUEST_TIMEOUT_SECONDS`, default
30s) and retries with exponential backoff on 429 and 5xx via urllib3. Airflow retries the task
twice on top of that.

## 2. Transform (dbt)

`dbt/weather/`

* **Source**: `raw.weather_daily`, declared in `models/sources.yml` with freshness thresholds
  and a description for every column.
* **Staging**: `stg_weather_daily` (view) parses the JSON payload into typed, renamed columns.
  It is the only model that knows the API's field names, so an upstream rename is a one-file
  change. A view because it is a thin cast layer with nothing to gain from materialising.
* **Marts** (tables): `mart_city_weather_daily`, the daily report per city enriched with a
  7-day rolling mean and the day-over-day change; and `mart_city_weather_summary`, one row per
  city over the whole window — the table a non-technical colleague would read first.

**34 tests.** Schema tests cover keys, nulls and allowed values: `unique` and `not_null` on the
grain key `city_id|weather_date`, `not_null` on every measurement, `accepted_values` on `city_id`
and `country_code`, and a `relationships` test from the mart back to staging. On top of those,
five singular tests cover ranges and shape — `max >= min`, plausible temperatures (catching a
sentinel value or a switch to Fahrenheit), non-negative precipitation, no duplicate
(city, date) pairs in raw, and every city present for every day.

`make dbt-docs` serves model and column documentation on :8081.

## 3. Orchestrate (Airflow)

`dags/weather_pipeline.py`

One DAG, `extract → load → dbt run → dbt test`, scheduled `@daily`. Every task is a thin
wrapper around a function in `pipelines/`, so the notebook and the DAG cannot drift apart.

* Driven by the **logical date** — the task signature takes `logical_date` and passes it to
  `target_date_for()`. Nothing reads "today", so `airflow backfill` replays cleanly.
* `catchup=True` so a backfill covers the range; `max_active_runs=1` so runs serialise on the
  shared warehouse.
* Retries: 2 with a 2-minute delay, and a 30-second HTTP timeout underneath.
* Idempotent on rerun: extract overwrites its own date-named landing file, load replaces that
  date's rows, dbt rebuilds the models in full.

## 4. Walk through (notebook)

`notebooks/walkthrough.ipynb`, committed with outputs.

It imports `pipelines` and calls the same functions the DAG calls — no logic is re-implemented.
Each stage is followed by evidence: row counts, sample rows, and the full `dbt run` / `dbt test`
output. Section 3 loads the same date twice and prints the counts either side. Section 6 inserts
a duplicate row on purpose so you can watch the grain test fail, then repairs it and watches it
pass. Section 7 queries the mart. Section 8 runs the real DAG end to end with
`airflow dags test`.

## Verifying

| Command | What it proves |
|---|---|
| `make reproduce` | the whole pipeline runs; the notebook re-executes top to bottom |
| `make dag-test DATE=2026-09-08` | Airflow executes the real DAG for one logical date |
| `make backfill BACKFILL_DAYS=30` | 30 logical dates load through the CLI |
| `make test` | Python unit tests: extract mapping, load idempotency, DAG wiring |
| `make dbt-run` / `make dbt-test` | 3 models build, 34 tests pass |
| `make dbt-docs` | model and column documentation on :8081 |

Re-run safety in one line — run it twice, the counts don't move:

```bash
make backfill BACKFILL_DAYS=3 && make backfill BACKFILL_DAYS=3
```

## Layout

```
pipelines/            the pipeline itself
  config.py           cities, paths and knobs, all overridable by env vars
  extract.py          Open-Meteo -> landed JSON, one logical date per run
  load.py             delete-then-insert into raw.weather_daily
  warehouse.py        warehouse connection and the raw DDL
  dbt_runner.py       runs dbt in-process, returns a typed result
  cli.py              python -m pipelines.cli extract|load|run|backfill|dbt-run|dbt-test
dags/                 weather_pipeline.py, the one DAG
dbt/weather/          source, staging model, two marts, tests and docs
notebooks/            walkthrough.ipynb
scripts/              reproduce.sh, run_notebook.py, bootstrap_local.sh
tests/                pytest unit tests
data/                 landing/ for raw JSON, warehouse/ for the DuckDB file (both gitignored)
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for the data flow, the grain, the idempotency mechanism
and the trade-offs behind each choice. [NOTES.md](NOTES.md) covers time spent, known gaps and
AI usage.

## One deviation worth stating up front

**The warehouse is DuckDB, not Postgres.** It is a single file under `data/warehouse/`, which
means no database service in the critical path and a notebook that behaves identically on a
laptop, in the container and in CI. The cost is DuckDB's single-writer rule, and the SQL and the
delete-then-insert load pattern are already what you would write against Postgres. Moving over
is a profile change plus swapping the JSON accessors in the staging model. ARCHITECTURE.md spells
out exactly what would change.
