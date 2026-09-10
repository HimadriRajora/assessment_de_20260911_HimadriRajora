# Notes

## Time spent

About four hours:

* ~35 min getting a working environment (see below)
* ~60 min on the `pipelines` package: extract, load, warehouse, dbt runner, CLI
* ~50 min on the dbt project: source, staging, two marts, docs and tests
* ~35 min on the Airflow DAG, most of it getting a real DAG run to go green
* ~50 min on the notebook, including running it end to end for the committed outputs
* ~40 min on the README, Makefile and Docker files

## About the machine I built this on

Worth saying, because it shaped two decisions.

There is no Docker on this machine, no root, and the only Python is 3.14, which neither
Airflow 3 nor dbt support yet. So I used `uv` to pull a private Python 3.12 and build a venv
against it. Rather than leave that as a local hack, it's `make up-local` /
`scripts/bootstrap_local.sh`, so anyone can reproduce the same setup.

Everything in the repo was actually run: the API extract, the loads, a 30-date backfill,
`dbt run`, `dbt test` and a full `airflow dags test` DAG run. The notebook was executed top to
bottom and committed with its outputs.

## Known gaps

**The Docker path is written but I never got to run it**, because there's no Docker here. I
kept it as small as I could for that reason: one image, two services, no database container,
the same pinned requirements I know work. But I can't claim to have watched `make up` come up.
`make up-local` and `make reproduce-local` I've run many times, and they reach the same place.

**DuckDB instead of Postgres.** A file-based warehouse means nothing to wait on and a notebook
that runs anywhere, which mattered more to me here than the engine. The cost is DuckDB's
one-writer rule: if Airflow is mid-run, a notebook write will block. In practice the DAG
arrives paused so it only bites if you unpause it and run the notebook at the same time.
Moving to Postgres is a profile change plus swapping the JSON accessors in staging; the
delete-then-insert load is already the pattern you'd use there.

**Backfill is sequential.** 30 dates x 5 cities is about 150 requests and roughly 30 seconds,
so it didn't seem worth parallelising. A wider window would want either concurrency or one
multi-day API call per run. The API takes a date range; I kept `start_date == end_date` so a
run maps to exactly one partition.

**No alerting.** Failures show up in the Airflow UI and nowhere else. No SLA, no callbacks.

**`dbt source freshness` is configured but nothing schedules it.** The thresholds are in
`sources.yml`.

**Airflow's metadata DB is SQLite**, via `airflow standalone`. Fine for one DAG on a laptop,
wrong for anything real.

**No CI.** `make reproduce` is the whole check. A small GitHub Action running a short backfill
plus `dbt test` would be the obvious next thing.

**`accepted_values` on `city_id` hard-codes the five cities**, so adding one means editing
`config.py` and `stg_weather_daily.yml`. That's deliberate, since the test exists to catch a
city showing up that nobody asked for, but it is a second place to remember.

## AI tools

I built this with Claude (Claude Code) alongside me, and used it heavily for first drafts:
the pipeline modules, the dbt models, the DAG, the Makefile and Docker files, and the first
pass of the notebook and this file. I then ran everything myself and worked through what
broke, which is where most of the time went. The things that only showed up by running it:

* Airflow 3.0.4 returning a 422 on every single task start, because pip had resolved a newer
  FastAPI/Starlette stack than it supports. Fixed by pinning Airflow's own constraint set, and
  there's a comment in `requirements.txt` so nobody has to find that twice.
* dbt 1.10 wanting generic test config nested under `arguments:`.
* dbt-duckdb caching an open write handle on a class attribute for the life of the process,
  which locked the Airflow task in another process out of the warehouse. It failed its load,
  burned both retries and only then went red, which took a while to understand. Handled in
  `dbt_runner.release_warehouse_connection()`.
* The API rejecting a `start_date` with a time component, after a DuckDB date came back as a
  pandas `Timestamp`.

The design calls were mine: one logical date per run, land before load, delete-then-insert for
idempotency, raw kept verbatim, staging as a view with marts as tables, DuckDB over Postgres,
and calling the JSON endpoint rather than the `openmeteo_requests` SDK from the brief (the SDK
decodes into numpy, so nothing reaches the raw table unmodified). I also decided the notebook
should break a test on purpose, because a wall of green tests tells you nothing about whether
they'd catch anything.

I checked the row counts and the mart figures against the raw JSON by hand, and read the DAG
run output rather than trusting the exit code.
