# Architecture

How the pipeline is put together, and why each piece is the way it is.

## Data flow

```
                    ┌──────────────────────────────────────────────┐
                    │  Open-Meteo archive API (no key)             │
                    │  GET /v1/archive?latitude=..&start_date=D..  │
                    └───────────────────┬──────────────────────────┘
                                        │ one request per city, start_date == end_date
                                        │ timeout 30s, retry on 429/5xx with backoff
                    ┌───────────────────▼──────────────────────────┐
   extract          │  data/landing/weather_<weather_date>.json     │
   pipelines/       │  one record per (city, day), API fields kept  │
   extract.py       │  verbatim as JSON                            │
                    └───────────────────┬──────────────────────────┘
                                        │ delete rows for this date, insert the batch,
                                        │ one transaction
                    ┌───────────────────▼──────────────────────────┐
   load             │  raw.weather_daily                            │
   pipelines/       │  grain: one row per (city_id, weather_date)    │
   load.py          │  payload JSON + api_meta JSON + _metadata      │
                    └───────────────────┬──────────────────────────┘
                                        │ dbt
                    ┌───────────────────▼──────────────────────────┐
   transform        │  staging.stg_weather_daily        (view)      │
   dbt/weather/     │  typed, renamed, surrogate key                │
                    └──────┬──────────────────────────┬────────────┘
                           │                          │
        ┌──────────────────▼─────────┐   ┌────────────▼──────────────────┐
        │ marts.mart_city_weather_   │   │ marts.mart_city_weather_      │
        │ daily            (table)   │──▶│ summary            (table)    │
        │ per city per day, rolling  │   │ per city, whole window        │
        │ 7d mean, day-over-day      │   │ avg/extremes/rain days        │
        └────────────────────────────┘   └───────────────────────────────┘
```

Airflow wraps the whole thing: `extract → load → dbt run → dbt test`, daily.

## The grain, and why it decides everything

Every design choice follows from one decision: **a run owns exactly one weather date.**

That gives the pipeline a natural partition key. The landing file is named after it
(`weather_2026-09-05.json`), the load deletes on it, and the `_batch_id` column records it. It
is also why the API call pins `start_date == end_date` rather than requesting a range: a run
maps to exactly one partition, so "re-run this date" has an unambiguous meaning.

Weather days are fixed to **UTC** (`timezone=UTC` on the request). Otherwise a "day" would mean
something different per city and a re-run could shift a row onto a neighbouring date, which
would quietly break the grain.

### Logical date vs weather date

The archive endpoint trails real time by a few days, so a run cannot load "yesterday". A run for
logical date *D* loads weather for *D − ARCHIVE_LAG_DAYS* (default 5):

```python
def target_date_for(logical_date, settings=None) -> date:
    return logical_date - timedelta(days=settings.archive_lag_days)
```

This lives in exactly one place. The DAG, the CLI and the notebook all call it, so they cannot
disagree about which day a run is responsible for, and a backfill over logical dates maps
one-to-one onto weather dates.

## Idempotency: delete-then-insert on the partition

```sql
BEGIN;
  DELETE FROM raw.weather_daily WHERE weather_date = ?;
  INSERT INTO raw.weather_daily (...) VALUES (...);  -- the batch for that date
COMMIT;
```

**Why this over the alternatives:**

| Mechanism | Why not |
|---|---|
| `INSERT ... ON CONFLICT` (upsert) | Needs a conflict target and keeps stale rows if the API stops returning a city; the run would have to reason about deletions separately. |
| Append + dedupe downstream | Pushes the problem into dbt, makes the raw table grow without bound, and means the raw row count no longer tells you anything. |
| Partition overwrite (table per date) | Right answer at volume, overkill for 150 rows, and awkward to query. |

Delete-then-insert matches the unit of work exactly. The run owns the date, so it is entitled to
replace it wholesale. It self-heals — if the API revises a day's measurements, the next run picks
up the correction rather than keeping both versions. And because it is one transaction, a failure
mid-insert rolls back rather than leaving the date half-written.

The cost is that it is a write amplification: re-running rewrites every row for the date even if
nothing changed. At five rows per date that is free.

**The test that guards it:** `unique` on `weather_key` (`city_id|weather_date`) in
`stg_weather_daily.yml`. If the load ever starts appending, that test goes red on the next run.
The notebook demonstrates this on purpose by inserting a duplicate row.

## Raw table shape

```sql
CREATE TABLE raw.weather_daily (
    city_id        VARCHAR   NOT NULL,  -- our key; the API only knows coordinates
    city_name      VARCHAR   NOT NULL,
    country_code   VARCHAR   NOT NULL,
    weather_date   DATE      NOT NULL,  -- partition key
    payload        JSON      NOT NULL,  -- API daily fields, unmodified
    api_meta       JSON      NOT NULL,  -- response-level fields, unmodified
    _batch_id      VARCHAR   NOT NULL,  -- the weather date this run covered
    _extracted_at  TIMESTAMP NOT NULL,
    _loaded_at     TIMESTAMP NOT NULL
);
```

Two rules hold here. **Nothing from the API is modified** — no renaming, no unit conversion, no
casting; `payload` is the API's own field names and values, serialised. And **anything we add
about our own processing is prefixed with `_`**, so it is obvious at a glance which columns came
from the source and which came from the pipeline.

`city_id` is ours rather than the API's, because the API has no concept of a city — only
coordinates, and it returns the centre of the grid cell it actually served, which is not the
coordinate you asked for. A stable key we assign is what makes joins and tests possible.

### Why the JSON endpoint rather than the `openmeteo_requests` SDK

The SDK decodes the flatbuffer response straight into numpy arrays. That is convenient, but it
means the payload has already been transformed before anything can store it, and "keep the API
fields unmodified" becomes impossible to honour. The JSON endpoint is the same URL and the same
data, so the pipeline uses plain `requests` and keeps the response as it arrived.

## Model layer

| Model | Materialisation | Why |
|---|---|---|
| `stg_weather_daily` | view | A thin cast-and-rename layer over raw. Nothing to gain from materialising, and it can never be stale. It is the only model that knows the API's field names, so an upstream rename is a one-file change. |
| `mart_city_weather_daily` | table | What people query. Same grain as staging, but each row is answerable on its own — it carries the 7-day rolling mean and the day-over-day change. |
| `mart_city_weather_summary` | table | One row per city over the whole window. The "which city was warmest, where did it rain most" table. |

Marts rebuild in full on every run. At 5 cities × 30 days that is cheaper than maintaining
incremental state, and it means a corrected backfill shows up immediately instead of needing a
`--full-refresh`. The point at which this stops being true is roughly when a rebuild takes longer
than the schedule interval; at that point `mart_city_weather_daily` becomes incremental on
`weather_date`, which is already the partition key.

Custom schemas (`staging`, `marts`) come from a `generate_schema_name` macro override, so
relations read as `marts.mart_city_weather_daily` rather than dbt's default of prefixing the
target schema.

## Testing strategy

Tests are declared in `sources.yml`, `stg_weather_daily.yml` and `marts.yml`, plus five singular
tests in `dbt/weather/tests/` — 34 in total. There is also a small pytest suite under `tests/`
covering the extract mapping, load idempotency and the DAG's wiring.

| Test | The regression it catches |
|---|---|
| `unique` on `weather_key` | the load losing idempotency and appending |
| `not_null` on the grain columns | a city or date going missing mid-load |
| `not_null` on every measurement | a city whose extract came back empty while the run stayed green |
| `accepted_values` on `city_id`, `country_code` | a city appearing that nobody configured |
| `relationships` mart → staging | a mart row with no staging row behind it |
| `assert_temperature_max_ge_min` | a column swap in staging |
| `assert_temperatures_are_plausible` | a sentinel value (-9999) or a switch to Fahrenheit/Kelvin |
| `assert_precipitation_is_non_negative` | physically impossible measurements |
| `assert_every_city_has_every_day` | a city whose extract failed while the run stayed green |

Source freshness thresholds are declared in `sources.yml` but are **not** part of `dbt test` —
they need `dbt source freshness`, which nothing currently schedules. That is a known gap.

The dbt tests run as the last task of the DAG, so a data regression fails the run rather than
landing quietly in the mart.

## Orchestration

```
extract ──▶ load ──▶ transform (dbt run) ──▶ test (dbt test)
```

Each task is a thin wrapper around a function in `pipelines/`. The DAG contains no logic of its
own, which is what lets the notebook run the identical code path.

| Setting | Value | Why |
|---|---|---|
| `schedule` | `@daily` | one run per logical date |
| `catchup` | `True` | backfills replay the range instead of jumping to now |
| `max_active_runs` | `1` | runs share one warehouse; serialising keeps it consistent |
| `retries` | `2`, 2-minute delay | absorbs a 429 or a blip without waking anyone |
| HTTP timeout | 30s | a hung request fails the task instead of holding a slot |

**Task boundaries.** Extract and load are separate tasks even though they could be one. They
fail for different reasons — the API being down is not the warehouse being down — and separating
them means a load failure can be retried without hitting the API again, since the landed file is
still on disk.

**What crosses between tasks** is the landing file path, over XCom. The data itself goes through
the filesystem, not XCom, so the payload size never becomes an orchestration problem.

## Warehouse choice

DuckDB, a single file under `data/warehouse/`.

**Why:** no database service in the critical path. `make reproduce` behaves identically on a
laptop, in the container and in CI, and the notebook has nothing to wait for. For a pipeline this
size, a Postgres container is a dependency to manage rather than a capability being used.

**The cost:** DuckDB permits one writer per file. If an Airflow run is mid-flight, a notebook
write blocks. In practice the DAG arrives paused so this only bites if you unpause it and run the
notebook simultaneously. It also means dbt's cached connection has to be released explicitly
after every invocation — `dbt_runner.release_warehouse_connection()` does this, because
dbt-duckdb otherwise holds the handle for the life of the process and locks out the next writer.

**Moving to Postgres** would be: a `profiles.yml` target change; `payload ->> 'field'` in the
staging model becomes `payload->>'field'` on a `jsonb` column (near-identical); `pipelines/
warehouse.py` swaps `duckdb.connect` for a psycopg connection; and `docker-compose.yml` gains a
postgres service with a healthcheck the other services wait on. The load pattern — delete on the
partition key, insert, one transaction — is already the Postgres one, and nothing about the grain,
the models or the tests changes.

## Failure modes

| What breaks | What happens | What you do |
|---|---|---|
| API 429 / 5xx | urllib3 retries with backoff; then the task fails and Airflow retries twice | usually nothing |
| API returns no rows for a day | load writes zero rows for that date and logs a warning; the column tests still pass, because there is nothing to violate — the gap shows up as a missing date in the mart | re-run the date once the archive catches up |
| Load fails mid-insert | transaction rolls back; the date keeps its previous contents | retry the task; the landed file is still there |
| dbt model fails | `transform` task fails, `test` never runs, mart keeps its last good state | fix the model, re-run |
| A test fails | `test` task fails the run; the mart is already rebuilt, so treat it as an alert | investigate before trusting the mart |
| Two writers at once | DuckDB raises a lock error | run one at a time (the DAG is paused by default) |

## What I would change at 100× the volume

* `mart_city_weather_daily` becomes incremental on `weather_date`.
* Extract fans out — one mapped task per city, or one API call per run covering a date range,
  rather than 5 sequential requests.
* Postgres or a columnar warehouse instead of a single DuckDB file, so concurrent readers and
  writers stop being a consideration.
* Landing files go to object storage with a date prefix, making the raw layer replayable
  independently of the warehouse.
* Alerting on task failure and on `dbt source freshness`, neither of which exists today.
