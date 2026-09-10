"""Warehouse access.

DuckDB is the warehouse: a single file that Airflow, dbt and the notebook all
open. That removes a database service from the critical path, which is what
lets ``make reproduce`` run identically on a laptop and in CI, while the SQL
(and the idempotency strategy) is the same shape you would write for Postgres.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterator

import duckdb

from pipelines.config import Settings, settings as default_settings

log = logging.getLogger(__name__)

RAW_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {relation} (
    city_id        VARCHAR   NOT NULL,
    city_name      VARCHAR   NOT NULL,
    country_code   VARCHAR   NOT NULL,
    weather_date   DATE      NOT NULL,
    payload        JSON      NOT NULL,  -- API fields, unmodified
    api_meta       JSON      NOT NULL,  -- response-level fields, unmodified
    _batch_id      VARCHAR   NOT NULL,  -- the weather date this run covered
    _extracted_at  TIMESTAMP NOT NULL,
    _loaded_at     TIMESTAMP NOT NULL
)
"""


@contextmanager
def connect(
    settings: Settings | None = None, read_only: bool = False
) -> Iterator[duckdb.DuckDBPyConnection]:
    """Open the warehouse, creating its parent directory on first use."""
    settings = settings or default_settings
    settings.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        con = duckdb.connect(str(settings.warehouse_path), read_only=read_only)
    except duckdb.ConnectionException:
        if not read_only:
            raise
        # DuckDB refuses a second connection to a file that is already open in
        # this process with a different config -- which happens whenever dbt
        # has been invoked in-process (the notebook does exactly that). Reading
        # is still safe, so share the existing configuration instead of failing.
        log.debug("Falling back to a read-write connection for %s", settings.warehouse_path)
        con = duckdb.connect(str(settings.warehouse_path))
    try:
        yield con
    finally:
        con.close()


def ensure_raw_table(
    con: duckdb.DuckDBPyConnection, settings: Settings | None = None
) -> None:
    settings = settings or default_settings
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {settings.raw_schema}")
    con.execute(RAW_TABLE_DDL.format(relation=settings.raw_relation))


def row_count(relation: str, settings: Settings | None = None) -> int:
    with connect(settings, read_only=True) as con:
        return con.execute(f"SELECT count(*) FROM {relation}").fetchone()[0]
