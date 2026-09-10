"""Load landed records into the raw table, idempotently.

Idempotency strategy: **delete-then-insert on the run's partition key**. A run
owns exactly one weather date, so re-running it deletes that date's rows and
re-inserts them inside a single transaction. Re-running never duplicates and
never leaves a half-written date behind. The same pattern works unchanged on
Postgres, where it would be a ``DELETE ... WHERE weather_date = %s`` in the
same transaction as the ``INSERT``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from pipelines.config import Settings, settings as default_settings
from pipelines.extract import read_landing
from pipelines.warehouse import connect, ensure_raw_table

log = logging.getLogger(__name__)

_COLUMNS = (
    "city_id",
    "city_name",
    "country_code",
    "weather_date",
    "payload",
    "api_meta",
    "_batch_id",
    "_extracted_at",
    "_loaded_at",
)


@dataclass(frozen=True)
class LoadResult:
    weather_date: date
    rows_deleted: int
    rows_inserted: int
    total_rows_in_table: int

    def __str__(self) -> str:  # pragma: no cover - display helper
        return (
            f"load {self.weather_date}: deleted={self.rows_deleted} "
            f"inserted={self.rows_inserted} table_total={self.total_rows_in_table}"
        )


def load_weather(
    records: Sequence[dict[str, Any]],
    weather_date: date,
    settings: Settings | None = None,
) -> LoadResult:
    """Replace all rows for ``weather_date`` with ``records``."""
    settings = settings or default_settings
    loaded_at = datetime.now(timezone.utc)
    relation = settings.raw_relation

    rows = [
        tuple(
            [
                r["city_id"],
                r["city_name"],
                r["country_code"],
                r["weather_date"],
                r["payload"],
                r["api_meta"],
                r["_batch_id"],
                r["_extracted_at"],
                loaded_at,
            ]
        )
        for r in records
    ]

    with connect(settings) as con:
        ensure_raw_table(con, settings)
        con.execute("BEGIN TRANSACTION")
        try:
            deleted = con.execute(
                f"SELECT count(*) FROM {relation} WHERE weather_date = ?",
                [weather_date],
            ).fetchone()[0]
            con.execute(
                f"DELETE FROM {relation} WHERE weather_date = ?", [weather_date]
            )
            if rows:
                placeholders = ", ".join(["?"] * len(_COLUMNS))
                con.executemany(
                    f"INSERT INTO {relation} ({', '.join(_COLUMNS)}) "
                    f"VALUES ({placeholders})",
                    rows,
                )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

        total = con.execute(f"SELECT count(*) FROM {relation}").fetchone()[0]

    result = LoadResult(weather_date, deleted, len(rows), total)
    log.info("%s", result)
    return result


def load_from_landing(
    weather_date: date, settings: Settings | None = None
) -> LoadResult:
    """Airflow-facing entrypoint: read the landed file, load it."""
    settings = settings or default_settings
    records = read_landing(weather_date, settings)
    return load_weather(records, weather_date, settings)


def load_landing_file(path: str | Path, settings: Settings | None = None) -> LoadResult:
    """Load a landed file by path (what the DAG receives over XCom)."""
    import json

    path = Path(path)
    records = json.loads(path.read_text(encoding="utf-8"))
    weather_date = date.fromisoformat(path.stem.replace("weather_", ""))
    return load_weather(records, weather_date, settings)
