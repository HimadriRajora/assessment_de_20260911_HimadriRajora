"""Extract one logical date of daily weather from the Open-Meteo archive API.

Design notes
------------
* One run = one logical date = one API call per city (``start_date == end_date``),
  which keeps a run small, cheap and trivially re-runnable.
* We call the plain JSON endpoint rather than the ``openmeteo_requests``
  flatbuffer SDK on purpose: the SDK decodes straight into numpy arrays, which
  means the raw payload is already transformed before it reaches the raw table.
  The brief asks for API fields to be stored unmodified, so we keep the JSON.
* Retries are handled by urllib3 with exponential backoff on the status codes
  Open-Meteo actually uses for throttling/outages (429, 5xx).
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipelines.config import (
    ARCHIVE_API_URL,
    CITIES,
    DAILY_VARIABLES,
    City,
    Settings,
    settings as default_settings,
)

log = logging.getLogger(__name__)


def target_date_for(logical_date: date | datetime, settings: Settings | None = None) -> date:
    """Map an Airflow logical date onto the weather date we can actually fetch.

    The archive endpoint trails real time by a few days, so a run for logical
    date D loads weather for ``D - ARCHIVE_LAG_DAYS``. Keeping this in one
    function means the DAG, the CLI and the notebook can never disagree.
    """
    settings = settings or default_settings
    if isinstance(logical_date, datetime):
        logical_date = logical_date.date()
    return logical_date - timedelta(days=settings.archive_lag_days)


def _session(retries: int = 5) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def _fetch_city(
    session: requests.Session, city: City, target_date: date, settings: Settings
) -> dict[str, Any]:
    params = {
        "latitude": city.latitude,
        "longitude": city.longitude,
        "start_date": target_date.isoformat(),
        "end_date": target_date.isoformat(),
        "daily": ",".join(DAILY_VARIABLES),
        # Fix the timezone so a "day" means the same thing for every city and
        # re-running never shifts a row onto a different date.
        "timezone": "UTC",
    }
    response = session.get(
        ARCHIVE_API_URL, params=params, timeout=settings.request_timeout_seconds
    )
    response.raise_for_status()
    return response.json()


def _to_records(
    city: City, target_date: date, payload: dict[str, Any], batch_id: str
) -> list[dict[str, Any]]:
    """Split one API response into one raw record per (city, day).

    Values are copied across verbatim -- no renaming, no unit conversion, no
    casting. Anything we add about *our* processing is prefixed with ``_``.
    """
    daily = payload.get("daily") or {}
    days: Sequence[str] = daily.get("time") or []
    api_meta = {k: v for k, v in payload.items() if k != "daily"}

    records: list[dict[str, Any]] = []
    for idx, day in enumerate(days):
        measurements = {
            field: values[idx]
            for field, values in daily.items()
            if field != "time" and isinstance(values, list) and idx < len(values)
        }
        records.append(
            {
                "city_id": city.city_id,
                "city_name": city.name,
                "country_code": city.country,
                "weather_date": day,
                # Unmodified API fields, exactly as returned.
                "payload": json.dumps({"time": day, **measurements}, sort_keys=True),
                "api_meta": json.dumps(api_meta, sort_keys=True),
                "_batch_id": batch_id,
                "_extracted_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    if not records:
        log.warning("No daily rows returned for %s on %s", city.city_id, target_date)
    return records


def extract_weather(
    target_date: date | datetime,
    cities: Iterable[City] = CITIES,
    settings: Settings | None = None,
) -> list[dict[str, Any]]:
    """Fetch one day of daily weather for every city. Returns raw records."""
    settings = settings or default_settings
    # Accept anything date-like (datetime, pandas Timestamp): the API rejects a
    # start_date carrying a time component.
    if isinstance(target_date, datetime):
        target_date = target_date.date()
    batch_id = f"{target_date.isoformat()}"
    records: list[dict[str, Any]] = []

    with _session() as session:
        for city in cities:
            payload = _fetch_city(session, city, target_date, settings)
            city_records = _to_records(city, target_date, payload, batch_id)
            log.info("Extracted %s row(s) for %s", len(city_records), city.city_id)
            records.extend(city_records)

    return records


def landing_path(target_date: date, settings: Settings | None = None) -> Path:
    settings = settings or default_settings
    return settings.landing_dir / f"weather_{target_date.isoformat()}.json"


def write_landing(
    records: list[dict[str, Any]], target_date: date, settings: Settings | None = None
) -> Path:
    """Land the raw records on disk before loading.

    Writing to a deterministic, date-keyed path means a re-run overwrites its
    own file instead of accumulating duplicates, and it gives us something to
    inspect when the API returns a surprise.
    """
    settings = settings or default_settings
    path = landing_path(target_date, settings)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(records, indent=2), encoding="utf-8")
    log.info("Wrote %s record(s) to %s", len(records), path)
    return path


def read_landing(target_date: date, settings: Settings | None = None) -> list[dict[str, Any]]:
    return json.loads(landing_path(target_date, settings).read_text(encoding="utf-8"))


def extract_to_landing(
    logical_date: date | datetime, settings: Settings | None = None
) -> Path:
    """Airflow-facing entrypoint: logical date in, landed file path out."""
    settings = settings or default_settings
    target = target_date_for(logical_date, settings)
    records = extract_weather(target, settings=settings)
    return write_landing(records, target, settings)
