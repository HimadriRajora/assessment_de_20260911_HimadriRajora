"""Test fixtures. Nothing here touches the network or the real warehouse."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from pipelines.config import CITIES, Settings

WEATHER_DATE = date(2026, 9, 1)

API_RESPONSE = {
    "latitude": 52.54833,
    "longitude": 13.407822,
    "elevation": 38.0,
    "generationtime_ms": 0.05,
    "utc_offset_seconds": 0,
    "timezone": "GMT",
    "timezone_abbreviation": "GMT",
    "daily_units": {
        "time": "iso8601",
        "temperature_2m_max": "°C",
        "temperature_2m_min": "°C",
        "temperature_2m_mean": "°C",
        "precipitation_sum": "mm",
        "wind_speed_10m_max": "km/h",
    },
    "daily": {
        "time": [WEATHER_DATE.isoformat()],
        "temperature_2m_max": [21.4],
        "temperature_2m_min": [11.2],
        "temperature_2m_mean": [16.1],
        "precipitation_sum": [0.0],
        "wind_speed_10m_max": [18.7],
    },
}


@pytest.fixture()
def tmp_settings(tmp_path: Path) -> Settings:
    return Settings(
        warehouse_path=tmp_path / "warehouse" / "test.duckdb",
        landing_dir=tmp_path / "landing",
        dbt_project_dir=tmp_path / "dbt",
        archive_lag_days=5,
        request_timeout_seconds=5,
    )


@pytest.fixture()
def sample_records() -> list[dict]:
    """One raw record per city, matching what extract produces."""
    return [
        {
            "city_id": city.city_id,
            "city_name": city.name,
            "country_code": city.country,
            "weather_date": WEATHER_DATE.isoformat(),
            "payload": json.dumps({"time": WEATHER_DATE.isoformat(), "temperature_2m_mean": 16.1}),
            "api_meta": json.dumps({"timezone": "GMT"}),
            "_batch_id": WEATHER_DATE.isoformat(),
            "_extracted_at": "2026-09-06T00:00:00+00:00",
        }
        for city in CITIES
    ]
