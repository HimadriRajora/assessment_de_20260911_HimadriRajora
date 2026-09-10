"""Central configuration: cities, paths and knobs, all overridable by env vars."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class City:
    """A location we pull weather for.

    ``city_id`` is our own stable key: the API has no notion of a city, only
    coordinates, so we need something durable to join and test on.
    """

    city_id: str
    name: str
    country: str
    latitude: float
    longitude: float


# Deliberately a handful. Adding a city is a one-line change and the pipeline
# stays idempotent because the raw table is keyed on (city_id, weather_date).
CITIES: tuple[City, ...] = (
    City("berlin", "Berlin", "DE", 52.52, 13.41),
    City("london", "London", "GB", 51.51, -0.13),
    City("new_york", "New York", "US", 40.71, -74.01),
    City("tokyo", "Tokyo", "JP", 35.69, 139.69),
    City("sydney", "Sydney", "AU", -33.87, 151.21),
)

# Daily variables requested from Open-Meteo. Order matters only for the
# flatbuffer SDK; we use the JSON API, which returns named arrays.
DAILY_VARIABLES: tuple[str, ...] = (
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "wind_speed_10m_max",
)

ARCHIVE_API_URL = "https://archive-api.open-meteo.com/v1/archive"


def _env_path(name: str, default: Path) -> Path:
    raw = os.environ.get(name)
    return Path(raw) if raw else default


@dataclass(frozen=True)
class Settings:
    warehouse_path: Path
    landing_dir: Path
    dbt_project_dir: Path
    archive_lag_days: int
    request_timeout_seconds: int
    raw_schema: str = "raw"
    raw_table: str = "weather_daily"

    @property
    def raw_relation(self) -> str:
        return f"{self.raw_schema}.{self.raw_table}"


def load_settings() -> Settings:
    """Build settings from the environment, falling back to repo-local paths."""
    return Settings(
        warehouse_path=_env_path(
            "WAREHOUSE_PATH", PROJECT_ROOT / "data" / "warehouse" / "weather.duckdb"
        ),
        landing_dir=_env_path("LANDING_DIR", PROJECT_ROOT / "data" / "landing"),
        dbt_project_dir=_env_path(
            "DBT_PROJECT_DIR", PROJECT_ROOT / "dbt" / "weather"
        ),
        archive_lag_days=int(os.environ.get("ARCHIVE_LAG_DAYS", "5")),
        request_timeout_seconds=int(os.environ.get("REQUEST_TIMEOUT_SECONDS", "30")),
    )


settings = load_settings()
