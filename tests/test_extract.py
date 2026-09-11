from datetime import date, datetime, timezone

import pytest

from pipelines.config import CITIES
from pipelines.extract import _to_records, target_date_for, write_landing, read_landing
from tests.conftest import API_RESPONSE, WEATHER_DATE


def test_target_date_applies_the_archive_lag(tmp_settings):
    assert target_date_for(date(2026, 9, 10), tmp_settings) == date(2026, 9, 5)


def test_target_date_accepts_a_datetime(tmp_settings):
    logical = datetime(2026, 9, 10, 3, 30, tzinfo=timezone.utc)
    assert target_date_for(logical, tmp_settings) == date(2026, 9, 5)


def test_to_records_emits_one_row_per_day_with_untouched_payload():
    records = _to_records(CITIES[0], WEATHER_DATE, API_RESPONSE, batch_id="b1")

    assert len(records) == 1
    record = records[0]
    assert record["city_id"] == "berlin"
    assert record["weather_date"] == WEATHER_DATE.isoformat()

    import json

    payload = json.loads(record["payload"])
    # Field names and values are exactly what the API returned.
    assert payload["temperature_2m_max"] == 21.4
    assert payload["wind_speed_10m_max"] == 18.7
    assert set(payload) == {
        "time",
        "temperature_2m_max",
        "temperature_2m_min",
        "temperature_2m_mean",
        "precipitation_sum",
        "wind_speed_10m_max",
    }
    # Response-level fields are kept separately, also unmodified.
    assert json.loads(record["api_meta"])["timezone"] == "GMT"


def test_to_records_is_empty_when_the_api_returns_no_days():
    empty = {**API_RESPONSE, "daily": {"time": []}}
    assert _to_records(CITIES[0], WEATHER_DATE, empty, batch_id="b1") == []


def test_landing_round_trip(tmp_settings, sample_records):
    path = write_landing(sample_records, WEATHER_DATE, tmp_settings)
    assert path.name == f"weather_{WEATHER_DATE.isoformat()}.json"
    assert read_landing(WEATHER_DATE, tmp_settings) == sample_records


def test_landing_rerun_overwrites_rather_than_appends(tmp_settings, sample_records):
    write_landing(sample_records, WEATHER_DATE, tmp_settings)
    write_landing(sample_records, WEATHER_DATE, tmp_settings)
    assert len(read_landing(WEATHER_DATE, tmp_settings)) == len(sample_records)
