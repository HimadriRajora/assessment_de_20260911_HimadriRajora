from datetime import date

from pipelines.load import load_weather
from pipelines.warehouse import connect, ensure_raw_table
from tests.conftest import WEATHER_DATE


def _count(settings) -> int:
    with connect(settings, read_only=True) as con:
        return con.execute(f"SELECT count(*) FROM {settings.raw_relation}").fetchone()[0]


def test_first_load_inserts_every_record(tmp_settings, sample_records):
    result = load_weather(sample_records, WEATHER_DATE, tmp_settings)

    assert result.rows_deleted == 0
    assert result.rows_inserted == len(sample_records)
    assert _count(tmp_settings) == len(sample_records)


def test_reloading_the_same_date_does_not_duplicate(tmp_settings, sample_records):
    load_weather(sample_records, WEATHER_DATE, tmp_settings)
    result = load_weather(sample_records, WEATHER_DATE, tmp_settings)

    assert result.rows_deleted == len(sample_records)
    assert result.rows_inserted == len(sample_records)
    assert _count(tmp_settings) == len(sample_records)


def test_loading_a_second_date_adds_to_the_table(tmp_settings, sample_records):
    other_date = date(2026, 9, 2)
    other_records = [{**r, "weather_date": other_date.isoformat()} for r in sample_records]

    load_weather(sample_records, WEATHER_DATE, tmp_settings)
    load_weather(other_records, other_date, tmp_settings)

    assert _count(tmp_settings) == 2 * len(sample_records)


def test_reloading_one_date_leaves_other_dates_alone(tmp_settings, sample_records):
    other_date = date(2026, 9, 2)
    other_records = [{**r, "weather_date": other_date.isoformat()} for r in sample_records]
    load_weather(sample_records, WEATHER_DATE, tmp_settings)
    load_weather(other_records, other_date, tmp_settings)

    load_weather(sample_records, WEATHER_DATE, tmp_settings)

    assert _count(tmp_settings) == 2 * len(sample_records)


def test_an_empty_extract_clears_the_date_without_error(tmp_settings, sample_records):
    load_weather(sample_records, WEATHER_DATE, tmp_settings)
    result = load_weather([], WEATHER_DATE, tmp_settings)

    assert result.rows_inserted == 0
    assert _count(tmp_settings) == 0


def test_the_raw_table_is_created_on_demand(tmp_settings):
    with connect(tmp_settings) as con:
        ensure_raw_table(con, tmp_settings)
        ensure_raw_table(con, tmp_settings)  # idempotent DDL
    assert _count(tmp_settings) == 0
