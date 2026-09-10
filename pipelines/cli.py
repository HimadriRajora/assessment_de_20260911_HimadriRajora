"""Thin CLI over the pipeline, so every stage is runnable by hand.

    python -m pipelines.cli extract --logical-date 2026-09-10
    python -m pipelines.cli load    --logical-date 2026-09-10
    python -m pipelines.cli run     --logical-date 2026-09-10   # extract+load
    python -m pipelines.cli backfill --days 30
    python -m pipelines.cli dbt-run
    python -m pipelines.cli dbt-test
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta

from pipelines.config import settings
from pipelines.dbt_runner import dbt_run, dbt_test, run_or_raise
from pipelines.extract import extract_to_landing, target_date_for
from pipelines.load import load_from_landing


def _parse_date(value: str | None) -> date:
    return date.fromisoformat(value) if value else date.today()


def run_date(logical_date: date) -> None:
    path = extract_to_landing(logical_date)
    print(f"extracted -> {path}")
    result = load_from_landing(target_date_for(logical_date))
    print(result)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

    parser = argparse.ArgumentParser(description="Weather pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    for name in ("extract", "load", "run"):
        p = sub.add_parser(name)
        p.add_argument("--logical-date", help="YYYY-MM-DD (defaults to today)")

    p_backfill = sub.add_parser("backfill")
    p_backfill.add_argument("--days", type=int, default=30)
    p_backfill.add_argument("--end-date", help="last logical date (defaults to today)")

    sub.add_parser("dbt-run")
    sub.add_parser("dbt-test")

    args = parser.parse_args(argv)

    if args.command == "extract":
        print(f"extracted -> {extract_to_landing(_parse_date(args.logical_date))}")
    elif args.command == "load":
        print(load_from_landing(target_date_for(_parse_date(args.logical_date))))
    elif args.command == "run":
        run_date(_parse_date(args.logical_date))
    elif args.command == "backfill":
        end = _parse_date(args.end_date)
        for offset in range(args.days - 1, -1, -1):
            run_date(end - timedelta(days=offset))
    elif args.command == "dbt-run":
        print(run_or_raise(dbt_run()).summary())
    elif args.command == "dbt-test":
        print(run_or_raise(dbt_test()).summary())

    print(f"warehouse: {settings.warehouse_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
