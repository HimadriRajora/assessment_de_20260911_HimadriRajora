"""Run dbt in-process via ``dbtRunner``.

Shelling out to ``dbt`` would work, but the programmatic runner gives us a
typed result object (success flag, per-node status) that Airflow can fail on
precisely, and it means the notebook and the DAG invoke dbt the exact same way.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Sequence

from dbt.adapters.factory import reset_adapters
from dbt.cli.main import dbtRunner, dbtRunnerResult

from pipelines.config import Settings, settings as default_settings

log = logging.getLogger(__name__)


@dataclass
class DbtResult:
    command: str
    success: bool
    node_results: list[dict[str, Any]] = field(default_factory=list)

    def summary(self) -> str:
        if not self.node_results:
            return f"dbt {self.command}: {'PASS' if self.success else 'FAIL'}"
        lines = [f"dbt {self.command}: {'PASS' if self.success else 'FAIL'}"]
        for node in self.node_results:
            lines.append(f"  {node['status']:<8} {node['node']}")
        return "\n".join(lines)

    def __str__(self) -> str:  # pragma: no cover - display helper
        return self.summary()


def release_warehouse_connection() -> None:
    """Close the DuckDB handle dbt keeps cached for the life of the process.

    dbt-duckdb stores one environment -- and its open DuckDB connection -- on a
    class attribute, and DuckDB permits a single writer per file. Left open, it
    blocks the next writer: the notebook cell after a ``dbt run``, or an Airflow
    task in another process. This reaches into dbt-duckdb internals, so it is
    deliberately defensive: failing to release is worth a debug log, not a crash.
    """
    try:
        from dbt.adapters.duckdb.connections import DuckDBConnectionManager
    except ImportError:  # pragma: no cover - only if the adapter changes shape
        return

    try:
        environment = DuckDBConnectionManager._ENV
        if environment is not None and hasattr(environment, "close"):
            environment.close()
        DuckDBConnectionManager.close_all_connections()
    except Exception:  # pragma: no cover - best effort by design
        log.debug("Could not release dbt's DuckDB connection", exc_info=True)


def _invoke(args: Sequence[str], settings: Settings | None) -> DbtResult:
    settings = settings or default_settings
    project_dir = str(settings.dbt_project_dir)
    # profiles.yml reads WAREHOUSE_PATH, so dbt can only ever point at the same
    # warehouse file the loader just wrote to.
    settings.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    os.environ["WAREHOUSE_PATH"] = str(settings.warehouse_path)
    full_args = [
        *args,
        "--project-dir",
        project_dir,
        "--profiles-dir",
        project_dir,
    ]
    log.info("Running dbt %s", " ".join(full_args))

    try:
        result: dbtRunnerResult = dbtRunner().invoke(list(full_args))
    finally:
        # dbt caches its adapter, and dbt-duckdb caches the warehouse connection
        # behind it. DuckDB allows one writer per file, so both have to go or the
        # next writer -- a notebook cell, an Airflow task in another process --
        # is locked out.
        reset_adapters()
        release_warehouse_connection()

    nodes: list[dict[str, Any]] = []
    if result.result is not None and hasattr(result.result, "results"):
        for node_result in result.result.results:
            nodes.append(
                {
                    "node": node_result.node.name,
                    "status": str(node_result.status),
                    "message": node_result.message,
                }
            )

    dbt_result = DbtResult(command=args[0], success=bool(result.success), node_results=nodes)
    if not dbt_result.success:
        log.error("%s", dbt_result.summary())
    return dbt_result


def dbt_run(settings: Settings | None = None, select: str | None = None) -> DbtResult:
    args = ["run"] + (["--select", select] if select else [])
    return _invoke(args, settings)


def dbt_test(settings: Settings | None = None, select: str | None = None) -> DbtResult:
    args = ["test"] + (["--select", select] if select else [])
    return _invoke(args, settings)


def dbt_build(settings: Settings | None = None) -> DbtResult:
    return _invoke(["build"], settings)


def run_or_raise(result: DbtResult) -> DbtResult:
    """Turn a failed dbt result into an exception so Airflow marks the task red."""
    if not result.success:
        raise RuntimeError(result.summary())
    return result
