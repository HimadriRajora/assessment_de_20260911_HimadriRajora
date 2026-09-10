#!/usr/bin/env bash
# End-to-end reproduction. This is the single definition of "run the whole
# thing", called by both `make reproduce` (Docker) and `make reproduce-local`.
#
# The notebook is the pipeline run: it extracts, loads, backfills, runs dbt and
# runs the dbt tests using the same functions the DAG calls. Executing it here
# means the committed outputs cannot drift from what the code actually does.
set -euo pipefail

PYTHON="${PYTHON:-python}"

cd "$(dirname "$0")/.."

echo "==> Executing notebooks/walkthrough.ipynb (extract -> load -> dbt -> tests)"
"$PYTHON" scripts/run_notebook.py

echo
echo "==> Done."
echo "    Warehouse : ${WAREHOUSE_PATH:-data/warehouse/weather.duckdb}"
echo "    Notebook  : notebooks/walkthrough.ipynb (re-executed with fresh outputs)"
