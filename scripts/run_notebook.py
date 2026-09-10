"""Execute the walkthrough notebook in place, keeping its outputs.

Used by `make reproduce` so the committed notebook is provably the notebook
that runs -- if a cell raises, this exits non-zero and the build fails.
"""

from __future__ import annotations

import sys
from pathlib import Path

import nbformat
from nbclient import NotebookClient

PROJECT_ROOT = Path(__file__).resolve().parent.parent
NOTEBOOK = PROJECT_ROOT / "notebooks" / "walkthrough.ipynb"


def main() -> int:
    notebook = nbformat.read(NOTEBOOK, as_version=4)
    client = NotebookClient(
        notebook,
        timeout=1800,
        kernel_name="python3",
        resources={"metadata": {"path": str(NOTEBOOK.parent)}},
        allow_errors=False,
    )
    client.execute()
    nbformat.write(notebook, NOTEBOOK)
    print(f"Executed and saved {NOTEBOOK.relative_to(PROJECT_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
