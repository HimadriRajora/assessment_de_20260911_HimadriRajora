#!/usr/bin/env bash
# Create .venv with the pinned runtime, without Docker.
# Uses uv (installed on demand) so we get a matching Python 3.12 even if the
# host only has some other version.
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v uv >/dev/null 2>&1; then
  export PATH="$HOME/.local/bin:$PATH"
fi
if ! command -v uv >/dev/null 2>&1; then
  echo "==> Installing uv"
  curl -LsSf https://astral.sh/uv/install.sh | sh
  export PATH="$HOME/.local/bin:$PATH"
fi

echo "==> Creating .venv (Python 3.12)"
uv python install 3.12
uv venv --python 3.12 .venv

echo "==> Installing pinned requirements"
VIRTUAL_ENV=.venv uv pip install -r requirements.txt

echo "==> Ready. Run: make reproduce-local"
