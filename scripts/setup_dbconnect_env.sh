#!/usr/bin/env bash
# Creates/refreshes .venv-dbconnect, a venv separate from the project's main
# uv-managed one. databricks-connect and plain pyspark (pinned ==3.5.3 for
# tests/unit's local Delta session) fight at Spark-context init time if both
# are importable in the same env, so this stays fully isolated - nothing
# here touches pyproject.toml/uv.lock. Run `tests/integration` (Databricks
# Connect, real serverless cluster) via this venv's pytest; run `tests/unit`
# (local pyspark+delta-spark) via the normal `uv run pytest`.
set -euo pipefail

VENV_DIR=".venv-dbconnect"

uv venv "$VENV_DIR"
uv pip install --python "$VENV_DIR" databricks-connect pytest python-dotenv

if [ -f "$VENV_DIR/Scripts/pytest.exe" ]; then
  PYTEST="$VENV_DIR/Scripts/pytest.exe"  # Windows venv layout
else
  PYTEST="$VENV_DIR/bin/pytest"
fi

echo "Done. Run integration tests with:"
echo "  $PYTEST tests/integration"
