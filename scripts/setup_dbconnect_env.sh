#!/usr/bin/env bash
# Creates/refreshes .venv-dbconnect, a venv separate from the project's main
# uv-managed one. databricks-connect and plain pyspark (pinned ==3.5.3 for
# tests/unit's local Delta session) fight at Spark-context init time if both
# are importable in the same env, so this stays fully isolated - nothing
# here touches pyproject.toml/uv.lock. Run `tests/integration` (Databricks
# Connect, real serverless cluster) via this venv's pytest; run `tests/unit`
# (local pyspark+delta-spark) via the normal `uv run pytest`.
#
# databricks-connect is pinned to 18.3 on Python 3.12, not latest - a newer
# release (verified broken: 19.0.0 on Python 3.13) requests a serverless
# "client image version" this project's Azure workspace doesn't support,
# failing with INVALID_PARAMETER_VALUE.INVALID_CLIENT_IMAGE_VERSION.
# Databricks' own compatibility table lists 18.0-18.3 as the current top
# serverless-compatible bracket, requiring Python 3.12 - re-pin both
# together rather than upgrading just the package if this breaks again:
# https://docs.databricks.com/aws/en/dev-tools/databricks-connect/requirements
set -euo pipefail

VENV_DIR=".venv-dbconnect"

uv venv "$VENV_DIR" --python 3.12
uv pip install --python "$VENV_DIR" "databricks-connect==18.3" pytest python-dotenv

if [ -f "$VENV_DIR/Scripts/pytest.exe" ]; then
  PYTEST="$VENV_DIR/Scripts/pytest.exe"  # Windows venv layout
else
  PYTEST="$VENV_DIR/bin/pytest"
fi

echo "Done. Run integration tests with:"
echo "  $PYTEST tests/integration"
