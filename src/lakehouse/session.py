"""Databricks Connect session factory, used only by `tests/integration` -
runs jobs against a real serverless cluster instead of the local
pyspark+delta-spark session `tests/unit` uses.

`databricks-connect` and plain `pyspark` fight at Spark-context init time if
both are importable in the same environment, so it's never a project
dependency (see `scripts/setup_dbconnect_env.sh` / CLAUDE.md "Working
locally"). The import stays inside `get()` rather than at module level so
`src.lakehouse` keeps importing cleanly in the default env, where
`databricks-connect` is never installed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class DatabricksConnectSession:
    """Builds a `SparkSession` against a serverless Databricks cluster."""

    @staticmethod
    def get(profile: str | None = None) -> "SparkSession":
        from databricks.connect import DatabricksSession

        builder = DatabricksSession.builder.serverless(True)
        if profile:
            builder = builder.profile(profile)
        return builder.getOrCreate()
