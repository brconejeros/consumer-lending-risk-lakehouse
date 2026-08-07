"""Bronze layer: land ADF's Parquet output into Unity Catalog as Delta tables.

`BronzeIngestionJob` is the single class every `notebooks/bronze/<table>.py`
notebook instantiates. The 8 notebooks stay separate files on purpose (each is
its own task in the `bronze_ingestion` Databricks Job, running in parallel) -
this class only removes the near-identical read/write code that used to be
copy-pasted across all 8, without collapsing them into one shared loop.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession

from src.lakehouse.base import LakehouseLayerJob

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BronzeTableConfig:
    """Everything a Bronze job needs to know about one source table."""

    table: str
    catalog: str = "consumer_lending_risk_lakehouse"
    landing_storage_account: str = "streditorigination01"
    landing_container: str = "landing"
    landing_path_override: str | None = None
    """Bypasses the derived ADLS path - unit tests point this at a local dir."""

    @property
    def landing_path(self) -> str:
        if self.landing_path_override:
            return self.landing_path_override
        return (
            f"abfss://{self.landing_container}@{self.landing_storage_account}"
            f".dfs.core.windows.net/{self.table}/"
        )

    @property
    def target_table(self) -> str:
        return f"{self.catalog}.bronze.{self.table}"


class BronzeIngestionJob(LakehouseLayerJob):
    """Parquet (ADF landing zone) -> Delta (`bronze` schema), full overwrite.

    No incremental/CDC state to track yet (see CLAUDE.md "Future
    enhancements"), so `transform`/`validate` are left as the base class's
    no-ops and every run replaces the target table wholesale.
    """

    layer = "bronze"

    def __init__(self, spark: SparkSession, config: BronzeTableConfig) -> None:
        super().__init__(spark)
        self.config = config

    def __repr__(self) -> str:
        return f"{type(self).__name__}(table={self.config.table!r})"

    def extract(self) -> DataFrame:
        logger.info("[bronze] reading %s", self.config.landing_path)
        return self.spark.read.parquet(self.config.landing_path)

    def load(self, df: DataFrame) -> None:
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(self.config.target_table)
        )
        row_count = self._last_write_row_count()
        logger.info(
            "[bronze] wrote %s rows=%s",
            self.config.target_table,
            row_count if row_count is not None else "unknown",
        )

    def _last_write_row_count(self) -> int | None:
        """Row count of the write just performed, read from Delta's commit
        metrics (`DESCRIBE HISTORY`) rather than an extra `df.count()` pass -
        cheap metadata lookup instead of a second full scan of a table that
        can run into tens of millions of rows (e.g. `bureau_balance`).
        """
        history_row = (
            self.spark.sql(f"DESCRIBE HISTORY {self.config.target_table} LIMIT 1")
            .select("operationMetrics")
            .first()
        )
        metrics = history_row["operationMetrics"] if history_row else None
        if not metrics or "numOutputRows" not in metrics:
            return None
        return int(metrics["numOutputRows"])
