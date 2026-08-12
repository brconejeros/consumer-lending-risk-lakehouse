"""Silver layer: conform Bronze Delta tables into the naming convention,
type-cast/dedup/null-check them, and drop rows that fail referential
integrity before promoting.

`SilverTransformJob`/`SilverTableConfig` stay table-agnostic - concrete
tables are wired up one per notebook under `notebooks/silver/<table>.py`,
the same way each Bronze notebook instantiates `BronzeTableConfig`.
`fk_checks` intentionally reads Bronze, not Silver
(CLAUDE.md's "Data quality" section: "foreign key integrity between Bronze
tables before promoting to Silver") - so every table's job can run
independently, in any order, with no dependency on a parent table having
already been written to Silver.

FK violations are **not** a hard failure: the real Home Credit dataset has
a genuine, non-trivial rate of orphaned foreign keys (e.g. ~11% of
`bureau_balance` rows reference a `bureau` credit that doesn't exist), so
raising and blocking the whole table's load isn't the right response to
that being data reality rather than a bug. Orphan rows get dropped with a
logged warning instead, so Silver stays referentially clean without an
engineer having to intervene on every run.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession

from src.lakehouse.base import LakehouseLayerJob
from src.lakehouse.naming import to_silver_column_name, to_silver_table_name

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FkCheck:
    """One referential-integrity check: rows whose `column` value doesn't
    exist as `ref_column` in `ref_table` get dropped, with a warning logged
    (not raised - see module docstring)."""

    column: str
    ref_table: str
    ref_column: str


@dataclass(frozen=True)
class SilverTableConfig:
    """Everything a Silver job needs to know about one table.

    `column_overrides`/`type_casts`/`dedup_keys` all key off the *Silver*
    (post-rename) column names, since `transform` renames before applying
    them. `dedup_keys` doubles as the null-handling policy: a table's grain
    columns must be non-null for a row to be meaningful, so `transform`
    drops null-grain-key rows before deduping on them.
    """

    table: str
    catalog: str = "consumer_lending_risk_lakehouse"
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    column_overrides: dict[str, str] = field(default_factory=dict)
    type_casts: dict[str, str] = field(default_factory=dict)
    dedup_keys: tuple[str, ...] = ()
    fk_checks: tuple[FkCheck, ...] = ()
    source_override: str | None = None
    """Bypasses the derived Bronze source table - unit tests point this at
    a local `spark_catalog` table so `run()` is testable without Unity
    Catalog."""

    @property
    def source_table(self) -> str:
        return self.source_override or f"{self.catalog}.{self.bronze_schema}.{self.table}"

    @property
    def target_table(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.{to_silver_table_name(self.table)}"


class SilverTransformJob(LakehouseLayerJob):
    """Bronze Delta -> Silver Delta: rename, cast, drop FK-orphaned rows,
    dedup."""

    layer = "silver"

    def __init__(self, spark: SparkSession, config: SilverTableConfig) -> None:
        super().__init__(spark)
        self.config = config

    def __repr__(self) -> str:
        return f"{type(self).__name__}(table={self.config.table!r})"

    def extract(self) -> DataFrame:
        logger.info("[silver] reading %s", self.config.source_table)
        return self.spark.table(self.config.source_table)

    def transform(self, df: DataFrame) -> DataFrame:
        for raw_name in df.columns:
            silver_name = to_silver_column_name(raw_name, self.config.column_overrides)
            df = df.withColumnRenamed(raw_name, silver_name)

        for column, cast_type in self.config.type_casts.items():
            df = df.withColumn(column, df[column].cast(cast_type))

        if self.config.dedup_keys:
            df = df.dropna(subset=list(self.config.dedup_keys))

        for fk in self.config.fk_checks:
            df = self._drop_fk_orphans(df, fk)

        return df.dropDuplicates(list(self.config.dedup_keys) or None)

    def _drop_fk_orphans(self, df: DataFrame, fk: FkCheck) -> DataFrame:
        ref = self.spark.table(fk.ref_table)
        orphans = df.join(ref, df[fk.column] == ref[fk.ref_column], "left_anti")
        orphan_count = orphans.count()
        if orphan_count:
            logger.warning(
                "[silver] %s.%s: dropping %d row(s) with no match in %s.%s",
                self.config.table,
                fk.column,
                orphan_count,
                fk.ref_table,
                fk.ref_column,
            )
            df = df.join(ref, df[fk.column] == ref[fk.ref_column], "left_semi")
        return df

    def load(self, df: DataFrame) -> None:
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(self.config.target_table)
        )
        row_count = self._last_write_row_count()
        logger.info(
            "[silver] wrote %s rows=%s",
            self.config.target_table,
            row_count if row_count is not None else "unknown",
        )

    def _last_write_row_count(self) -> int | None:
        """Same rationale as `BronzeIngestionJob`: read Delta's commit
        metrics instead of an extra full-table `count()` pass."""
        history_row = (
            self.spark.sql(f"DESCRIBE HISTORY {self.config.target_table} LIMIT 1")
            .select("operationMetrics")
            .first()
        )
        metrics = history_row["operationMetrics"] if history_row else None
        if not metrics or "numOutputRows" not in metrics:
            return None
        return int(metrics["numOutputRows"])
