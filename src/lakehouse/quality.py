"""Quality layer: Great Expectations checks against a Gold table.

Complements the checks already enforced upstream (Silver's grain-column
null checks and FK-orphan filtering - see CLAUDE.md's "Data quality") with
the two checks that only make sense once Gold's aggregation/union has
happened: `CurrId` uniqueness in `fact_application` (the fact table's
declared grain) and plausible ranges for age/income fields. Unlike
Silver's FK orphans - a documented, tolerated characteristic of the real
source data - a duplicate `CurrId` is a genuine pipeline bug, so
`validate()` raises rather than filtering rows out.

Uses Great Expectations' Core/Fluent API (>=1.0) against the Gold
DataFrame in memory via an ephemeral `DataContext` - no persistent GX
project config to check in, since the expectation suite is built in code
here and discarded after each run.

`load()` appends one row per expectation to a `quality.check_results`
audit table - reached only when `validate()` doesn't raise, since the
base `LakehouseLayerJob.run()` calls `validate` before `load` (see
`src/lakehouse/base.py`). A failing run is still visible: `validate()`
logs every expectation's outcome before raising, so the failure detail
lands in the notebook/Job run's own logs even though nothing is written
to the audit table for that run.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

import great_expectations as gx
from great_expectations.core.expectation_validation_result import ExpectationSuiteValidationResult
from great_expectations.expectations.expectation import Expectation
from pyspark.sql import DataFrame, Row, SparkSession

from src.lakehouse.base import LakehouseLayerJob

logger = logging.getLogger(__name__)


class DataQualityError(Exception):
    """Raised when one or more Great Expectations checks fail against a Gold table."""


@dataclass(frozen=True)
class QualityCheckConfig:
    """Everything a quality-check job needs to know about one Gold table."""

    target: str
    expectations: tuple[Expectation, ...]
    catalog: str = "consumer_lending_risk_lakehouse"
    gold_schema: str = "gold"
    quality_schema: str = "quality"
    results_table: str = "check_results"
    source_override: str | None = None
    """Bypasses the derived Gold source table - unit tests point this at a
    local `spark_catalog` table so `run()` is testable without Unity
    Catalog, same pattern as `SilverTableConfig.source_override`."""

    @property
    def source_table(self) -> str:
        return self.source_override or f"{self.catalog}.{self.gold_schema}.{self.target}"

    @property
    def results_target_table(self) -> str:
        return f"{self.catalog}.{self.quality_schema}.{self.results_table}"


class QualityCheckJob(LakehouseLayerJob):
    """Runs a `QualityCheckConfig`'s expectations against its Gold table."""

    layer = "quality"

    def __init__(self, spark: SparkSession, config: QualityCheckConfig) -> None:
        super().__init__(spark)
        self.config = config
        self._result: ExpectationSuiteValidationResult | None = None

    def __repr__(self) -> str:
        return f"{type(self).__name__}(target={self.config.target!r})"

    def extract(self) -> DataFrame:
        logger.info("[quality] reading %s", self.config.source_table)
        return self.spark.table(self.config.source_table)

    def validate(self, df: DataFrame) -> None:
        self._result = self._run_suite(df)

        failures = []
        for expectation_result in self._result.results:
            config = expectation_result.expectation_config
            log = logger.info if expectation_result.success else logger.error
            log(
                "[quality] %s: %s(%s) success=%s",
                self.config.target,
                config.type,
                config.kwargs,
                expectation_result.success,
            )
            if not expectation_result.success:
                failures.append(f"{config.type}({config.kwargs})")

        if failures:
            raise DataQualityError(
                f"{self.config.target}: {len(failures)} check(s) failed: {'; '.join(failures)}"
            )

    def _run_suite(self, df: DataFrame) -> ExpectationSuiteValidationResult:
        context = gx.get_context(mode="ephemeral")
        data_source = context.data_sources.add_spark(name=f"{self.config.target}_spark_ds")
        asset = data_source.add_dataframe_asset(name=f"{self.config.target}_asset")
        batch_definition = asset.add_batch_definition_whole_dataframe(f"{self.config.target}_batch")
        batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

        suite = context.suites.add(gx.ExpectationSuite(name=f"{self.config.target}_suite"))
        for expectation in self.config.expectations:
            suite.add_expectation(expectation)

        return batch.validate(suite)

    def load(self, df: DataFrame) -> None:
        results_df = self._results_to_dataframe()
        (
            results_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(self.config.results_target_table)
        )
        logger.info(
            "[quality] appended %d check result(s) to %s",
            len(self._result.results),
            self.config.results_target_table,
        )

    def _results_to_dataframe(self) -> DataFrame:
        run_at = datetime.now(timezone.utc)
        rows = [
            Row(
                Target=self.config.target,
                CheckType=expectation_result.expectation_config.type,
                Column=expectation_result.expectation_config.kwargs.get("column"),
                Success=bool(expectation_result.success),
                RunAt=run_at,
            )
            for expectation_result in self._result.results
        ]
        return self.spark.createDataFrame(rows)
