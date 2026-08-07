"""Shared template-method base for medallion-layer jobs (Bronze/Silver/Gold).

Every layer's notebooks call the same `run()` entry point; only `extract`/
`load` are required per layer, `transform`/`validate` default to no-ops so a
layer that doesn't need them (Bronze, today) can skip them entirely.
"""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


class LakehouseLayerJob(ABC):
    """One medallion-layer job: extract -> transform -> validate -> load."""

    layer: str = "unknown"

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    @abstractmethod
    def extract(self) -> DataFrame:
        """Read the source data for this job."""

    def transform(self, df: DataFrame) -> DataFrame:
        """Apply layer-specific transformations. Identity by default."""
        return df

    def validate(self, df: DataFrame) -> None:
        """Raise if `df` fails this layer's data quality checks. No-op by default."""
        return None

    @abstractmethod
    def load(self, df: DataFrame) -> None:
        """Persist `df` as this job's output."""

    def run(self) -> DataFrame:
        started = time.perf_counter()
        logger.info("[%s] %s: starting", self.layer, self)
        df = self.extract()
        df = self.transform(df)
        self.validate(df)
        self.load(df)
        logger.info(
            "[%s] %s: finished in %.1fs",
            self.layer,
            self,
            time.perf_counter() - started,
        )
        return df
