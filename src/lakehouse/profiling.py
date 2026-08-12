"""Small Spark helpers shared across the per-table `notebooks/
silver_profiling/<table>.py` notebooks, so each stays a thin script rather
than duplicating this logic 8 times over.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def null_rate(df: DataFrame, columns: list[str]) -> DataFrame:
    """Fraction of nulls per column, for a quick data-quality spot check."""
    return df.select(
        [F.round(F.mean(F.col(c).isNull().cast("int")), 4).alias(c) for c in columns]
    )


def fk_orphan_count(
    child_df: DataFrame, child_col: str, parent_df: DataFrame, parent_col: str
) -> int:
    """Rows in `child_df` whose `child_col` has no match in
    `parent_df.parent_col` - what `SilverTransformJob.transform()`'s
    `FkCheck` handling will drop (with a logged warning) if this is
    non-zero."""
    return child_df.join(
        parent_df, child_df[child_col] == parent_df[parent_col], "left_anti"
    ).count()
