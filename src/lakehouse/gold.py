"""Gold layer: the star schema - `fact_application` plus 4 dimensions, each
pre-aggregated to `SK_ID_CURR` (`CurrId`) grain so `fact_application` joins
to every dimension 1:1.

Unlike Bronze/Silver (one generic class, many small per-table configs -
the read/write/rename shape is identical across all 8 tables), Gold's 5
outputs each have a genuinely different join/aggregation shape, so
`GoldAggregationJob` stays a shared base (just `load()` plus the
`GoldTableConfig` naming) and each output gets its own small subclass
overriding `extract()`/`transform()` - wired up one per notebook under
`notebooks/gold/<output>.py`, the same way each Bronze/Silver notebook
instantiates its own config.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.lakehouse.base import LakehouseLayerJob

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class GoldTableConfig:
    """Everything a Gold job needs to know about one output table."""

    target: str
    catalog: str = "consumer_lending_risk_lakehouse"
    silver_schema: str = "silver"
    gold_schema: str = "gold"

    def silver_table(self, table: str) -> str:
        return f"{self.catalog}.{self.silver_schema}.{table}"

    @property
    def target_table(self) -> str:
        return f"{self.catalog}.{self.gold_schema}.{self.target}"


class GoldAggregationJob(LakehouseLayerJob):
    """Shared `load()` for every Gold output - `extract()`/`transform()`
    are each output's own aggregation logic (see the subclasses below)."""

    layer = "gold"

    def __init__(self, spark: SparkSession, config: GoldTableConfig) -> None:
        super().__init__(spark)
        self.config = config

    def __repr__(self) -> str:
        return f"{type(self).__name__}(target={self.config.target!r})"

    def load(self, df: DataFrame) -> None:
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(self.config.target_table)
        )
        row_count = self._last_write_row_count()
        logger.info(
            "[gold] wrote %s rows=%s",
            self.config.target_table,
            row_count if row_count is not None else "unknown",
        )

    def _last_write_row_count(self) -> int | None:
        """Same rationale as Bronze/Silver: read Delta's commit metrics
        instead of an extra full-table `count()` pass."""
        history_row = (
            self.spark.sql(f"DESCRIBE HISTORY {self.config.target_table} LIMIT 1")
            .select("operationMetrics")
            .first()
        )
        metrics = history_row["operationMetrics"] if history_row else None
        if not metrics or "numOutputRows" not in metrics:
            return None
        return int(metrics["numOutputRows"])


def _fill_zero(df: DataFrame, columns: list[str]) -> DataFrame:
    """Count/sum aggregates should be 0 for a `CurrId` with no related
    rows, not null - unlike avg/min/max, where null correctly means "no
    data to average"."""
    return df.na.fill(0, subset=columns)


class FactApplicationJob(GoldAggregationJob):
    """`tb_application_train` + `tb_application_test`, unioned. `Target`
    stays nullable (test rows have none); `SampleTypeCd` distinguishes
    them so both samples join to every dimension the same way."""

    def extract(self) -> DataFrame:
        train = self.spark.table(self.config.silver_table("tb_application_train")).withColumn(
            "SampleTypeCd", F.lit("TRAIN")
        )
        test = (
            self.spark.table(self.config.silver_table("tb_application_test"))
            .withColumn("Target", F.lit(None).cast("bigint"))
            .withColumn("SampleTypeCd", F.lit("TEST"))
        )
        return train.unionByName(test)


class DimBureauJob(GoldAggregationJob):
    """`tb_bureau` + `tb_bureau_balance`, rolled up to `CurrId` in two
    steps: balance history -> per-`BureauId` signal, then bureau ->
    per-`CurrId` aggregate."""

    STATUS_CODES = ("0", "1", "2", "3", "4", "5")
    DPD_CODES = ("1", "2", "3", "4", "5")

    def extract(self) -> DataFrame:
        bureau = self.spark.table(self.config.silver_table("tb_bureau"))
        bureau_balance = self.spark.table(self.config.silver_table("tb_bureau_balance"))
        return bureau.join(self._bureau_balance_rollup(bureau_balance), on="BureauId", how="left")

    @classmethod
    def _bureau_balance_rollup(cls, bureau_balance: DataFrame) -> DataFrame:
        return bureau_balance.groupBy("BureauId").agg(
            F.count("*").alias("BbMonthsCnt"),
            F.sum(F.when(F.col("StatusCd").isin(*cls.DPD_CODES), 1).otherwise(0)).alias(
                "BbDpdMonthsCnt"
            ),
            F.max(F.when(F.col("StatusCd").isin(*cls.STATUS_CODES), F.col("StatusCd"))).alias(
                "BbWorstDpdCd"
            ),
        )

    def transform(self, df: DataFrame) -> DataFrame:
        result = df.groupBy("CurrId").agg(
            F.count("*").alias("BureauCnt"),
            F.sum(F.when(F.col("CreditActiveCd") == "Active", 1).otherwise(0)).alias("BureauActiveCnt"),
            F.sum(F.when(F.col("CreditActiveCd") == "Closed", 1).otherwise(0)).alias("BureauClosedCnt"),
            F.sum("CreditSumAmt").alias("CreditSumAmtSum"),
            F.sum("CreditSumDebtAmt").alias("CreditSumDebtAmtSum"),
            F.sum("CreditSumOverdueAmt").alias("CreditSumOverdueAmtSum"),
            F.max("CreditMaxOverdueAmt").alias("CreditMaxOverdueAmtMax"),
            F.max("CreditDayOverdue").alias("CreditDayOverdueMax"),
            F.sum("CreditProlongCnt").alias("CreditProlongCntSum"),
            F.min("CreditDays").alias("CreditDaysMin"),
            F.max("CreditDays").alias("CreditDaysMax"),
            F.sum("BbDpdMonthsCnt").alias("BbDpdMonthsCntSum"),
            F.max("BbWorstDpdCd").alias("BbWorstDpdCdMax"),
        )
        count_sum_cols = [
            "BureauCnt",
            "BureauActiveCnt",
            "BureauClosedCnt",
            "CreditSumAmtSum",
            "CreditSumDebtAmtSum",
            "CreditSumOverdueAmtSum",
            "CreditProlongCntSum",
            "BbDpdMonthsCntSum",
        ]
        return _fill_zero(result, count_sum_cols)


class DimPreviousApplicationJob(GoldAggregationJob):
    """`tb_previous_application` + `tb_pos_cash_balance`, rolled up to
    `CurrId` in two steps: POS/cash history -> per-`PrevId` signal, then
    previous applications -> per-`CurrId` aggregate."""

    def extract(self) -> DataFrame:
        previous_application = self.spark.table(self.config.silver_table("tb_previous_application"))
        pos_cash_balance = self.spark.table(self.config.silver_table("tb_pos_cash_balance"))
        return previous_application.join(
            self._pos_cash_rollup(pos_cash_balance), on="PrevId", how="left"
        )

    @staticmethod
    def _pos_cash_rollup(pos_cash_balance: DataFrame) -> DataFrame:
        return pos_cash_balance.groupBy("PrevId").agg(
            F.count("*").alias("PosMonthsCnt"),
            F.max("SkDpd").alias("PosMaxSkDpd"),
            F.sum(F.when(F.col("SkDpd") > 0, 1).otherwise(0)).alias("PosDpdMonthsCnt"),
        )

    def transform(self, df: DataFrame) -> DataFrame:
        result = df.groupBy("CurrId").agg(
            F.count("*").alias("PrevApplicationCnt"),
            F.sum(F.when(F.col("ContractStatusCd") == "Approved", 1).otherwise(0)).alias("ApprovedCnt"),
            F.sum(F.when(F.col("ContractStatusCd") == "Refused", 1).otherwise(0)).alias("RefusedCnt"),
            F.sum(F.when(F.col("ContractStatusCd") == "Canceled", 1).otherwise(0)).alias("CanceledCnt"),
            F.sum("CreditAmt").alias("CreditAmtSum"),
            F.avg("CreditAmt").alias("CreditAmtAvg"),
            F.avg("AnnuityAmt").alias("AnnuityAmtAvg"),
            F.sum("ApplicationAmt").alias("ApplicationAmtSum"),
            F.min("DecisionDays").alias("DecisionDaysMin"),
            F.max("DecisionDays").alias("DecisionDaysMax"),
            F.max("PosMaxSkDpd").alias("PosMaxSkDpdMax"),
            F.sum("PosDpdMonthsCnt").alias("PosDpdMonthsCntSum"),
        )
        count_sum_cols = [
            "PrevApplicationCnt",
            "ApprovedCnt",
            "RefusedCnt",
            "CanceledCnt",
            "CreditAmtSum",
            "ApplicationAmtSum",
            "PosDpdMonthsCntSum",
        ]
        return _fill_zero(result, count_sum_cols)


class DimInstallmentsAggJob(GoldAggregationJob):
    """`tb_installments_payments`, direct to `CurrId` - no rollup needed,
    `CurrId` is already present at this table's own grain."""

    def extract(self) -> DataFrame:
        return self.spark.table(self.config.silver_table("tb_installments_payments"))

    def transform(self, df: DataFrame) -> DataFrame:
        days_late = F.col("EntryPaymentDays") - F.col("InstalmentDays")
        payment_ratio = F.when(F.col("InstalmentAmt") != 0, F.col("PaymentAmt") / F.col("InstalmentAmt"))
        result = df.groupBy("CurrId").agg(
            F.count("*").alias("InstallmentCnt"),
            F.sum(F.when(F.col("EntryPaymentDays") > F.col("InstalmentDays"), 1).otherwise(0)).alias(
                "LateInstallmentCnt"
            ),
            F.sum(F.when(F.col("PaymentAmt") < F.col("InstalmentAmt"), 1).otherwise(0)).alias(
                "ShortPaymentCnt"
            ),
            F.sum("PaymentAmt").alias("PaymentAmtSum"),
            F.sum("InstalmentAmt").alias("InstalmentAmtSum"),
            F.avg(days_late).alias("DaysLateAvg"),
            F.avg(payment_ratio).alias("PaymentRatioAvg"),
        )
        count_sum_cols = [
            "InstallmentCnt",
            "LateInstallmentCnt",
            "ShortPaymentCnt",
            "PaymentAmtSum",
            "InstalmentAmtSum",
        ]
        return _fill_zero(result, count_sum_cols)


class DimCreditCardAggJob(GoldAggregationJob):
    """`tb_credit_card_balance`, direct to `CurrId` - `CurrId` is already
    present at this table's own grain."""

    def extract(self) -> DataFrame:
        return self.spark.table(self.config.silver_table("tb_credit_card_balance"))

    def transform(self, df: DataFrame) -> DataFrame:
        utilization = F.when(
            F.col("CreditLimitActualAmt") != 0, F.col("BalanceAmt") / F.col("CreditLimitActualAmt")
        )
        result = df.groupBy("CurrId").agg(
            F.countDistinct("PrevId").alias("CreditCardCnt"),
            F.avg("BalanceAmt").alias("BalanceAmtAvg"),
            F.max("BalanceAmt").alias("BalanceAmtMax"),
            F.avg("CreditLimitActualAmt").alias("CreditLimitActualAmtAvg"),
            F.max("CreditLimitActualAmt").alias("CreditLimitActualAmtMax"),
            F.avg(utilization).alias("UtilizationRatioAvg"),
            F.sum("DrawingsAtmCurrentAmt").alias("DrawingsAtmCurrentAmtSum"),
            F.max("SkDpd").alias("SkDpdMax"),
        )
        count_sum_cols = ["CreditCardCnt", "DrawingsAtmCurrentAmtSum"]
        return _fill_zero(result, count_sum_cols)
