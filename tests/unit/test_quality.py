import pytest
from great_expectations.expectations import ExpectColumnValuesToBeBetween, ExpectColumnValuesToBeUnique

from src.lakehouse.quality import DataQualityError, QualityCheckConfig, QualityCheckJob


def test_config_derives_source_and_results_table():
    config = QualityCheckConfig(target="fact_application", expectations=())

    assert config.source_table == "consumer_lending_risk_lakehouse.gold.fact_application"
    assert config.results_target_table == "consumer_lending_risk_lakehouse.quality.check_results"


def test_config_source_override_bypasses_gold_table():
    config = QualityCheckConfig(
        target="fact_application", expectations=(), source_override="spark_catalog.gold.fact_application"
    )

    assert config.source_table == "spark_catalog.gold.fact_application"


def test_validate_passes_when_all_expectations_hold(spark):
    df = spark.createDataFrame([(1, 50000.0), (2, 60000.0)], ["CurrId", "IncomeTotalAmt"])
    config = QualityCheckConfig(
        target="fact_application",
        expectations=(
            ExpectColumnValuesToBeUnique(column="CurrId"),
            ExpectColumnValuesToBeBetween(column="IncomeTotalAmt", min_value=0, max_value=1_000_000),
        ),
    )
    job = QualityCheckJob(spark, config)

    job.validate(df)  # should not raise


def test_validate_raises_on_duplicate_curr_id(spark):
    df = spark.createDataFrame([(1, 50000.0), (1, 60000.0)], ["CurrId", "IncomeTotalAmt"])
    config = QualityCheckConfig(
        target="fact_application",
        expectations=(ExpectColumnValuesToBeUnique(column="CurrId"),),
    )
    job = QualityCheckJob(spark, config)

    with pytest.raises(DataQualityError, match="ExpectColumnValuesToBeUnique"):
        job.validate(df)


def test_validate_tolerates_out_of_range_values_within_mostly_threshold(spark):
    # 1 outlier out of 1000 rows (99.9%) should still pass a mostly=0.99 bound.
    rows = [(i, 50000.0) for i in range(999)] + [(999, 117_000_000.0)]
    df = spark.createDataFrame(rows, ["CurrId", "IncomeTotalAmt"])
    config = QualityCheckConfig(
        target="fact_application",
        expectations=(
            ExpectColumnValuesToBeBetween(
                column="IncomeTotalAmt", min_value=0, max_value=1_000_000, mostly=0.99
            ),
        ),
    )
    job = QualityCheckJob(spark, config)

    job.validate(df)  # should not raise


def test_run_appends_one_result_row_per_expectation(spark):
    spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
    spark.sql("CREATE SCHEMA IF NOT EXISTS quality")
    spark.createDataFrame([(1, 50000.0), (2, 60000.0)], ["CurrId", "IncomeTotalAmt"]).write.mode(
        "overwrite"
    ).saveAsTable("spark_catalog.gold.fact_application_quality_test")

    config = QualityCheckConfig(
        target="fact_application_quality_test",
        catalog="spark_catalog",
        expectations=(
            ExpectColumnValuesToBeUnique(column="CurrId"),
            ExpectColumnValuesToBeBetween(column="IncomeTotalAmt", min_value=0, max_value=1_000_000),
        ),
    )
    QualityCheckJob(spark, config).run()

    written = spark.table(config.results_target_table)
    assert written.count() == 2
    assert set(written.select("Success").distinct().toPandas()["Success"]) == {True}
