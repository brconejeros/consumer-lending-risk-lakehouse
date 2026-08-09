import pytest

from src.lakehouse.silver import FkCheck, SilverTableConfig, SilverTransformJob


def test_config_derives_source_and_target_table():
    config = SilverTableConfig(table="bureau")

    assert config.source_table == "consumer_lending_risk_lakehouse.bronze.bureau"
    assert config.target_table == "consumer_lending_risk_lakehouse.silver.tb_bureau"


def test_config_reflects_pos_cash_balance_source_casing():
    config = SilverTableConfig(table="POS_CASH_balance")

    assert config.source_table == "consumer_lending_risk_lakehouse.bronze.POS_CASH_balance"
    assert config.target_table == "consumer_lending_risk_lakehouse.silver.tb_pos_cash_balance"


def test_config_source_override_bypasses_bronze_table():
    config = SilverTableConfig(table="bureau", source_override="spark_catalog.bronze.bureau")

    assert config.source_table == "spark_catalog.bronze.bureau"


def test_transform_renames_columns_via_naming_convention(spark):
    df = spark.createDataFrame([(1, 100), (2, 200)], ["SK_ID_CURR", "AMT_CREDIT"])
    job = SilverTransformJob(spark, SilverTableConfig(table="bureau"))

    result = job.transform(df)

    assert sorted(result.columns) == ["CreditAmt", "CurrId"]


def test_transform_applies_column_overrides(spark):
    df = spark.createDataFrame([(1,)], ["SK_ID_CURR"])
    config = SilverTableConfig(table="bureau", column_overrides={"SK_ID_CURR": "ApplicationId"})
    job = SilverTransformJob(spark, config)

    result = job.transform(df)

    assert result.columns == ["ApplicationId"]


def test_transform_applies_type_casts(spark):
    df = spark.createDataFrame([("1",)], ["SK_ID_CURR"])
    config = SilverTableConfig(table="bureau", type_casts={"CurrId": "int"})
    job = SilverTransformJob(spark, config)

    result = job.transform(df)

    assert dict(result.dtypes)["CurrId"] == "int"


def test_transform_dedupes_on_configured_keys(spark):
    df = spark.createDataFrame(
        [(1, 100), (1, 999), (2, 200)], ["SK_ID_CURR", "AMT_CREDIT"]
    )
    config = SilverTableConfig(table="bureau", dedup_keys=("CurrId",))
    job = SilverTransformJob(spark, config)

    result = job.transform(df)

    assert result.count() == 2


def test_validate_passes_when_no_orphaned_fk_rows(spark):
    spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
    spark.createDataFrame([(1,), (2,)], ["BureauId"]).write.mode("overwrite").saveAsTable(
        "spark_catalog.bronze.tb_bureau_ref"
    )
    df = spark.createDataFrame([(1,), (2,)], ["BureauId"])
    config = SilverTableConfig(
        table="bureau_balance",
        fk_checks=(FkCheck("BureauId", "spark_catalog.bronze.tb_bureau_ref", "BureauId"),),
    )
    job = SilverTransformJob(spark, config)

    job.validate(df)


def test_validate_raises_on_orphaned_fk_rows(spark):
    spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
    spark.createDataFrame([(1,)], ["BureauId"]).write.mode("overwrite").saveAsTable(
        "spark_catalog.bronze.tb_bureau_ref_2"
    )
    df = spark.createDataFrame([(1,), (99,)], ["BureauId"])
    config = SilverTableConfig(
        table="bureau_balance",
        fk_checks=(FkCheck("BureauId", "spark_catalog.bronze.tb_bureau_ref_2", "BureauId"),),
    )
    job = SilverTransformJob(spark, config)

    with pytest.raises(ValueError):
        job.validate(df)


def test_run_lands_source_table_as_silver_delta_table(spark, tmp_path):
    spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
    spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
    spark.createDataFrame([(1, 100), (2, 200)], ["SK_ID_CURR", "AMT_CREDIT"]).write.mode(
        "overwrite"
    ).saveAsTable("spark_catalog.bronze.silver_run_source")

    config = SilverTableConfig(
        table="silver_run_source",
        catalog="spark_catalog",
        source_override="spark_catalog.bronze.silver_run_source",
    )
    SilverTransformJob(spark, config).run()

    written = spark.table(config.target_table)
    assert sorted(written.columns) == ["CreditAmt", "CurrId"]
    assert written.count() == 2
