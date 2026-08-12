from src.lakehouse.silver import CodeDescription, FkCheck, SilverTableConfig, SilverTransformJob


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


def test_transform_nulls_out_configured_sentinel_values(spark):
    df = spark.createDataFrame([(1, 365243), (2, -637)], ["SK_ID_CURR", "DAYS_EMPLOYED"])
    config = SilverTableConfig(
        table="application_train",
        sentinel_nulls={"EmployedDays": (365243,)},
    )
    job = SilverTransformJob(spark, config)

    result = job.transform(df).collect()

    values = {row["CurrId"]: row["EmployedDays"] for row in result}
    assert values[1] is None
    assert values[2] == -637


def test_transform_keeps_row_when_sentinel_column_has_a_real_value(spark):
    df = spark.createDataFrame([(1, -637)], ["SK_ID_CURR", "DAYS_EMPLOYED"])
    config = SilverTableConfig(table="application_train", sentinel_nulls={"EmployedDays": (365243,)})
    job = SilverTransformJob(spark, config)

    result = job.transform(df)

    assert result.count() == 1


def test_transform_adds_code_description_column(spark):
    df = spark.createDataFrame([("BUR1", "0"), ("BUR2", "C")], ["SK_ID_BUREAU", "STATUS"])
    config = SilverTableConfig(
        table="bureau_balance",
        column_overrides={"STATUS": "StatusCd"},
        code_descriptions=(
            CodeDescription(
                source_column="StatusCd",
                target_column="StatusDesc",
                mapping={"0": "No DPD", "C": "Closed"},
            ),
        ),
    )
    job = SilverTransformJob(spark, config)

    result = job.transform(df).collect()

    values = {row["BureauId"]: row["StatusDesc"] for row in result}
    assert values["BUR1"] == "No DPD"
    assert values["BUR2"] == "Closed"


def test_transform_keeps_original_coded_column_alongside_description(spark):
    df = spark.createDataFrame([("BUR1", "0")], ["SK_ID_BUREAU", "STATUS"])
    config = SilverTableConfig(
        table="bureau_balance",
        column_overrides={"STATUS": "StatusCd"},
        code_descriptions=(
            CodeDescription(source_column="StatusCd", target_column="StatusDesc", mapping={"0": "No DPD"}),
        ),
    )
    job = SilverTransformJob(spark, config)

    result = job.transform(df)

    assert "StatusCd" in result.columns
    assert result.collect()[0]["StatusCd"] == "0"


def test_transform_dedupes_on_configured_keys(spark):
    df = spark.createDataFrame(
        [(1, 100), (1, 999), (2, 200)], ["SK_ID_CURR", "AMT_CREDIT"]
    )
    config = SilverTableConfig(table="bureau", dedup_keys=("CurrId",))
    job = SilverTransformJob(spark, config)

    result = job.transform(df)

    assert result.count() == 2


def test_transform_drops_rows_with_null_dedup_keys(spark):
    df = spark.createDataFrame([(1, 100), (None, 200)], ["SK_ID_CURR", "AMT_CREDIT"])
    config = SilverTableConfig(table="bureau", dedup_keys=("CurrId",))
    job = SilverTransformJob(spark, config)

    result = job.transform(df)

    assert result.count() == 1
    assert result.collect()[0]["CurrId"] == 1


def test_transform_keeps_null_rows_when_no_dedup_keys_configured(spark):
    df = spark.createDataFrame([(1, 100), (None, 200)], ["SK_ID_CURR", "AMT_CREDIT"])
    job = SilverTransformJob(spark, SilverTableConfig(table="bureau"))

    result = job.transform(df)

    assert result.count() == 2


def test_transform_keeps_all_rows_when_no_orphaned_fk_rows(spark):
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

    result = job.transform(df)

    assert result.count() == 2


def test_transform_drops_orphaned_fk_rows_and_logs_a_warning(spark, caplog):
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

    with caplog.at_level("WARNING"):
        result = job.transform(df)

    assert result.count() == 1
    assert result.collect()[0]["BureauId"] == 1
    assert "dropping 1 row" in caplog.text


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
