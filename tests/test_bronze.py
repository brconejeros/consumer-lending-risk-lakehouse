from src.lakehouse.bronze import BronzeIngestionJob, BronzeTableConfig


def test_config_derives_landing_path_and_target_table():
    config = BronzeTableConfig(table="bureau")

    assert config.landing_path == (
        "abfss://landing@streditorigination01.dfs.core.windows.net/bureau/"
    )
    assert config.target_table == "consumer_lending_risk_lakehouse.bronze.bureau"


def test_config_landing_path_override_bypasses_adls_url():
    config = BronzeTableConfig(table="bureau", landing_path_override="/tmp/landing/bureau")

    assert config.landing_path == "/tmp/landing/bureau"


def test_config_reflects_pos_cash_balance_source_casing():
    config = BronzeTableConfig(table="POS_CASH_balance")

    assert config.landing_path.endswith("/POS_CASH_balance/")
    assert config.target_table == "consumer_lending_risk_lakehouse.bronze.POS_CASH_balance"


def test_extract_reads_landing_zone_parquet(spark, tmp_path):
    landing_dir = str(tmp_path / "landing")
    spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"]).write.parquet(landing_dir)

    config = BronzeTableConfig(table="sample", landing_path_override=landing_dir)
    job = BronzeIngestionJob(spark, config)

    result = job.extract()

    assert sorted(r["id"] for r in result.collect()) == [1, 2]


def test_run_lands_parquet_as_a_delta_table(spark, tmp_path):
    spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
    landing_dir = str(tmp_path / "landing")
    spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "value"]).write.parquet(
        landing_dir
    )

    config = BronzeTableConfig(
        table="sample_run",
        catalog="spark_catalog",
        landing_path_override=landing_dir,
    )
    BronzeIngestionJob(spark, config).run()

    written = spark.table(config.target_table)
    assert written.count() == 3
    assert sorted(r["value"] for r in written.collect()) == ["a", "b", "c"]


def test_rerun_overwrites_rather_than_appends(spark, tmp_path):
    spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
    landing_dir = str(tmp_path / "landing")
    config = BronzeTableConfig(
        table="sample_overwrite",
        catalog="spark_catalog",
        landing_path_override=landing_dir,
    )
    job = BronzeIngestionJob(spark, config)

    spark.createDataFrame([(1,)], ["id"]).write.mode("overwrite").parquet(landing_dir)
    job.run()
    assert spark.table(config.target_table).count() == 1

    spark.createDataFrame([(1,), (2,)], ["id"]).write.mode("overwrite").parquet(landing_dir)
    job.run()
    assert spark.table(config.target_table).count() == 2


def test_rerun_with_changed_schema_overwrites_schema(spark, tmp_path):
    spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
    landing_dir = str(tmp_path / "landing")
    config = BronzeTableConfig(
        table="sample_schema_change",
        catalog="spark_catalog",
        landing_path_override=landing_dir,
    )
    job = BronzeIngestionJob(spark, config)

    spark.createDataFrame([(1,)], ["id"]).write.mode("overwrite").parquet(landing_dir)
    job.run()
    assert spark.table(config.target_table).columns == ["id"]

    spark.createDataFrame([(1, "x")], ["id", "value"]).write.mode("overwrite").parquet(
        landing_dir
    )
    job.run()
    assert spark.table(config.target_table).columns == ["id", "value"]
