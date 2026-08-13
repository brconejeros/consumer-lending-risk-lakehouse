from src.lakehouse.gold import (
    DimBureauJob,
    DimCreditCardAggJob,
    DimInstallmentsAggJob,
    DimPreviousApplicationJob,
    FactApplicationJob,
    GoldTableConfig,
)


def test_config_derives_target_table():
    config = GoldTableConfig(target="dim_bureau")

    assert config.target_table == "consumer_lending_risk_lakehouse.gold.dim_bureau"


def test_config_derives_silver_table():
    config = GoldTableConfig(target="dim_bureau")

    assert config.silver_table("tb_bureau") == "consumer_lending_risk_lakehouse.silver.tb_bureau"


def test_fact_application_unions_train_and_test_with_sample_type_and_nullable_target(spark):
    spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
    spark.createDataFrame([(1, 100, 1)], ["CurrId", "IncomeTotalAmt", "Target"]).write.mode(
        "overwrite"
    ).saveAsTable("spark_catalog.silver.tb_application_train")
    spark.createDataFrame([(2, 200)], ["CurrId", "IncomeTotalAmt"]).write.mode(
        "overwrite"
    ).saveAsTable("spark_catalog.silver.tb_application_test")

    config = GoldTableConfig(target="fact_application", catalog="spark_catalog")
    job = FactApplicationJob(spark, config)

    by_curr = {row["CurrId"]: row for row in job.extract().collect()}

    assert by_curr[1]["SampleTypeCd"] == "TRAIN"
    assert by_curr[1]["Target"] == 1
    assert by_curr[2]["SampleTypeCd"] == "TEST"
    assert by_curr[2]["Target"] is None


def test_dim_bureau_aggregates_to_curr_id_with_two_step_rollup(spark):
    spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
    spark.createDataFrame(
        [(1, 10, "Active", 1000.0), (1, 11, "Closed", 500.0), (2, 12, "Active", 200.0)],
        ["CurrId", "BureauId", "CreditActiveCd", "CreditSumAmt"],
    ).write.mode("overwrite").saveAsTable("spark_catalog.silver.tb_bureau")
    spark.createDataFrame(
        [(10, "0"), (10, "2"), (11, "C")], ["BureauId", "StatusCd"]
    ).write.mode("overwrite").saveAsTable("spark_catalog.silver.tb_bureau_balance")

    config = GoldTableConfig(target="dim_bureau", catalog="spark_catalog")
    job = DimBureauJob(spark, config)

    by_curr = {row["CurrId"]: row for row in job.transform(job.extract()).collect()}

    assert by_curr[1]["BureauCnt"] == 2
    assert by_curr[1]["BureauActiveCnt"] == 1
    assert by_curr[1]["BureauClosedCnt"] == 1
    assert by_curr[1]["CreditSumAmtSum"] == 1500.0
    assert by_curr[1]["BbDpdMonthsCntSum"] == 1  # only StatusCd '2' counts as DPD
    assert by_curr[1]["BbWorstDpdCdMax"] == "2"
    # CurrId 2's only bureau credit (12) has no bureau_balance rows at all -
    # sum should come back 0, not null.
    assert by_curr[2]["BbDpdMonthsCntSum"] == 0


def test_dim_previous_application_aggregates_with_pos_cash_rollup(spark):
    spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
    spark.createDataFrame(
        [(1, 100, "Approved", 5000.0), (1, 101, "Refused", 2000.0)],
        ["CurrId", "PrevId", "ContractStatusCd", "CreditAmt"],
    ).write.mode("overwrite").saveAsTable("spark_catalog.silver.tb_previous_application")
    spark.createDataFrame([(100, 5), (100, 0)], ["PrevId", "SkDpd"]).write.mode(
        "overwrite"
    ).saveAsTable("spark_catalog.silver.tb_pos_cash_balance")

    config = GoldTableConfig(target="dim_previous_application", catalog="spark_catalog")
    job = DimPreviousApplicationJob(spark, config)

    row = job.transform(job.extract()).collect()[0]

    assert row["PrevApplicationCnt"] == 2
    assert row["ApprovedCnt"] == 1
    assert row["RefusedCnt"] == 1
    assert row["CreditAmtSum"] == 7000.0
    assert row["PosMaxSkDpdMax"] == 5
    assert row["PosDpdMonthsCntSum"] == 1


def test_dim_installments_agg_flags_late_and_short_payments(spark):
    spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
    spark.createDataFrame(
        [
            (1, -10, -12, 100.0, 100.0),  # paid on time, in full
            (1, -5, -1, 100.0, 50.0),  # paid late and short
        ],
        ["CurrId", "InstalmentDays", "EntryPaymentDays", "InstalmentAmt", "PaymentAmt"],
    ).write.mode("overwrite").saveAsTable("spark_catalog.silver.tb_installments_payments")

    config = GoldTableConfig(target="dim_installments_agg", catalog="spark_catalog")
    job = DimInstallmentsAggJob(spark, config)

    row = job.transform(job.extract()).collect()[0]

    assert row["InstallmentCnt"] == 2
    assert row["LateInstallmentCnt"] == 1
    assert row["ShortPaymentCnt"] == 1


def test_dim_credit_card_agg_computes_distinct_cards_and_utilization(spark):
    spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
    spark.createDataFrame(
        [(1, 500, 500.0, 1000.0), (1, 500, 250.0, 1000.0), (1, 501, 100.0, 200.0)],
        ["CurrId", "PrevId", "BalanceAmt", "CreditLimitActualAmt"],
    ).write.mode("overwrite").saveAsTable("spark_catalog.silver.tb_credit_card_balance")

    config = GoldTableConfig(target="dim_credit_card_agg", catalog="spark_catalog")
    job = DimCreditCardAggJob(spark, config)

    row = job.transform(job.extract()).collect()[0]

    assert row["CreditCardCnt"] == 2


def test_dim_credit_card_agg_run_lands_as_delta_table(spark):
    spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
    spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
    spark.createDataFrame(
        [(1, 500, 500.0, 1000.0)],
        ["CurrId", "PrevId", "BalanceAmt", "CreditLimitActualAmt"],
    ).write.mode("overwrite").saveAsTable("spark_catalog.silver.tb_credit_card_balance")

    config = GoldTableConfig(target="dim_credit_card_agg_run_test", catalog="spark_catalog")
    DimCreditCardAggJob(spark, config).run()

    written = spark.table(config.target_table)
    assert written.count() == 1
