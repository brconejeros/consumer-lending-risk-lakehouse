# Databricks notebook source
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))

from src.lakehouse.silver import SilverTableConfig, SilverTransformJob

# Same overrides as application_train - identical schema minus TARGET.
COLUMN_OVERRIDES = {
    "EXT_SOURCE_1": "ExtSource1Score",
    "EXT_SOURCE_2": "ExtSource2Score",
    "EXT_SOURCE_3": "ExtSource3Score",
    "AMT_REQ_CREDIT_BUREAU_HOUR": "ReqCreditBureauHourCnt",
    "AMT_REQ_CREDIT_BUREAU_DAY": "ReqCreditBureauDayCnt",
    "AMT_REQ_CREDIT_BUREAU_WEEK": "ReqCreditBureauWeekCnt",
    "AMT_REQ_CREDIT_BUREAU_MON": "ReqCreditBureauMonCnt",
    "AMT_REQ_CREDIT_BUREAU_QRT": "ReqCreditBureauQrtCnt",
    "AMT_REQ_CREDIT_BUREAU_YEAR": "ReqCreditBureauYearCnt",
}

# Same DAYS_EMPLOYED sentinel as application_train - see that notebook.
SENTINEL_NULLS = {"EmployedDays": (365243,)}

SilverTransformJob(
    spark,
    SilverTableConfig(
        table="application_test",
        column_overrides=COLUMN_OVERRIDES,
        sentinel_nulls=SENTINEL_NULLS,
        dedup_keys=("CurrId",),
    ),
).run()
