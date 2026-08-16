# Databricks notebook source
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))

from src.lakehouse.silver import SilverTableConfig, SilverTransformJob

# EXT_SOURCE_* -> Score (strongest predictive signal in the dataset, see
# notebooks/silver/profiling/application_train.py); AMT_REQ_CREDIT_BUREAU_*
# -> Cnt (these are enquiry counts despite the AMT_ prefix - a real
# source-data naming quirk).
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

# DAYS_EMPLOYED uses 365243 (~1000 years) as a "not currently employed"
# sentinel instead of a real day count - affects ~18% of rows. Nulled out
# rather than dropped, since the row itself is still meaningful.
SENTINEL_NULLS = {"EmployedDays": (365243,)}

SilverTransformJob(
    spark,
    SilverTableConfig(
        table="application_train",
        column_overrides=COLUMN_OVERRIDES,
        sentinel_nulls=SENTINEL_NULLS,
        dedup_keys=("CurrId",),
    ),
).run()
