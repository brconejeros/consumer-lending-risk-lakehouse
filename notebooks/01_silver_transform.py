# Databricks notebook source
# MAGIC %md
# MAGIC # Silver transform: all 8 tables
# MAGIC
# MAGIC Runs every table through `SilverTransformJob` in one pass, per
# MAGIC CLAUDE.md's Silver-layer convention (unlike Bronze's 8 parallel
# MAGIC per-table notebooks/tasks). `column_overrides`/`dedup_keys`/`fk_checks`
# MAGIC below come out of `notebooks/silver_profiling.py`'s findings - see that
# MAGIC notebook for the "why" behind each one.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..")))

from src.lakehouse.silver import FkCheck, SilverTableConfig, SilverTransformJob

CATALOG = "consumer_lending_risk_lakehouse"
BRONZE = f"{CATALOG}.bronze"

# COMMAND ----------

# `EXT_SOURCE_*` -> Score (strongest predictive signal in the dataset, see
# profiling notebook); `AMT_REQ_CREDIT_BUREAU_*` -> Cnt (these are enquiry
# counts despite the AMT_ prefix - a real source-data naming quirk, not a
# monetary amount).
APPLICATION_OVERRIDES = {
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

# Coded categorical fields with no NAME_/CODE_ prefix, so the mechanical
# rule wouldn't add a Cd suffix on its own.
BUREAU_OVERRIDES = {
    "CREDIT_ACTIVE": "CreditActiveCd",
    "CREDIT_CURRENCY": "CreditCurrencyCd",
    "CREDIT_TYPE": "CreditTypeCd",
}

BUREAU_BALANCE_OVERRIDES = {
    "STATUS": "StatusCd",
}

# NFLAG_* doesn't match the FLAG_ prefix rule (different string), so these
# silently got no Flg suffix until overridden explicitly.
PREVIOUS_APPLICATION_OVERRIDES = {
    "CHANNEL_TYPE": "ChannelTypeCd",
    "PRODUCT_COMBINATION": "ProductCombinationCd",
    "NFLAG_LAST_APPL_IN_DAY": "LastApplInDayFlg",
    "NFLAG_MICRO_CASH": "MicroCashFlg",
    "NFLAG_INSURED_ON_APPROVAL": "InsuredOnApprovalFlg",
}

# This table's grain keys - worth naming deliberately rather than leaving to
# the plain-PascalCase fallback (NUM_ isn't a rule prefix).
INSTALLMENTS_PAYMENTS_OVERRIDES = {
    "NUM_INSTALMENT_VERSION": "InstalmentVersionCd",
    "NUM_INSTALMENT_NUMBER": "InstalmentNumberCnt",
}

# COMMAND ----------

TABLE_CONFIGS = [
    SilverTableConfig(
        table="application_train",
        column_overrides=APPLICATION_OVERRIDES,
        dedup_keys=("CurrId",),
    ),
    SilverTableConfig(
        table="application_test",
        column_overrides=APPLICATION_OVERRIDES,
        dedup_keys=("CurrId",),
    ),
    SilverTableConfig(
        table="bureau",
        column_overrides=BUREAU_OVERRIDES,
        dedup_keys=("BureauId",),
    ),
    SilverTableConfig(
        table="bureau_balance",
        column_overrides=BUREAU_BALANCE_OVERRIDES,
        dedup_keys=("BureauId", "MonthsBalance"),
        fk_checks=(FkCheck("BureauId", f"{BRONZE}.bureau", "SK_ID_BUREAU"),),
    ),
    SilverTableConfig(
        table="previous_application",
        column_overrides=PREVIOUS_APPLICATION_OVERRIDES,
        dedup_keys=("PrevId",),
    ),
    SilverTableConfig(
        table="POS_CASH_balance",
        dedup_keys=("PrevId", "MonthsBalance"),
        fk_checks=(FkCheck("PrevId", f"{BRONZE}.previous_application", "SK_ID_PREV"),),
    ),
    SilverTableConfig(
        table="credit_card_balance",
        dedup_keys=("PrevId", "MonthsBalance"),
        fk_checks=(FkCheck("PrevId", f"{BRONZE}.previous_application", "SK_ID_PREV"),),
    ),
    SilverTableConfig(
        table="installments_payments",
        column_overrides=INSTALLMENTS_PAYMENTS_OVERRIDES,
        dedup_keys=("PrevId", "InstalmentNumberCnt"),
        fk_checks=(FkCheck("PrevId", f"{BRONZE}.previous_application", "SK_ID_PREV"),),
    ),
]

# COMMAND ----------

for config in TABLE_CONFIGS:
    SilverTransformJob(spark, config).run()
