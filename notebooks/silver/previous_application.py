# Databricks notebook source
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))

from src.lakehouse.silver import SilverTableConfig, SilverTransformJob

# NFLAG_* doesn't match the FLAG_ prefix rule (different string), so these
# silently got no Flg suffix until overridden explicitly. CHANNEL_TYPE/
# PRODUCT_COMBINATION are categorical with no NAME_/CODE_ prefix.
COLUMN_OVERRIDES = {
    "CHANNEL_TYPE": "ChannelTypeCd",
    "PRODUCT_COMBINATION": "ProductCombinationCd",
    "NFLAG_LAST_APPL_IN_DAY": "LastApplInDayFlg",
    "NFLAG_MICRO_CASH": "MicroCashFlg",
    "NFLAG_INSURED_ON_APPROVAL": "InsuredOnApprovalFlg",
}

SilverTransformJob(
    spark,
    SilverTableConfig(
        table="previous_application",
        column_overrides=COLUMN_OVERRIDES,
        dedup_keys=("PrevId",),
    ),
).run()
