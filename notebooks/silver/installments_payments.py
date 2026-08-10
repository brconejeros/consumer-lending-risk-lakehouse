# Databricks notebook source
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))

from src.lakehouse.silver import FkCheck, SilverTableConfig, SilverTransformJob

CATALOG = "consumer_lending_risk_lakehouse"

# This table's grain keys - worth naming deliberately rather than leaving to
# the plain-PascalCase fallback (NUM_ isn't a rule prefix).
COLUMN_OVERRIDES = {
    "NUM_INSTALMENT_VERSION": "InstalmentVersionCd",
    "NUM_INSTALMENT_NUMBER": "InstalmentNumberCnt",
}

SilverTransformJob(
    spark,
    SilverTableConfig(
        table="installments_payments",
        column_overrides=COLUMN_OVERRIDES,
        dedup_keys=("PrevId", "InstalmentNumberCnt"),
        fk_checks=(FkCheck("PrevId", f"{CATALOG}.bronze.previous_application", "SK_ID_PREV"),),
    ),
).run()
