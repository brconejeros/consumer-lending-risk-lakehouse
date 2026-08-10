# Databricks notebook source
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))

from src.lakehouse.silver import FkCheck, SilverTableConfig, SilverTransformJob

CATALOG = "consumer_lending_risk_lakehouse"

SilverTransformJob(
    spark,
    SilverTableConfig(
        table="bureau_balance",
        column_overrides={"STATUS": "StatusCd"},
        dedup_keys=("BureauId", "MonthsBalance"),
        fk_checks=(FkCheck("BureauId", f"{CATALOG}.bronze.bureau", "SK_ID_BUREAU"),),
    ),
).run()
