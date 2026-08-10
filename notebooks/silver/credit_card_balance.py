# Databricks notebook source
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))

from src.lakehouse.silver import FkCheck, SilverTableConfig, SilverTransformJob

CATALOG = "consumer_lending_risk_lakehouse"

SilverTransformJob(
    spark,
    SilverTableConfig(
        table="credit_card_balance",
        dedup_keys=("PrevId", "MonthsBalance"),
        fk_checks=(FkCheck("PrevId", f"{CATALOG}.bronze.previous_application", "SK_ID_PREV"),),
    ),
).run()
