# Databricks notebook source
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))

from src.lakehouse.silver import CodeDescription, FkCheck, SilverTableConfig, SilverTransformJob

CATALOG = "consumer_lending_risk_lakehouse"

# STATUS is a documented DPD-bucket code (see docs/data_dictionary.md) -
# StatusDesc decodes it into text alongside the original StatusCd column.
CODE_DESCRIPTIONS = (
    CodeDescription(
        source_column="StatusCd",
        target_column="StatusDesc",
        mapping={
            "0": "No DPD",
            "1": "DPD 1-30",
            "2": "DPD 31-60",
            "3": "DPD 61-90",
            "4": "DPD 91-120",
            "5": "DPD 120+ or sold/written off",
            "C": "Closed",
            "X": "Status unknown",
        },
    ),
)

SilverTransformJob(
    spark,
    SilverTableConfig(
        table="bureau_balance",
        column_overrides={"STATUS": "StatusCd"},
        code_descriptions=CODE_DESCRIPTIONS,
        dedup_keys=("BureauId", "MonthsBalance"),
        fk_checks=(FkCheck("BureauId", f"{CATALOG}.bronze.bureau", "SK_ID_BUREAU"),),
    ),
).run()
