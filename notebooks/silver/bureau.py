# Databricks notebook source
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))

from src.lakehouse.silver import SilverTableConfig, SilverTransformJob

# Coded categorical fields with no NAME_/CODE_ prefix, so the mechanical
# naming rule wouldn't add a Cd suffix on its own.
COLUMN_OVERRIDES = {
    "CREDIT_ACTIVE": "CreditActiveCd",
    "CREDIT_CURRENCY": "CreditCurrencyCd",
    "CREDIT_TYPE": "CreditTypeCd",
}

SilverTransformJob(
    spark,
    SilverTableConfig(
        table="bureau",
        column_overrides=COLUMN_OVERRIDES,
        dedup_keys=("BureauId",),
    ),
).run()
