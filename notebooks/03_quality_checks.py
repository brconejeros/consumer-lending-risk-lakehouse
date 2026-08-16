# Databricks notebook source
# MAGIC %pip install great-expectations==1.20.0

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..")))

from great_expectations.expectations import ExpectColumnValuesToBeBetween, ExpectColumnValuesToBeUnique

from src.lakehouse.quality import QualityCheckConfig, QualityCheckJob

# CurrId is fact_application's declared grain (CLAUDE.md: "Gold ... grain
# is 1 row per SK_ID_CURR") - a duplicate here is a pipeline bug, not a
# real-data quirk, so this stays a strict (non-`mostly`) check.
# BirthDays is DAYS_BIRTH, always negative (days before the application
# date) - -36500/-6570 is the 100yo/18yo bound; the field has no known
# sentinels or outliers, so it also stays strict.
# IncomeTotalAmt tolerates a small fraction of extreme values via `mostly`
# - the real Home Credit dataset has a handful of genuine high-income
# outliers (e.g. one row at 117,000,000), not bugs, so a hard 100% bound
# would fail every run on real data.
FACT_APPLICATION_EXPECTATIONS = (
    ExpectColumnValuesToBeUnique(column="CurrId"),
    ExpectColumnValuesToBeBetween(column="BirthDays", min_value=-36500, max_value=-6570),
    ExpectColumnValuesToBeBetween(
        column="IncomeTotalAmt", min_value=0, max_value=10_000_000, mostly=0.999
    ),
)

QualityCheckJob(
    spark,
    QualityCheckConfig(target="fact_application", expectations=FACT_APPLICATION_EXPECTATIONS),
).run()
