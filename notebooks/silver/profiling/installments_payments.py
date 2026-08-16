# Databricks notebook source
# MAGIC %md
# MAGIC # Silver profiling: installments_payments
# MAGIC
# MAGIC Exploratory, not a pipeline stage - before `notebooks/silver/
# MAGIC installments_payments.py` conforms it. See `docs/data_dictionary.md`
# MAGIC for the full column reference.
# MAGIC
# MAGIC **What it is:** actual repayment history - one row per installment
# MAGIC payment made (or missed) against a previous Home Credit credit.
# MAGIC
# MAGIC **Grain:** one row per `SK_ID_PREV` + `NUM_INSTALMENT_NUMBER`.
# MAGIC
# MAGIC **Business relevance:** the most direct behavioral signal in the whole
# MAGIC dataset - `DAYS_ENTRY_PAYMENT` vs. `DAYS_INSTALMENT` (paid late or on
# MAGIC time?) and `AMT_PAYMENT` vs. `AMT_INSTALMENT` (paid in full or short?)
# MAGIC is literally "did this person pay what they owed, when they owed it,"
# MAGIC for every installment Home Credit has ever billed them.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..", "..")))

from pyspark.sql import functions as F

from src.lakehouse.profiling import fk_orphan_count

CATALOG = "consumer_lending_risk_lakehouse"

# COMMAND ----------

previous_application = spark.table(f"{CATALOG}.bronze.previous_application")
installments_payments = spark.table(f"{CATALOG}.bronze.installments_payments")

print(f"rows={installments_payments.count()}")
orphans = fk_orphan_count(installments_payments, "SK_ID_PREV", previous_application, "SK_ID_PREV")
print(f"installments_payments rows with no matching previous_application.SK_ID_PREV: {orphans}")

late_or_short = installments_payments.filter(
    (F.col("DAYS_ENTRY_PAYMENT") > F.col("DAYS_INSTALMENT"))
    | (F.col("AMT_PAYMENT") < F.col("AMT_INSTALMENT"))
).count()
print(f"payments late and/or short of the prescribed amount: {late_or_short}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Key predictive columns:** `DAYS_ENTRY_PAYMENT` - `DAYS_INSTALMENT`
# MAGIC (days late) and `AMT_PAYMENT` / `AMT_INSTALMENT` (fraction paid) -
# MAGIC both computed in Gold from this table's raw columns.
# MAGIC
# MAGIC **Data-quality watch-outs:** `NUM_INSTALMENT_VERSION`/
# MAGIC `NUM_INSTALMENT_NUMBER` don't match any prefix rule (`NUM_` isn't one of
# MAGIC the rule prefixes) - overridden to `InstalmentVersionCd`/
# MAGIC `InstalmentNumberCnt` since they're this table's grain keys and are
# MAGIC worth naming deliberately rather than leaving to the plain-PascalCase
# MAGIC fallback. Referential integrity: ~9% of rows (checked above) reference
# MAGIC a `SK_ID_PREV` that doesn't exist in `previous_application` - a genuine
# MAGIC source-data gap, not a bug. `notebooks/silver/installments_payments.py`'s
# MAGIC `FkCheck` drops those rows with a logged warning rather than failing
# MAGIC the run.
