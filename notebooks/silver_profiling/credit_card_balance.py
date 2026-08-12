# Databricks notebook source
# MAGIC %md
# MAGIC # Silver profiling: credit_card_balance
# MAGIC
# MAGIC Exploratory, not a pipeline stage - before `notebooks/silver/
# MAGIC credit_card_balance.py` conforms it. See `docs/data_dictionary.md` for
# MAGIC the full column reference.
# MAGIC
# MAGIC **What it is:** monthly balance snapshots for the client's previous
# MAGIC Home Credit credit cards.
# MAGIC
# MAGIC **Grain:** one row per `SK_ID_PREV` + `MONTHS_BALANCE`.
# MAGIC
# MAGIC **Business relevance:** revolving-credit behavior (drawings vs. limit,
# MAGIC minimum-payment-only patterns via `AMT_PAYMENT_CURRENT` vs.
# MAGIC `AMT_INST_MIN_REGULARITY`) is a different risk signal than the
# MAGIC installment-loan behavior the other tables capture.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))

from src.lakehouse.profiling import fk_orphan_count

CATALOG = "consumer_lending_risk_lakehouse"

# COMMAND ----------

previous_application = spark.table(f"{CATALOG}.bronze.previous_application")
credit_card_balance = spark.table(f"{CATALOG}.bronze.credit_card_balance")

print(f"rows={credit_card_balance.count()}")
orphans = fk_orphan_count(credit_card_balance, "SK_ID_PREV", previous_application, "SK_ID_PREV")
print(f"credit_card_balance rows with no matching previous_application.SK_ID_PREV: {orphans}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Key predictive columns:** `AMT_BALANCE` relative to
# MAGIC `AMT_CREDIT_LIMIT_ACTUAL` (utilization), and `AMT_DRAWINGS_ATM_CURRENT`
# MAGIC (cash-advance usage is a common risk flag).
# MAGIC
# MAGIC **Data-quality watch-outs:** `AMT_RECIVABLE` is spelled that way in the
# MAGIC source CSV (missing the second "E") - kept as-is for traceability; the
# MAGIC mechanical rule still produces a correct `RecivableAmt`. Naming needs
# MAGIC no other overrides. Referential integrity is the real watch-out here:
# MAGIC ~28% of rows (checked above) reference a `SK_ID_PREV` that doesn't
# MAGIC exist in `previous_application` - the highest orphan rate of any table
# MAGIC profiled, a genuine source-data gap rather than a bug.
# MAGIC `notebooks/silver/credit_card_balance.py`'s `FkCheck` drops those rows
# MAGIC with a logged warning rather than failing the run.
