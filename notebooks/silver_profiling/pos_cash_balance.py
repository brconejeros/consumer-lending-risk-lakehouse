# Databricks notebook source
# MAGIC %md
# MAGIC # Silver profiling: POS_CASH_balance
# MAGIC
# MAGIC Exploratory, not a pipeline stage - before `notebooks/silver/
# MAGIC pos_cash_balance.py` conforms it. See `docs/data_dictionary.md` for the
# MAGIC full column reference.
# MAGIC
# MAGIC **What it is:** monthly balance snapshots for the client's previous
# MAGIC point-of-sale and cash loans with Home Credit.
# MAGIC
# MAGIC **Grain:** one row per `SK_ID_PREV` + `MONTHS_BALANCE`.
# MAGIC
# MAGIC **Business relevance:** `SK_DPD`/`SK_DPD_DEF` trended over months gives
# MAGIC the same "how did they actually behave over time" signal
# MAGIC `bureau_balance` gives for outside credit, but for Home Credit's own
# MAGIC POS/cash products specifically.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))

from src.lakehouse.profiling import fk_orphan_count

CATALOG = "consumer_lending_risk_lakehouse"

# COMMAND ----------

previous_application = spark.table(f"{CATALOG}.bronze.previous_application")
pos_cash_balance = spark.table(f"{CATALOG}.bronze.POS_CASH_balance")

print(f"rows={pos_cash_balance.count()}")
orphans = fk_orphan_count(pos_cash_balance, "SK_ID_PREV", previous_application, "SK_ID_PREV")
print(f"POS_CASH_balance rows with no matching previous_application.SK_ID_PREV: {orphans}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Key predictive columns:** `SK_DPD` / `SK_DPD_DEF` (days past due, with
# MAGIC and without a small-balance tolerance) and `NAME_CONTRACT_STATUS`
# MAGIC trended across months.
# MAGIC
# MAGIC **Data-quality watch-outs:** none needed beyond the automatic naming
# MAGIC rules - every column here maps cleanly (`NAME_CONTRACT_STATUS`→`Cd`,
# MAGIC `CNT_INSTALMENT*`→`Cnt`, `SK_ID_PREV`/`SK_ID_CURR`→`Id`).
