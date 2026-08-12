# Databricks notebook source
# MAGIC %md
# MAGIC # Silver profiling: bureau_balance
# MAGIC
# MAGIC Exploratory, not a pipeline stage - before `notebooks/silver/
# MAGIC bureau_balance.py` conforms it. See `docs/data_dictionary.md` for the
# MAGIC full column reference.
# MAGIC
# MAGIC **What it is:** monthly balance snapshots for each `bureau` credit -
# MAGIC the payment-status history behind each bureau-reported credit line.
# MAGIC
# MAGIC **Grain:** one row per `SK_ID_BUREAU` + `MONTHS_BALANCE` (month offset
# MAGIC from the application date).
# MAGIC
# MAGIC **Business relevance:** turns `bureau`'s point-in-time snapshot into a
# MAGIC trend - months of consecutive DPD (`STATUS` buckets `1`-`5`) is a much
# MAGIC stronger signal than a single overdue-amount field, and is exactly the
# MAGIC kind of history a thin-file-at-Home-Credit applicant might still have
# MAGIC elsewhere.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))

from src.lakehouse.profiling import fk_orphan_count

CATALOG = "consumer_lending_risk_lakehouse"

# COMMAND ----------

bureau = spark.table(f"{CATALOG}.bronze.bureau")
bureau_balance = spark.table(f"{CATALOG}.bronze.bureau_balance")

print(f"rows={bureau_balance.count()}")
display(bureau_balance.groupBy("STATUS").count().orderBy("STATUS"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Key predictive columns:** `STATUS` (DPD bucket per month) is
# MAGIC essentially the whole table's value - Gold will aggregate it into
# MAGIC something like "months at DPD 60+" or "worst DPD bucket observed" per
# MAGIC applicant.
# MAGIC
# MAGIC **Data-quality watch-outs:**
# MAGIC - `STATUS` is a coded field with no prefix - overridden to `StatusCd`.
# MAGIC - This is the table CLAUDE.md's "Data quality" FK check is written for:
# MAGIC   every `SK_ID_BUREAU` here should exist in `bureau`. **It doesn't,
# MAGIC   for a real and non-trivial fraction of rows** - spot-checked below
# MAGIC   against live Bronze data, ~11% of rows (see the count below)
# MAGIC   reference a `bureau` credit that doesn't exist. This is a genuine
# MAGIC   characteristic of the source dataset, not a bug -
# MAGIC   `notebooks/silver/bureau_balance.py`'s `FkCheck` drops those rows
# MAGIC   with a logged warning rather than failing the run.

# COMMAND ----------

orphans = fk_orphan_count(bureau_balance, "SK_ID_BUREAU", bureau, "SK_ID_BUREAU")
print(f"bureau_balance rows with no matching bureau.SK_ID_BUREAU: {orphans}")
