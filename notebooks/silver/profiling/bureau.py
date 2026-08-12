# Databricks notebook source
# MAGIC %md
# MAGIC # Silver profiling: bureau
# MAGIC
# MAGIC Exploratory, not a pipeline stage - before `notebooks/silver/bureau.py`
# MAGIC conforms it. See `docs/data_dictionary.md` for the full column
# MAGIC reference.
# MAGIC
# MAGIC **What it is:** every credit the applicant had reported to the Credit
# MAGIC Bureau by other institutions, as of the application date - not just
# MAGIC Home Credit's own history.
# MAGIC
# MAGIC **Grain:** one row per previous Credit Bureau credit (`SK_ID_BUREAU`,
# MAGIC unique per application); an applicant can have 0, 1, or many rows.
# MAGIC
# MAGIC **Business relevance:** this is the "outside" credit history a
# MAGIC thin-file applicant may or may not have - active vs. closed credit mix,
# MAGIC overdue amounts, and how many institutions they've borrowed from are
# MAGIC all classic bureau-style risk signals, independent of anything Home
# MAGIC Credit has observed directly.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..", "..")))

from src.lakehouse.profiling import null_rate

CATALOG = "consumer_lending_risk_lakehouse"

# COMMAND ----------

bureau = spark.table(f"{CATALOG}.bronze.bureau")
print(f"rows={bureau.count()}, distinct SK_ID_BUREAU={bureau.select('SK_ID_BUREAU').distinct().count()}")
display(bureau.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC **Key predictive columns:**
# MAGIC - `CREDIT_ACTIVE` - active vs. closed vs. sold credit mix.
# MAGIC - `AMT_CREDIT_SUM_OVERDUE` / `CREDIT_DAY_OVERDUE` - direct delinquency
# MAGIC   signal from another lender.
# MAGIC - `CNT_CREDIT_PROLONG` - how often a credit was extended/renegotiated.
# MAGIC
# MAGIC **Data-quality watch-outs:**
# MAGIC - `CREDIT_ACTIVE`, `CREDIT_CURRENCY`, `CREDIT_TYPE` are coded categorical
# MAGIC   fields with no `NAME_`/`CODE_` prefix, so the mechanical naming rule
# MAGIC   wouldn't add a `Cd` suffix automatically - overridden explicitly in
# MAGIC   `notebooks/silver/bureau.py`.
# MAGIC - The real source CSV/Bronze header is `SK_ID_BUREAU` - an earlier
# MAGIC   version of `docs/data_dictionary.md` mislabeled it `SK_BUREAU_ID`,
# MAGIC   now fixed there too.

# COMMAND ----------

display(null_rate(bureau, ["CREDIT_ACTIVE", "AMT_CREDIT_SUM_OVERDUE", "DAYS_CREDIT_ENDDATE"]))
