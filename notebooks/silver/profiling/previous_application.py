# Databricks notebook source
# MAGIC %md
# MAGIC # Silver profiling: previous_application
# MAGIC
# MAGIC Exploratory, not a pipeline stage - before `notebooks/silver/
# MAGIC previous_application.py` conforms it. See `docs/data_dictionary.md` for
# MAGIC the full column reference.
# MAGIC
# MAGIC **What it is:** every prior application the client made *to Home
# MAGIC Credit itself* - approved, cancelled, refused, or unused offer - not
# MAGIC just disbursed credit.
# MAGIC
# MAGIC **Grain:** one row per `SK_ID_PREV`.
# MAGIC
# MAGIC **Business relevance:** this is Home Credit's own first-party memory of
# MAGIC the applicant - prior approval/refusal patterns and `CODE_REJECT_REASON`
# MAGIC are a direct signal the bureau tables can't provide, and it's the
# MAGIC parent table for the POS/credit-card/installment history tables
# MAGIC (profiled separately).

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..", "..")))

from pyspark.sql import functions as F

from src.lakehouse.profiling import null_rate

CATALOG = "consumer_lending_risk_lakehouse"

# COMMAND ----------

previous_application = spark.table(f"{CATALOG}.bronze.previous_application")
print(f"rows={previous_application.count()}, distinct SK_ID_PREV={previous_application.select('SK_ID_PREV').distinct().count()}")
display(previous_application.groupBy("NAME_CONTRACT_STATUS").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC **Key predictive columns:**
# MAGIC - `NAME_CONTRACT_STATUS` - was the prior application approved/refused/
# MAGIC   cancelled; refusal history is a strong repeat signal.
# MAGIC - `CODE_REJECT_REASON` - *why* it was refused, when applicable.
# MAGIC - `DAYS_DECISION` - recency of the prior application.
# MAGIC
# MAGIC **Data-quality watch-outs:**
# MAGIC - `NFLAG_LAST_APPL_IN_DAY`, `NFLAG_MICRO_CASH`, `NFLAG_INSURED_ON_APPROVAL`
# MAGIC   are boolean flags prefixed `NFLAG_`, not `FLAG_` - the mechanical rule
# MAGIC   only matches `FLAG_`, so these silently got no `Flg` suffix until
# MAGIC   overridden explicitly.
# MAGIC - `CHANNEL_TYPE` and `PRODUCT_COMBINATION` are categorical with no
# MAGIC   `NAME_`/`CODE_` prefix - overridden to `Cd`.
# MAGIC - This is the parent table for the FK checks profiled in
# MAGIC   `pos_cash_balance.py`, `credit_card_balance.py`, and
# MAGIC   `installments_payments.py`.

# COMMAND ----------

display(null_rate(previous_application, ["CODE_REJECT_REASON", "AMT_DOWN_PAYMENT", "PRODUCT_COMBINATION"]))
