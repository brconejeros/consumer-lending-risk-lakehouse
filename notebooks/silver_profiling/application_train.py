# Databricks notebook source
# MAGIC %md
# MAGIC # Silver profiling: application_train
# MAGIC
# MAGIC Exploratory, not a pipeline stage - answers "what do we actually know
# MAGIC about this table, and why does it matter for predicting default?"
# MAGIC before `notebooks/silver/application_train.py` conforms it. See
# MAGIC `docs/data_dictionary.md` for the full column-by-column reference -
# MAGIC this notebook is the "why it matters" companion, not a replacement.
# MAGIC
# MAGIC **What it is:** the main table - one row per loan application, with
# MAGIC the applicant's demographics, income, employment, housing, and a set
# MAGIC of normalized building/region statistics. Carries `TARGET` (1 =
# MAGIC payment difficulties) - `application_test` (profiled separately)
# MAGIC shares this schema minus `TARGET`.
# MAGIC
# MAGIC **Grain:** one row per `SK_ID_CURR`, the primary key every other table
# MAGIC joins back to.
# MAGIC
# MAGIC **Business relevance:** this *is* the applicant snapshot at the moment
# MAGIC of application - the "no robust bank history" thin-file signal the
# MAGIC whole project is about comes from here (short `DAYS_EMPLOYED`, low
# MAGIC `EXT_SOURCE_*` coverage) combined with the bureau/previous-application
# MAGIC history tables.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))

from pyspark.sql import functions as F

from src.lakehouse.profiling import null_rate

CATALOG = "consumer_lending_risk_lakehouse"

# COMMAND ----------

application_train = spark.table(f"{CATALOG}.bronze.application_train")
print(f"rows={application_train.count()}, distinct SK_ID_CURR={application_train.select('SK_ID_CURR').distinct().count()}")
display(application_train.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC **Key predictive columns:**
# MAGIC - `EXT_SOURCE_1`, `EXT_SOURCE_2`, `EXT_SOURCE_3` - normalized scores from
# MAGIC   external data sources; consistently the strongest individual
# MAGIC   predictors of `TARGET` in this dataset, which is exactly why they get
# MAGIC   their own `Score` suffix in Silver rather than falling under the
# MAGIC   generic amount/code rules.
# MAGIC - `DAYS_BIRTH`, `DAYS_EMPLOYED` - age and job tenure; short employment
# MAGIC   relative to age is a classic thin-file instability signal.
# MAGIC - `AMT_CREDIT` / `AMT_ANNUITY` / `AMT_INCOME_TOTAL` - credit-to-income
# MAGIC   and annuity-to-income ratios (computed in Gold) are standard
# MAGIC   affordability signals.
# MAGIC - `NAME_EDUCATION_TYPE`, `NAME_INCOME_TYPE`, `OCCUPATION_TYPE` - stable
# MAGIC   categorical risk segments.
# MAGIC
# MAGIC **Data-quality watch-outs:**
# MAGIC - `EXT_SOURCE_1` has the highest null rate of the three (checked below) -
# MAGIC   expect Gold-layer imputation/handling, not a Silver-layer job.
# MAGIC - `AMT_REQ_CREDIT_BUREAU_{HOUR,DAY,WEEK,MON,QRT,YEAR}` are **counts** of
# MAGIC   bureau enquiries despite the `AMT_` prefix - a real naming quirk in
# MAGIC   the source dataset. The mechanical Silver naming rule would mislabel
# MAGIC   these `...Amt`; `notebooks/silver/application_train.py` overrides
# MAGIC   them to `...Cnt`.
# MAGIC - All `DAYS_*` columns are negative day-counts relative to the
# MAGIC   application date, not calendar dates - `DAYS_EMPLOYED` in particular
# MAGIC   has a well-known sentinel value (365243, ~18% of rows here) for "not
# MAGIC   currently employed" that isn't a real day count.
# MAGIC   `notebooks/silver/application_train.py` nulls it out via
# MAGIC   `SilverTableConfig.sentinel_nulls` rather than dropping the row or
# MAGIC   leaving the nonsense value in place.

# COMMAND ----------

display(null_rate(application_train, ["EXT_SOURCE_1", "EXT_SOURCE_2", "EXT_SOURCE_3", "OCCUPATION_TYPE"]))

print("DAYS_EMPLOYED sentinel (365243) rows:", application_train.filter(F.col("DAYS_EMPLOYED") == 365243).count())
