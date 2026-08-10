# Databricks notebook source
# MAGIC %md
# MAGIC # Silver profiling: application_test
# MAGIC
# MAGIC Exploratory, not a pipeline stage - before `notebooks/silver/
# MAGIC application_test.py` conforms it. Shares `application_train`'s schema
# MAGIC and grain exactly, minus `TARGET` (it's the held-out sample) - see
# MAGIC `application_train.py`'s profiling notebook for the full business-
# MAGIC relevance/key-predictive-columns writeup, which applies here too. This
# MAGIC notebook only covers what's specific to `application_test`: does it
# MAGIC actually match `application_train`'s shape and null patterns.
# MAGIC
# MAGIC **Grain:** one row per `SK_ID_CURR`.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))

from src.lakehouse.profiling import null_rate

CATALOG = "consumer_lending_risk_lakehouse"

# COMMAND ----------

application_train = spark.table(f"{CATALOG}.bronze.application_train")
application_test = spark.table(f"{CATALOG}.bronze.application_test")

print(f"application_test: rows={application_test.count()}, distinct SK_ID_CURR={application_test.select('SK_ID_CURR').distinct().count()}")
print("columns only in application_train:", set(application_train.columns) - set(application_test.columns))
display(application_test.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC **Data-quality watch-outs:** the only expected schema difference from
# MAGIC `application_train` is the missing `TARGET` column (confirmed above).
# MAGIC Null rates on the same key columns should track `application_train`'s
# MAGIC closely - a large divergence would suggest the train/test split isn't a
# MAGIC simple random sample.

# COMMAND ----------

display(null_rate(application_test, ["EXT_SOURCE_1", "EXT_SOURCE_2", "EXT_SOURCE_3", "OCCUPATION_TYPE"]))
