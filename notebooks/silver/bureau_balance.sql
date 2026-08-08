-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## bureau_balance — profiling
-- MAGIC
-- MAGIC **What is this table?** Monthly balance/status history for credits
-- MAGIC reported to Credit Bureau — this is the largest table in the dataset
-- MAGIC (~27.3M rows, per `CLAUDE.md`'s "Status") because it's `bureau` rows
-- MAGIC multiplied out by however many months of history each CB credit has.
-- MAGIC
-- MAGIC **What's one row?** (`SK_ID_BUREAU`, `MONTHS_BALANCE`) — a composite
-- MAGIC key, no single-column primary key.
-- MAGIC
-- MAGIC **How do I connect it to an applicant?** Join to `bureau` on
-- MAGIC `SK_ID_BUREAU` first — this table has no `SK_ID_CURR` column at all.
-- MAGIC
-- MAGIC **Why does it matter for predicting default?** `bureau` tells you a
-- MAGIC client *had* external credit; `bureau_balance` tells you how they
-- MAGIC *behaved* on it month by month — a much stronger risk signal than the
-- MAGIC credit's existence alone, and the closest external analogue to the
-- MAGIC payment-history behavior `installments_payments` captures for Home
-- MAGIC Credit's own products.
-- MAGIC
-- MAGIC **Which columns matter most?** `STATUS`, particularly the DPD buckets
-- MAGIC — `1` (1-30 days past due) through `5` (120+ days past due or written
-- MAGIC off/sold). Aggregating the worst-ever `STATUS` and the share of months
-- MAGIC spent delinquent per `SK_ID_BUREAU` is a standard feature-engineering
-- MAGIC move for this dataset.
-- MAGIC
-- MAGIC **What should I watch out for?** No `SK_ID_CURR` column — a naive join
-- MAGIC straight to `application_train` will silently return zero rows rather
-- MAGIC than erroring. The FK check below (every `SK_ID_BUREAU` must exist in
-- MAGIC `bureau`) is the one `CLAUDE.md`'s "Data quality" section calls out
-- MAGIC explicitly.

-- COMMAND ----------

-- Row count and uniqueness of (SK_ID_BUREAU, MONTHS_BALANCE)
SELECT
  COUNT(*) AS row_count,
  COUNT(DISTINCT SK_ID_BUREAU) AS distinct_sk_id_bureau,
  COUNT(DISTINCT SK_ID_BUREAU, MONTHS_BALANCE) AS distinct_key_combo,
  COUNT(*) - COUNT(DISTINCT SK_ID_BUREAU, MONTHS_BALANCE) AS duplicate_key_combo
FROM consumer_lending_risk_lakehouse.bronze.bureau_balance;

-- COMMAND ----------

-- FK check: every SK_ID_BUREAU must exist in bureau
SELECT COUNT(*) AS orphan_sk_id_bureau_count
FROM consumer_lending_risk_lakehouse.bronze.bureau_balance bb
LEFT ANTI JOIN consumer_lending_risk_lakehouse.bronze.bureau b
  ON b.SK_ID_BUREAU = bb.SK_ID_BUREAU;

-- COMMAND ----------

-- Range check
SELECT
  MIN(MONTHS_BALANCE) AS min_months_balance,
  MAX(MONTHS_BALANCE) AS max_months_balance,
  AVG(MONTHS_BALANCE) AS avg_months_balance
FROM consumer_lending_risk_lakehouse.bronze.bureau_balance;

-- COMMAND ----------

-- Distinct STATUS values
SELECT STATUS, COUNT(*) AS row_count
FROM consumer_lending_risk_lakehouse.bronze.bureau_balance
GROUP BY STATUS
ORDER BY STATUS;
