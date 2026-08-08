-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## bureau_balance — profiling
-- MAGIC
-- MAGIC Monthly balance history for each credit in `bureau` — grain is one row
-- MAGIC per (`SK_ID_BUREAU`, `MONTHS_BALANCE`). This is the largest table in the
-- MAGIC dataset (~27.3M rows, per `CLAUDE.md`'s "Status") because it's `bureau`
-- MAGIC rows multiplied out by however many months of history each CB credit
-- MAGIC has. It doesn't carry `SK_ID_CURR` directly — reaching an application
-- MAGIC row means joining through `bureau` first.
-- MAGIC
-- MAGIC **Why this matters:** `bureau` tells you a client *had* external
-- MAGIC credit; `bureau_balance` tells you how they *behaved* on it month by
-- MAGIC month — a much stronger risk signal than the credit's existence alone,
-- MAGIC and the closest external analogue to the payment-history behavior
-- MAGIC `installments_payments` captures for Home Credit's own products.
-- MAGIC
-- MAGIC **Key columns:** `STATUS`, particularly the DPD buckets — `1`
-- MAGIC (1-30 days past due) through `5` (120+ days past due or written
-- MAGIC off/sold). Aggregating the worst-ever `STATUS` and the share of months
-- MAGIC spent delinquent per `SK_ID_BUREAU` is a standard feature-engineering
-- MAGIC move for this dataset worth planning for in Silver/Gold.
-- MAGIC
-- MAGIC The FK check here — every `SK_ID_BUREAU` must exist in `bureau` — is the
-- MAGIC one `CLAUDE.md`'s "Data quality" section calls out explicitly.

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
WHERE NOT EXISTS (
  SELECT 1 FROM consumer_lending_risk_lakehouse.bronze.bureau b WHERE b.SK_ID_BUREAU = bb.SK_ID_BUREAU
);

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
