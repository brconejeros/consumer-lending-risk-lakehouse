-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## bureau — profiling
-- MAGIC
-- MAGIC All of a client's previous credits from *other* financial institutions
-- MAGIC that were reported to Credit Bureau, for clients who have a loan in our
-- MAGIC sample. Grain is one row per previous CB credit (`SK_ID_BUREAU`) — a
-- MAGIC given `SK_ID_CURR` can have zero, one, or many rows here.
-- MAGIC `bureau_balance` hangs off this table via `SK_ID_BUREAU` (monthly balance
-- MAGIC history per CB credit).
-- MAGIC
-- MAGIC **Why this matters:** this table *is* the "robust bank history" the
-- MAGIC business problem is framed around — for applicants Home Credit has no
-- MAGIC track record with, external Credit Bureau data is the only window into
-- MAGIC how they've handled credit elsewhere.
-- MAGIC
-- MAGIC **Key columns:** `CREDIT_ACTIVE` (active vs. closed credits) and
-- MAGIC `AMT_CREDIT_SUM_DEBT` (current outstanding debt) drive a client's overall
-- MAGIC external debt exposure. `CREDIT_DAY_OVERDUE` and `AMT_CREDIT_MAX_OVERDUE`
-- MAGIC are direct delinquency signals. `DAYS_CREDIT` (recency) matters because
-- MAGIC recent bureau activity is more predictive than credits from years ago.
-- MAGIC
-- MAGIC These queries check `SK_ID_BUREAU` uniqueness, that every `SK_ID_CURR`
-- MAGIC traces back to an application row, null rates/ranges on the debt and
-- MAGIC overdue fields, and the `CREDIT_ACTIVE`/`CREDIT_TYPE`/`CREDIT_CURRENCY`
-- MAGIC category distributions.

-- COMMAND ----------

-- Row count and SK_ID_BUREAU uniqueness
SELECT
  COUNT(*) AS row_count,
  COUNT(DISTINCT SK_ID_BUREAU) AS distinct_sk_id_bureau,
  COUNT(*) - COUNT(DISTINCT SK_ID_BUREAU) AS duplicate_sk_id_bureau
FROM consumer_lending_risk_lakehouse.bronze.bureau;

-- COMMAND ----------

-- FK check: SK_ID_CURR must exist in application_train or application_test
SELECT COUNT(*) AS orphan_sk_id_curr_count
FROM consumer_lending_risk_lakehouse.bronze.bureau b
WHERE NOT EXISTS (
    SELECT 1 FROM consumer_lending_risk_lakehouse.bronze.application_train a WHERE a.SK_ID_CURR = b.SK_ID_CURR
  )
  AND NOT EXISTS (
    SELECT 1 FROM consumer_lending_risk_lakehouse.bronze.application_test t WHERE t.SK_ID_CURR = b.SK_ID_CURR
  );

-- COMMAND ----------

-- Null rate on Silver-relevant columns
SELECT
  COUNT(*) AS total_rows,
  ROUND(100.0 * SUM(CASE WHEN AMT_CREDIT_SUM_DEBT IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_amt_credit_sum_debt,
  ROUND(100.0 * SUM(CASE WHEN DAYS_CREDIT_ENDDATE IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_days_credit_enddate,
  ROUND(100.0 * SUM(CASE WHEN AMT_CREDIT_MAX_OVERDUE IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_amt_credit_max_overdue,
  ROUND(100.0 * SUM(CASE WHEN AMT_ANNUITY IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_amt_annuity
FROM consumer_lending_risk_lakehouse.bronze.bureau;

-- COMMAND ----------

-- Range checks
SELECT
  MIN(DAYS_CREDIT) AS min_days_credit,
  MAX(DAYS_CREDIT) AS max_days_credit,
  AVG(DAYS_CREDIT) AS avg_days_credit,
  MIN(CREDIT_DAY_OVERDUE) AS min_credit_day_overdue,
  MAX(CREDIT_DAY_OVERDUE) AS max_credit_day_overdue,
  MIN(AMT_CREDIT_SUM) AS min_amt_credit_sum,
  MAX(AMT_CREDIT_SUM) AS max_amt_credit_sum,
  AVG(AMT_CREDIT_SUM) AS avg_amt_credit_sum
FROM consumer_lending_risk_lakehouse.bronze.bureau;

-- COMMAND ----------

-- Distinct values on key categorical fields
SELECT 'CREDIT_ACTIVE' AS column_name, CREDIT_ACTIVE AS value, COUNT(*) AS row_count FROM consumer_lending_risk_lakehouse.bronze.bureau GROUP BY CREDIT_ACTIVE
UNION ALL
SELECT 'CREDIT_TYPE', CREDIT_TYPE, COUNT(*) FROM consumer_lending_risk_lakehouse.bronze.bureau GROUP BY CREDIT_TYPE
UNION ALL
SELECT 'CREDIT_CURRENCY', CREDIT_CURRENCY, COUNT(*) FROM consumer_lending_risk_lakehouse.bronze.bureau GROUP BY CREDIT_CURRENCY
ORDER BY column_name, value;
