-- Databricks notebook source
-- Row count and SK_ID_PREV uniqueness
SELECT
  COUNT(*) AS row_count,
  COUNT(DISTINCT SK_ID_PREV) AS distinct_sk_id_prev,
  COUNT(*) - COUNT(DISTINCT SK_ID_PREV) AS duplicate_sk_id_prev
FROM consumer_lending_risk_lakehouse.bronze.previous_application;

-- COMMAND ----------

-- FK check: SK_ID_CURR must exist in application_train or application_test
SELECT COUNT(*) AS orphan_sk_id_curr_count
FROM consumer_lending_risk_lakehouse.bronze.previous_application p
WHERE NOT EXISTS (
    SELECT 1 FROM consumer_lending_risk_lakehouse.bronze.application_train a WHERE a.SK_ID_CURR = p.SK_ID_CURR
  )
  AND NOT EXISTS (
    SELECT 1 FROM consumer_lending_risk_lakehouse.bronze.application_test t WHERE t.SK_ID_CURR = p.SK_ID_CURR
  );

-- COMMAND ----------

-- Null rate on Silver-relevant columns
SELECT
  COUNT(*) AS total_rows,
  ROUND(100.0 * SUM(CASE WHEN RATE_INTEREST_PRIMARY IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_rate_interest_primary,
  ROUND(100.0 * SUM(CASE WHEN RATE_INTEREST_PRIVILEGED IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_rate_interest_privileged,
  ROUND(100.0 * SUM(CASE WHEN AMT_DOWN_PAYMENT IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_amt_down_payment,
  ROUND(100.0 * SUM(CASE WHEN AMT_ANNUITY IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_amt_annuity,
  ROUND(100.0 * SUM(CASE WHEN CNT_PAYMENT IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_cnt_payment
FROM consumer_lending_risk_lakehouse.bronze.previous_application;

-- COMMAND ----------

-- Range checks and DAYS_FIRST_DRAWING sentinel
SELECT
  MIN(AMT_APPLICATION) AS min_amt_application,
  MAX(AMT_APPLICATION) AS max_amt_application,
  MIN(AMT_CREDIT) AS min_amt_credit,
  MAX(AMT_CREDIT) AS max_amt_credit,
  MIN(DAYS_DECISION) AS min_days_decision,
  MAX(DAYS_DECISION) AS max_days_decision,
  SUM(CASE WHEN DAYS_FIRST_DRAWING = 365243 THEN 1 ELSE 0 END) AS days_first_drawing_sentinel_count,
  MIN(CASE WHEN DAYS_FIRST_DRAWING != 365243 THEN DAYS_FIRST_DRAWING END) AS min_days_first_drawing_excl_sentinel,
  MAX(CASE WHEN DAYS_FIRST_DRAWING != 365243 THEN DAYS_FIRST_DRAWING END) AS max_days_first_drawing_excl_sentinel
FROM consumer_lending_risk_lakehouse.bronze.previous_application;

-- COMMAND ----------

-- Distinct values on key categorical fields
SELECT 'NAME_CONTRACT_STATUS' AS column_name, NAME_CONTRACT_STATUS AS value, COUNT(*) AS row_count FROM consumer_lending_risk_lakehouse.bronze.previous_application GROUP BY NAME_CONTRACT_STATUS
UNION ALL
SELECT 'NAME_CONTRACT_TYPE', NAME_CONTRACT_TYPE, COUNT(*) FROM consumer_lending_risk_lakehouse.bronze.previous_application GROUP BY NAME_CONTRACT_TYPE
UNION ALL
SELECT 'CODE_REJECT_REASON', CODE_REJECT_REASON, COUNT(*) FROM consumer_lending_risk_lakehouse.bronze.previous_application GROUP BY CODE_REJECT_REASON
ORDER BY column_name, value;
