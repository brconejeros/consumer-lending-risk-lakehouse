-- Databricks notebook source
-- Row count and uniqueness of (SK_ID_PREV, MONTHS_BALANCE)
SELECT
  COUNT(*) AS row_count,
  COUNT(DISTINCT SK_ID_PREV) AS distinct_sk_id_prev,
  COUNT(DISTINCT SK_ID_PREV, MONTHS_BALANCE) AS distinct_key_combo,
  COUNT(*) - COUNT(DISTINCT SK_ID_PREV, MONTHS_BALANCE) AS duplicate_key_combo
FROM consumer_lending_risk_lakehouse.bronze.POS_CASH_balance;

-- COMMAND ----------

-- FK checks: SK_ID_PREV -> previous_application, SK_ID_CURR -> application
SELECT
  (SELECT COUNT(*) FROM consumer_lending_risk_lakehouse.bronze.POS_CASH_balance p
   WHERE NOT EXISTS (
     SELECT 1 FROM consumer_lending_risk_lakehouse.bronze.previous_application pa WHERE pa.SK_ID_PREV = p.SK_ID_PREV
   )) AS orphan_sk_id_prev_count,
  (SELECT COUNT(*) FROM consumer_lending_risk_lakehouse.bronze.POS_CASH_balance p
   WHERE NOT EXISTS (
       SELECT 1 FROM consumer_lending_risk_lakehouse.bronze.application_train a WHERE a.SK_ID_CURR = p.SK_ID_CURR
     )
     AND NOT EXISTS (
       SELECT 1 FROM consumer_lending_risk_lakehouse.bronze.application_test t WHERE t.SK_ID_CURR = p.SK_ID_CURR
     )) AS orphan_sk_id_curr_count;

-- COMMAND ----------

-- Null rate on Silver-relevant columns
SELECT
  COUNT(*) AS total_rows,
  ROUND(100.0 * SUM(CASE WHEN CNT_INSTALMENT IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_cnt_instalment,
  ROUND(100.0 * SUM(CASE WHEN CNT_INSTALMENT_FUTURE IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_cnt_instalment_future
FROM consumer_lending_risk_lakehouse.bronze.POS_CASH_balance;

-- COMMAND ----------

-- Range checks
SELECT
  MIN(SK_DPD) AS min_sk_dpd,
  MAX(SK_DPD) AS max_sk_dpd,
  AVG(SK_DPD) AS avg_sk_dpd,
  MIN(SK_DPD_DEF) AS min_sk_dpd_def,
  MAX(SK_DPD_DEF) AS max_sk_dpd_def,
  MIN(MONTHS_BALANCE) AS min_months_balance,
  MAX(MONTHS_BALANCE) AS max_months_balance
FROM consumer_lending_risk_lakehouse.bronze.POS_CASH_balance;

-- COMMAND ----------

-- Distinct NAME_CONTRACT_STATUS values
SELECT NAME_CONTRACT_STATUS, COUNT(*) AS row_count
FROM consumer_lending_risk_lakehouse.bronze.POS_CASH_balance
GROUP BY NAME_CONTRACT_STATUS
ORDER BY NAME_CONTRACT_STATUS;
