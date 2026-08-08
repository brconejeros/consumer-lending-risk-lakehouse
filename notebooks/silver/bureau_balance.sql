-- Databricks notebook source
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
