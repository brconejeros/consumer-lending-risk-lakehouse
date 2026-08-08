-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## credit_card_balance — profiling
-- MAGIC
-- MAGIC Monthly balance snapshots for the applicant's previous Home Credit
-- MAGIC credit cards. Same grain and FK shape as `POS_CASH_balance`
-- MAGIC ((`SK_ID_PREV`, `MONTHS_BALANCE`), with both `SK_ID_PREV` and
-- MAGIC `SK_ID_CURR` carried directly), but for revolving credit card products
-- MAGIC instead of POS/cash loans.
-- MAGIC
-- MAGIC The `AMT_DRAWINGS_*` fields are known to have high null rates in this
-- MAGIC dataset (cards with no drawing activity that month) — quantified below
-- MAGIC rather than assumed.

-- COMMAND ----------

-- Row count and uniqueness of (SK_ID_PREV, MONTHS_BALANCE)
SELECT
  COUNT(*) AS row_count,
  COUNT(DISTINCT SK_ID_PREV) AS distinct_sk_id_prev,
  COUNT(DISTINCT SK_ID_PREV, MONTHS_BALANCE) AS distinct_key_combo,
  COUNT(*) - COUNT(DISTINCT SK_ID_PREV, MONTHS_BALANCE) AS duplicate_key_combo
FROM consumer_lending_risk_lakehouse.bronze.credit_card_balance;

-- COMMAND ----------

-- FK checks: SK_ID_PREV -> previous_application, SK_ID_CURR -> application
SELECT
  (SELECT COUNT(*) FROM consumer_lending_risk_lakehouse.bronze.credit_card_balance c
   WHERE NOT EXISTS (
     SELECT 1 FROM consumer_lending_risk_lakehouse.bronze.previous_application pa WHERE pa.SK_ID_PREV = c.SK_ID_PREV
   )) AS orphan_sk_id_prev_count,
  (SELECT COUNT(*) FROM consumer_lending_risk_lakehouse.bronze.credit_card_balance c
   WHERE NOT EXISTS (
       SELECT 1 FROM consumer_lending_risk_lakehouse.bronze.application_train a WHERE a.SK_ID_CURR = c.SK_ID_CURR
     )
     AND NOT EXISTS (
       SELECT 1 FROM consumer_lending_risk_lakehouse.bronze.application_test t WHERE t.SK_ID_CURR = c.SK_ID_CURR
     )) AS orphan_sk_id_curr_count;

-- COMMAND ----------

-- Null rate on Silver-relevant columns
SELECT
  COUNT(*) AS total_rows,
  ROUND(100.0 * SUM(CASE WHEN AMT_DRAWINGS_ATM_CURRENT IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_amt_drawings_atm_current,
  ROUND(100.0 * SUM(CASE WHEN AMT_DRAWINGS_OTHER_CURRENT IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_amt_drawings_other_current,
  ROUND(100.0 * SUM(CASE WHEN AMT_DRAWINGS_POS_CURRENT IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_amt_drawings_pos_current,
  ROUND(100.0 * SUM(CASE WHEN AMT_PAYMENT_CURRENT IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_amt_payment_current
FROM consumer_lending_risk_lakehouse.bronze.credit_card_balance;

-- COMMAND ----------

-- Range checks
SELECT
  MIN(AMT_BALANCE) AS min_amt_balance,
  MAX(AMT_BALANCE) AS max_amt_balance,
  AVG(AMT_BALANCE) AS avg_amt_balance,
  MIN(SK_DPD) AS min_sk_dpd,
  MAX(SK_DPD) AS max_sk_dpd
FROM consumer_lending_risk_lakehouse.bronze.credit_card_balance;

-- COMMAND ----------

-- Distinct NAME_CONTRACT_STATUS values
SELECT NAME_CONTRACT_STATUS, COUNT(*) AS row_count
FROM consumer_lending_risk_lakehouse.bronze.credit_card_balance
GROUP BY NAME_CONTRACT_STATUS
ORDER BY NAME_CONTRACT_STATUS;
