-- Databricks notebook source
-- Row count
SELECT COUNT(*) AS row_count
FROM consumer_lending_risk_lakehouse.bronze.installments_payments;

-- COMMAND ----------

-- FK checks: SK_ID_PREV -> previous_application, SK_ID_CURR -> application
SELECT
  (SELECT COUNT(*) FROM consumer_lending_risk_lakehouse.bronze.installments_payments i
   WHERE NOT EXISTS (
     SELECT 1 FROM consumer_lending_risk_lakehouse.bronze.previous_application pa WHERE pa.SK_ID_PREV = i.SK_ID_PREV
   )) AS orphan_sk_id_prev_count,
  (SELECT COUNT(*) FROM consumer_lending_risk_lakehouse.bronze.installments_payments i
   WHERE NOT EXISTS (
       SELECT 1 FROM consumer_lending_risk_lakehouse.bronze.application_train a WHERE a.SK_ID_CURR = i.SK_ID_CURR
     )
     AND NOT EXISTS (
       SELECT 1 FROM consumer_lending_risk_lakehouse.bronze.application_test t WHERE t.SK_ID_CURR = i.SK_ID_CURR
     )) AS orphan_sk_id_curr_count;

-- COMMAND ----------

-- Null rate: nulls here represent missed installments (per docs/data_dictionary.md)
SELECT
  COUNT(*) AS total_rows,
  SUM(CASE WHEN AMT_PAYMENT IS NULL THEN 1 ELSE 0 END) AS missed_payment_count,
  ROUND(100.0 * SUM(CASE WHEN AMT_PAYMENT IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missed_payment,
  SUM(CASE WHEN DAYS_ENTRY_PAYMENT IS NULL THEN 1 ELSE 0 END) AS null_days_entry_payment_count
FROM consumer_lending_risk_lakehouse.bronze.installments_payments;

-- COMMAND ----------

-- Installment number range and payment-delay signal (DAYS_ENTRY_PAYMENT vs DAYS_INSTALMENT)
SELECT
  MIN(NUM_INSTALMENT_NUMBER) AS min_instalment_number,
  MAX(NUM_INSTALMENT_NUMBER) AS max_instalment_number,
  MIN(DAYS_ENTRY_PAYMENT - DAYS_INSTALMENT) AS min_payment_delay_days,
  MAX(DAYS_ENTRY_PAYMENT - DAYS_INSTALMENT) AS max_payment_delay_days,
  AVG(DAYS_ENTRY_PAYMENT - DAYS_INSTALMENT) AS avg_payment_delay_days,
  SUM(CASE WHEN DAYS_ENTRY_PAYMENT > DAYS_INSTALMENT THEN 1 ELSE 0 END) AS late_payment_count
FROM consumer_lending_risk_lakehouse.bronze.installments_payments
WHERE DAYS_ENTRY_PAYMENT IS NOT NULL;
