-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## installments_payments — profiling
-- MAGIC
-- MAGIC Repayment history for previously disbursed Home Credit credits. Unlike
-- MAGIC the balance tables, this isn't a monthly snapshot — it's one row per
-- MAGIC installment payment event (made or missed), keyed by
-- MAGIC (`SK_ID_PREV`, `NUM_INSTALMENT_NUMBER`, `NUM_INSTALMENT_VERSION`). A null
-- MAGIC `AMT_PAYMENT`/`DAYS_ENTRY_PAYMENT` represents a missed installment, not a
-- MAGIC data-quality defect — quantified explicitly rather than filtered out.
-- MAGIC
-- MAGIC **Why this matters:** this is the most granular repayment behavior
-- MAGIC available anywhere in the dataset — actual payment timing and amount
-- MAGIC vs. what was scheduled, per installment. Aggregated per applicant
-- MAGIC (average delay, worst delay, count of missed payments), it's typically
-- MAGIC one of the single strongest feature groups for predicting `TARGET` in
-- MAGIC public analyses of this dataset.
-- MAGIC
-- MAGIC **Key columns:** `DAYS_ENTRY_PAYMENT - DAYS_INSTALMENT` (actual vs.
-- MAGIC scheduled payment date) is the core late-payment signal this table
-- MAGIC contributes to Gold. `AMT_PAYMENT` relative to `AMT_INSTALMENT`
-- MAGIC (underpayment) is the equivalent signal for partial rather than late
-- MAGIC payments.

-- COMMAND ----------

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
