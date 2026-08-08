-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## installments_payments — profiling
-- MAGIC
-- MAGIC **What is this table?** Repayment history for previously disbursed
-- MAGIC Home Credit credits. Unlike the balance tables, this isn't a monthly
-- MAGIC snapshot — it's one row per installment payment event, made or missed.
-- MAGIC
-- MAGIC **What's one row?** (`SK_ID_PREV`, `NUM_INSTALMENT_NUMBER`,
-- MAGIC `NUM_INSTALMENT_VERSION`) — a composite key.
-- MAGIC
-- MAGIC **How do I connect it to an applicant?** `SK_ID_CURR` is carried
-- MAGIC directly (→ `application_train`/`application_test`); `SK_ID_PREV` (→
-- MAGIC `previous_application`) is also available, and both FK paths are
-- MAGIC checked below.
-- MAGIC
-- MAGIC **Why does it matter for predicting default?** This is the most
-- MAGIC granular repayment behavior available anywhere in the dataset — actual
-- MAGIC payment timing and amount vs. what was scheduled, per installment.
-- MAGIC Aggregated per applicant (average delay, worst delay, count of missed
-- MAGIC payments), it's typically one of the single strongest feature groups
-- MAGIC for predicting `TARGET` in public analyses of this dataset.
-- MAGIC
-- MAGIC **Which columns matter most?** `DAYS_ENTRY_PAYMENT - DAYS_INSTALMENT`
-- MAGIC (actual vs. scheduled payment date) is the core late-payment signal
-- MAGIC this table contributes to Gold. `AMT_PAYMENT` relative to
-- MAGIC `AMT_INSTALMENT` (underpayment) is the equivalent signal for partial
-- MAGIC rather than late payments.
-- MAGIC
-- MAGIC **What should I watch out for?** A null `AMT_PAYMENT`/
-- MAGIC `DAYS_ENTRY_PAYMENT` represents a *missed* installment, not a
-- MAGIC data-quality defect — filtering those rows out (rather than counting
-- MAGIC them) would silently throw away the delinquency signal this table
-- MAGIC exists to capture.

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
