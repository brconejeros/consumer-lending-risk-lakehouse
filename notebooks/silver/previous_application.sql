-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## previous_application — profiling
-- MAGIC
-- MAGIC Every prior Home Credit loan *application* (not just disbursed credits —
-- MAGIC an application can be rejected or cancelled) for clients who have a loan
-- MAGIC in our sample. Grain is one row per previous application (`SK_ID_PREV`).
-- MAGIC Three tables hang off this one via `SK_ID_PREV`: `POS_CASH_balance`,
-- MAGIC `credit_card_balance`, and `installments_payments` — all monthly/event
-- MAGIC history for whatever credit resulted from a given previous application.
-- MAGIC
-- MAGIC **Why this matters:** unlike `bureau`, this is history *with Home
-- MAGIC Credit itself* — a repeat applicant's past approval/rejection pattern
-- MAGIC is a direct prior on their current application's risk, in a way
-- MAGIC external bureau data can only approximate.
-- MAGIC
-- MAGIC **Key columns:** `NAME_CONTRACT_STATUS` (Approved/Refused/Cancelled/
-- MAGIC Unused) and `CODE_REJECT_REASON` capture Home Credit's own past risk
-- MAGIC decisions about this client. The gap between `AMT_APPLICATION` (what
-- MAGIC they asked for) and `AMT_CREDIT` (what they were actually granted) is a
-- MAGIC useful proxy for how much the underwriting process trusted them.
-- MAGIC
-- MAGIC Several fields here are known from the public Home Credit dataset to be
-- MAGIC almost entirely null (`RATE_INTEREST_PRIMARY`/`RATE_INTEREST_PRIVILEGED`)
-- MAGIC or to use a `365243` sentinel in place of a real day count
-- MAGIC (`DAYS_FIRST_DRAWING`) — both are checked explicitly below.

-- COMMAND ----------

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
