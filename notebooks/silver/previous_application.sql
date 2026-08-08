-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## previous_application — profiling
-- MAGIC
-- MAGIC **What is this table?** Every prior Home Credit loan *application*
-- MAGIC (not just disbursed credits — an application can be rejected or
-- MAGIC cancelled) for clients who have a loan in our sample.
-- MAGIC
-- MAGIC **What's one row?** `SK_ID_PREV` — primary key, one row per previous
-- MAGIC application. `SK_ID_CURR` is also present but not unique (one applicant
-- MAGIC can have many previous applications).
-- MAGIC
-- MAGIC **How do I connect it to an applicant?** Join on `SK_ID_CURR` directly
-- MAGIC back to `application_train`/`application_test`. Three tables hang off
-- MAGIC *this* table via `SK_ID_PREV`: `POS_CASH_balance`, `credit_card_balance`,
-- MAGIC and `installments_payments` — monthly/event history for whatever credit
-- MAGIC resulted from a given previous application.
-- MAGIC
-- MAGIC **Why does it matter for predicting default?** Unlike `bureau`, this is
-- MAGIC history *with Home Credit itself* — a repeat applicant's past
-- MAGIC approval/rejection pattern is a direct prior on their current
-- MAGIC application's risk, in a way external bureau data can only approximate.
-- MAGIC
-- MAGIC **Which columns matter most?** `NAME_CONTRACT_STATUS`
-- MAGIC (Approved/Refused/Cancelled/Unused) and `CODE_REJECT_REASON` capture
-- MAGIC Home Credit's own past risk decisions about this client. The gap
-- MAGIC between `AMT_APPLICATION` (what they asked for) and `AMT_CREDIT` (what
-- MAGIC they were actually granted) is a useful proxy for how much the
-- MAGIC underwriting process trusted them.
-- MAGIC
-- MAGIC **What should I watch out for?** `RATE_INTEREST_PRIMARY`/
-- MAGIC `RATE_INTEREST_PRIVILEGED` are known to be almost entirely null in this
-- MAGIC dataset — not a data-quality bug to fix. `DAYS_FIRST_DRAWING` uses the
-- MAGIC same `365243` sentinel as `application`'s `DAYS_EMPLOYED`. Both are
-- MAGIC checked explicitly below.

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
