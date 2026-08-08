-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## POS_CASH_balance — profiling
-- MAGIC
-- MAGIC **What is this table?** Monthly balance snapshots for the applicant's
-- MAGIC previous Home Credit point-of-sale and cash loans — same shape as
-- MAGIC `bureau_balance`, but for Home Credit's *own* previous credits rather
-- MAGIC than external Credit Bureau ones.
-- MAGIC
-- MAGIC **What's one row?** (`SK_ID_PREV`, `MONTHS_BALANCE`) — a composite key.
-- MAGIC
-- MAGIC **How do I connect it to an applicant?** `SK_ID_CURR` is carried
-- MAGIC directly (→ `application_train`/`application_test`), so no join through
-- MAGIC `previous_application` is required to reach the applicant — though
-- MAGIC `SK_ID_PREV` (→ `previous_application`) is also available and both FK
-- MAGIC paths are checked below.
-- MAGIC
-- MAGIC **Why does it matter for predicting default?** This is direct, literal
-- MAGIC repayment behavior on Home Credit's own products — the single most
-- MAGIC trustworthy "did this person pay us back on time before" signal
-- MAGIC available, since it isn't filtered through a third party the way
-- MAGIC `bureau`/`bureau_balance` are.
-- MAGIC
-- MAGIC **Which columns matter most?** `SK_DPD`/`SK_DPD_DEF` (days past due,
-- MAGIC with and without a tolerance for small amounts) are the key
-- MAGIC delinquency signal this table contributes to Gold. `NAME_CONTRACT_STATUS`
-- MAGIC (active/completed/signed) contextualizes whether a DPD reading came
-- MAGIC from a still-open or already-closed credit.
-- MAGIC
-- MAGIC **What should I watch out for?** Same row/column shape as
-- MAGIC `bureau_balance` — don't confuse the two. This one is Home Credit's own
-- MAGIC history; `bureau_balance` is external.

-- COMMAND ----------

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
