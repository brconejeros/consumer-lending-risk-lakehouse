-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## application_test — profiling
-- MAGIC
-- MAGIC **What is this table?** Same shape as `application_train`, minus the
-- MAGIC `TARGET` label — the holdout sample the business problem is ultimately
-- MAGIC scored against.
-- MAGIC
-- MAGIC **What's one row?** `SK_ID_CURR` — primary key, one row per loan
-- MAGIC application.
-- MAGIC
-- MAGIC **How do I connect it to an applicant?** Same as `application_train` —
-- MAGIC every satellite table (`bureau`, `previous_application`, and what hangs
-- MAGIC off them) reaches back to this table (or `application_train`) via
-- MAGIC `SK_ID_CURR`.
-- MAGIC
-- MAGIC **Why does it matter for predicting default?** This is the set of
-- MAGIC applicants the whole pipeline ultimately has to answer the business
-- MAGIC question for — no `TARGET` to learn from, so the answer has to come
-- MAGIC entirely from `application_test`'s own fields plus everything joined in
-- MAGIC from the other tables.
-- MAGIC
-- MAGIC **Which columns matter most?** Same predictive columns as
-- MAGIC `application_train` (`EXT_SOURCE_1/2/3`, `DAYS_BIRTH`, `DAYS_EMPLOYED`,
-- MAGIC `AMT_CREDIT`/`AMT_INCOME_TOTAL`) — profiled here mainly to confirm the
-- MAGIC same null-rate/range/category patterns hold as in the training sample.
-- MAGIC
-- MAGIC **What should I watch out for?** Same `DAYS_EMPLOYED` sentinel as
-- MAGIC `application_train`; also worth checking that category values seen in
-- MAGIC training (e.g. `NAME_INCOME_TYPE`) are consistent here — a Silver
-- MAGIC transform tuned only against `application_train` risks breaking on
-- MAGIC unseen categories or shape differences in `application_test`.

-- COMMAND ----------

-- Row count and SK_ID_CURR uniqueness
SELECT
  COUNT(*) AS row_count,
  COUNT(DISTINCT SK_ID_CURR) AS distinct_sk_id_curr,
  COUNT(*) - COUNT(DISTINCT SK_ID_CURR) AS duplicate_sk_id_curr
FROM consumer_lending_risk_lakehouse.bronze.application_test;

-- COMMAND ----------

-- Null rate on Silver-relevant columns
SELECT
  COUNT(*) AS total_rows,
  ROUND(100.0 * SUM(CASE WHEN EXT_SOURCE_1 IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_ext_source_1,
  ROUND(100.0 * SUM(CASE WHEN EXT_SOURCE_2 IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_ext_source_2,
  ROUND(100.0 * SUM(CASE WHEN EXT_SOURCE_3 IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_ext_source_3,
  ROUND(100.0 * SUM(CASE WHEN OCCUPATION_TYPE IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_occupation_type,
  ROUND(100.0 * SUM(CASE WHEN AMT_ANNUITY IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_amt_annuity,
  ROUND(100.0 * SUM(CASE WHEN AMT_GOODS_PRICE IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_amt_goods_price,
  ROUND(100.0 * SUM(CASE WHEN NAME_TYPE_SUITE IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_name_type_suite
FROM consumer_lending_risk_lakehouse.bronze.application_test;

-- COMMAND ----------

-- Range checks: age (from DAYS_BIRTH), DAYS_EMPLOYED sentinel, income/credit/children
SELECT
  MIN(-DAYS_BIRTH / 365.25) AS min_age_years,
  MAX(-DAYS_BIRTH / 365.25) AS max_age_years,
  AVG(-DAYS_BIRTH / 365.25) AS avg_age_years,
  SUM(CASE WHEN DAYS_EMPLOYED = 365243 THEN 1 ELSE 0 END) AS days_employed_sentinel_count,
  MIN(CASE WHEN DAYS_EMPLOYED != 365243 THEN DAYS_EMPLOYED END) AS min_days_employed_excl_sentinel,
  MAX(CASE WHEN DAYS_EMPLOYED != 365243 THEN DAYS_EMPLOYED END) AS max_days_employed_excl_sentinel,
  MIN(AMT_INCOME_TOTAL) AS min_income,
  MAX(AMT_INCOME_TOTAL) AS max_income,
  AVG(AMT_INCOME_TOTAL) AS avg_income,
  MIN(AMT_CREDIT) AS min_credit,
  MAX(AMT_CREDIT) AS max_credit,
  AVG(AMT_CREDIT) AS avg_credit,
  MIN(CNT_CHILDREN) AS min_cnt_children,
  MAX(CNT_CHILDREN) AS max_cnt_children
FROM consumer_lending_risk_lakehouse.bronze.application_test;

-- COMMAND ----------

-- Distinct values on key categorical fields
SELECT 'CODE_GENDER' AS column_name, CODE_GENDER AS value, COUNT(*) AS row_count FROM consumer_lending_risk_lakehouse.bronze.application_test GROUP BY CODE_GENDER
UNION ALL
SELECT 'NAME_CONTRACT_TYPE', NAME_CONTRACT_TYPE, COUNT(*) FROM consumer_lending_risk_lakehouse.bronze.application_test GROUP BY NAME_CONTRACT_TYPE
UNION ALL
SELECT 'NAME_INCOME_TYPE', NAME_INCOME_TYPE, COUNT(*) FROM consumer_lending_risk_lakehouse.bronze.application_test GROUP BY NAME_INCOME_TYPE
UNION ALL
SELECT 'NAME_EDUCATION_TYPE', NAME_EDUCATION_TYPE, COUNT(*) FROM consumer_lending_risk_lakehouse.bronze.application_test GROUP BY NAME_EDUCATION_TYPE
UNION ALL
SELECT 'NAME_FAMILY_STATUS', NAME_FAMILY_STATUS, COUNT(*) FROM consumer_lending_risk_lakehouse.bronze.application_test GROUP BY NAME_FAMILY_STATUS
UNION ALL
SELECT 'NAME_HOUSING_TYPE', NAME_HOUSING_TYPE, COUNT(*) FROM consumer_lending_risk_lakehouse.bronze.application_test GROUP BY NAME_HOUSING_TYPE
ORDER BY column_name, value;
