# Silver-Layer Profiling Notebooks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add 8 Databricks SQL notebooks under `notebooks/silver/`, one per Bronze table, that run profiling/data-quality queries to inform the upcoming `01_silver_transform.py`.

**Architecture:** Each notebook is a standalone, read-only Databricks SQL notebook (`-- Databricks notebook source` header, `-- COMMAND ----------` cell separators) querying `consumer_lending_risk_lakehouse.bronze.<Table>` directly. No shared code, no writes — every file is independently runnable and independently reviewable.

**Tech Stack:** Databricks SQL (Spark SQL dialect), Unity Catalog.

## Global Constraints

- Catalog: `consumer_lending_risk_lakehouse`; source schema: `bronze` (read-only, no writes anywhere in this plan).
- File format: literal Databricks notebook export format — first line `-- Databricks notebook source`, cells separated by a blank line, `-- COMMAND ----------`, blank line.
- Table/column names use the source dataset's original casing (e.g. `SK_ID_CURR`, `POS_CASH_balance`) per `CLAUDE.md` → "Conventions". The physical bureau-credit join column is `SK_ID_BUREAU` (verified against `notebooks/bronze/bureau.py`'s `TABLE` constants and `docs/data_dictionary.md`'s note on the `SK_BUREAU_ID`/`SK_ID_BUREAU` naming discrepancy) — use `SK_ID_BUREAU`, not `SK_BUREAU_ID`.
- Bronze table names (from `notebooks/bronze/*.py`'s `TABLE` constants): `application_train`, `application_test`, `bureau`, `bureau_balance`, `previous_application`, `POS_CASH_balance`, `credit_card_balance`, `installments_payments`.
- Spec of record: `docs/superpowers/specs/2026-08-07-silver-profiling-notebooks-design.md` — every query below implements one of that spec's per-table bullet points.
- No execution in this session (no live Databricks connection available) — verification is a manual column/table-name cross-check against `docs/data_dictionary.md`, not a run. The user will run each notebook in Databricks after this lands to validate against real data.

---

### Task 1: `application_train.sql`

**Files:**
- Create: `notebooks/silver/application_train.sql`

**Interfaces:**
- Consumes: `consumer_lending_risk_lakehouse.bronze.application_train` (Delta table, already populated per project Status in `CLAUDE.md`)
- Produces: nothing consumed by other tasks — fully standalone

- [ ] **Step 1: Write the notebook**

```sql
-- Databricks notebook source
-- Row count and SK_ID_CURR uniqueness
SELECT
  COUNT(*) AS row_count,
  COUNT(DISTINCT SK_ID_CURR) AS distinct_sk_id_curr,
  COUNT(*) - COUNT(DISTINCT SK_ID_CURR) AS duplicate_sk_id_curr
FROM consumer_lending_risk_lakehouse.bronze.application_train;

-- COMMAND ----------

-- TARGET class balance
SELECT
  TARGET,
  COUNT(*) AS row_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM consumer_lending_risk_lakehouse.bronze.application_train
GROUP BY TARGET
ORDER BY TARGET;

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
FROM consumer_lending_risk_lakehouse.bronze.application_train;

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
FROM consumer_lending_risk_lakehouse.bronze.application_train;

-- COMMAND ----------

-- Distinct values on key categorical fields
SELECT 'CODE_GENDER' AS column_name, CODE_GENDER AS value, COUNT(*) AS row_count FROM consumer_lending_risk_lakehouse.bronze.application_train GROUP BY CODE_GENDER
UNION ALL
SELECT 'NAME_CONTRACT_TYPE', NAME_CONTRACT_TYPE, COUNT(*) FROM consumer_lending_risk_lakehouse.bronze.application_train GROUP BY NAME_CONTRACT_TYPE
UNION ALL
SELECT 'NAME_INCOME_TYPE', NAME_INCOME_TYPE, COUNT(*) FROM consumer_lending_risk_lakehouse.bronze.application_train GROUP BY NAME_INCOME_TYPE
UNION ALL
SELECT 'NAME_EDUCATION_TYPE', NAME_EDUCATION_TYPE, COUNT(*) FROM consumer_lending_risk_lakehouse.bronze.application_train GROUP BY NAME_EDUCATION_TYPE
UNION ALL
SELECT 'NAME_FAMILY_STATUS', NAME_FAMILY_STATUS, COUNT(*) FROM consumer_lending_risk_lakehouse.bronze.application_train GROUP BY NAME_FAMILY_STATUS
UNION ALL
SELECT 'NAME_HOUSING_TYPE', NAME_HOUSING_TYPE, COUNT(*) FROM consumer_lending_risk_lakehouse.bronze.application_train GROUP BY NAME_HOUSING_TYPE
ORDER BY column_name, value;
```

- [ ] **Step 2: Verify column references**

Cross-check every column named above against `docs/data_dictionary.md`'s `application_{train|test}` section: `SK_ID_CURR`, `TARGET`, `EXT_SOURCE_1/2/3`, `OCCUPATION_TYPE`, `AMT_ANNUITY`, `AMT_GOODS_PRICE`, `NAME_TYPE_SUITE`, `DAYS_BIRTH`, `DAYS_EMPLOYED`, `AMT_INCOME_TOTAL`, `AMT_CREDIT`, `CNT_CHILDREN`, `CODE_GENDER`, `NAME_CONTRACT_TYPE`, `NAME_INCOME_TYPE`, `NAME_EDUCATION_TYPE`, `NAME_FAMILY_STATUS`, `NAME_HOUSING_TYPE` — all present. Confirm the file has exactly 5 `-- COMMAND ----------` separators (5 cells).

- [ ] **Step 3: Stage the file**

```bash
git add notebooks/silver/application_train.sql
```

---

### Task 2: `application_test.sql`

**Files:**
- Create: `notebooks/silver/application_test.sql`

**Interfaces:**
- Consumes: `consumer_lending_risk_lakehouse.bronze.application_test`
- Produces: nothing consumed by other tasks

- [ ] **Step 1: Write the notebook**

Identical to Task 1's `application_train.sql`, with the table swapped to `application_test` and the `TARGET` class-balance cell removed (the column doesn't exist in this table).

```sql
-- Databricks notebook source
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
```

- [ ] **Step 2: Verify column references**

Same column list as Task 1 minus `TARGET`. Confirm the file has exactly 4 `-- COMMAND ----------` separators (4 cells) and that no `TARGET` reference remains.

- [ ] **Step 3: Stage the file**

```bash
git add notebooks/silver/application_test.sql
```

---

### Task 3: `bureau.sql`

**Files:**
- Create: `notebooks/silver/bureau.sql`

**Interfaces:**
- Consumes: `consumer_lending_risk_lakehouse.bronze.bureau`, `consumer_lending_risk_lakehouse.bronze.application_train`, `consumer_lending_risk_lakehouse.bronze.application_test` (FK check only)
- Produces: nothing consumed by other tasks

- [ ] **Step 1: Write the notebook**

```sql
-- Databricks notebook source
-- Row count and SK_ID_BUREAU uniqueness
SELECT
  COUNT(*) AS row_count,
  COUNT(DISTINCT SK_ID_BUREAU) AS distinct_sk_id_bureau,
  COUNT(*) - COUNT(DISTINCT SK_ID_BUREAU) AS duplicate_sk_id_bureau
FROM consumer_lending_risk_lakehouse.bronze.bureau;

-- COMMAND ----------

-- FK check: SK_ID_CURR must exist in application_train or application_test
SELECT COUNT(*) AS orphan_sk_id_curr_count
FROM consumer_lending_risk_lakehouse.bronze.bureau b
WHERE NOT EXISTS (
    SELECT 1 FROM consumer_lending_risk_lakehouse.bronze.application_train a WHERE a.SK_ID_CURR = b.SK_ID_CURR
  )
  AND NOT EXISTS (
    SELECT 1 FROM consumer_lending_risk_lakehouse.bronze.application_test t WHERE t.SK_ID_CURR = b.SK_ID_CURR
  );

-- COMMAND ----------

-- Null rate on Silver-relevant columns
SELECT
  COUNT(*) AS total_rows,
  ROUND(100.0 * SUM(CASE WHEN AMT_CREDIT_SUM_DEBT IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_amt_credit_sum_debt,
  ROUND(100.0 * SUM(CASE WHEN DAYS_CREDIT_ENDDATE IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_days_credit_enddate,
  ROUND(100.0 * SUM(CASE WHEN AMT_CREDIT_MAX_OVERDUE IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_amt_credit_max_overdue,
  ROUND(100.0 * SUM(CASE WHEN AMT_ANNUITY IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_amt_annuity
FROM consumer_lending_risk_lakehouse.bronze.bureau;

-- COMMAND ----------

-- Range checks
SELECT
  MIN(DAYS_CREDIT) AS min_days_credit,
  MAX(DAYS_CREDIT) AS max_days_credit,
  AVG(DAYS_CREDIT) AS avg_days_credit,
  MIN(CREDIT_DAY_OVERDUE) AS min_credit_day_overdue,
  MAX(CREDIT_DAY_OVERDUE) AS max_credit_day_overdue,
  MIN(AMT_CREDIT_SUM) AS min_amt_credit_sum,
  MAX(AMT_CREDIT_SUM) AS max_amt_credit_sum,
  AVG(AMT_CREDIT_SUM) AS avg_amt_credit_sum
FROM consumer_lending_risk_lakehouse.bronze.bureau;

-- COMMAND ----------

-- Distinct values on key categorical fields
SELECT 'CREDIT_ACTIVE' AS column_name, CREDIT_ACTIVE AS value, COUNT(*) AS row_count FROM consumer_lending_risk_lakehouse.bronze.bureau GROUP BY CREDIT_ACTIVE
UNION ALL
SELECT 'CREDIT_TYPE', CREDIT_TYPE, COUNT(*) FROM consumer_lending_risk_lakehouse.bronze.bureau GROUP BY CREDIT_TYPE
UNION ALL
SELECT 'CREDIT_CURRENCY', CREDIT_CURRENCY, COUNT(*) FROM consumer_lending_risk_lakehouse.bronze.bureau GROUP BY CREDIT_CURRENCY
ORDER BY column_name, value;
```

- [ ] **Step 2: Verify column references**

Cross-check against `docs/data_dictionary.md`'s `bureau` section: `SK_ID_BUREAU`, `SK_ID_CURR`, `AMT_CREDIT_SUM_DEBT`, `DAYS_CREDIT_ENDDATE`, `AMT_CREDIT_MAX_OVERDUE`, `AMT_ANNUITY`, `DAYS_CREDIT`, `CREDIT_DAY_OVERDUE`, `AMT_CREDIT_SUM`, `CREDIT_ACTIVE`, `CREDIT_TYPE`, `CREDIT_CURRENCY` — all present. Confirm `SK_ID_BUREAU` is used (not `SK_BUREAU_ID`), per the Global Constraints note.

- [ ] **Step 3: Stage the file**

```bash
git add notebooks/silver/bureau.sql
```

---

### Task 4: `bureau_balance.sql`

**Files:**
- Create: `notebooks/silver/bureau_balance.sql`

**Interfaces:**
- Consumes: `consumer_lending_risk_lakehouse.bronze.bureau_balance`, `consumer_lending_risk_lakehouse.bronze.bureau` (FK check only)
- Produces: nothing consumed by other tasks

- [ ] **Step 1: Write the notebook**

```sql
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
```

- [ ] **Step 2: Verify column references**

Cross-check against `docs/data_dictionary.md`'s `bureau_balance` section: `SK_ID_BUREAU`, `MONTHS_BALANCE`, `STATUS` — all present. This is the FK check CLAUDE.md's "Data quality" section names explicitly — confirm the `NOT EXISTS` direction is correct (child `bureau_balance` checked against parent `bureau`, not the reverse).

- [ ] **Step 3: Stage the file**

```bash
git add notebooks/silver/bureau_balance.sql
```

---

### Task 5: `previous_application.sql`

**Files:**
- Create: `notebooks/silver/previous_application.sql`

**Interfaces:**
- Consumes: `consumer_lending_risk_lakehouse.bronze.previous_application`, `consumer_lending_risk_lakehouse.bronze.application_train`, `consumer_lending_risk_lakehouse.bronze.application_test` (FK check only)
- Produces: nothing consumed by other tasks

- [ ] **Step 1: Write the notebook**

```sql
-- Databricks notebook source
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
```

- [ ] **Step 2: Verify column references**

Cross-check against `docs/data_dictionary.md`'s `previous_application` section: `SK_ID_PREV`, `SK_ID_CURR`, `RATE_INTEREST_PRIMARY`, `RATE_INTEREST_PRIVILEGED`, `AMT_DOWN_PAYMENT`, `AMT_ANNUITY`, `CNT_PAYMENT`, `AMT_APPLICATION`, `AMT_CREDIT`, `DAYS_DECISION`, `DAYS_FIRST_DRAWING`, `NAME_CONTRACT_STATUS`, `NAME_CONTRACT_TYPE`, `CODE_REJECT_REASON` — all present.

- [ ] **Step 3: Stage the file**

```bash
git add notebooks/silver/previous_application.sql
```

---

### Task 6: `pos_cash_balance.sql`

**Files:**
- Create: `notebooks/silver/pos_cash_balance.sql`

**Interfaces:**
- Consumes: `consumer_lending_risk_lakehouse.bronze.POS_CASH_balance`, `consumer_lending_risk_lakehouse.bronze.previous_application`, `consumer_lending_risk_lakehouse.bronze.application_train`, `consumer_lending_risk_lakehouse.bronze.application_test` (FK checks only)
- Produces: nothing consumed by other tasks

- [ ] **Step 1: Write the notebook**

```sql
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
```

- [ ] **Step 2: Verify column references**

Cross-check against `docs/data_dictionary.md`'s `POS_CASH_balance` section: `SK_ID_PREV`, `SK_ID_CURR`, `MONTHS_BALANCE`, `CNT_INSTALMENT`, `CNT_INSTALMENT_FUTURE`, `SK_DPD`, `SK_DPD_DEF`, `NAME_CONTRACT_STATUS` — all present. Confirm the table name is `POS_CASH_balance` (matches `notebooks/bronze/pos_cash_balance.py`'s `TABLE` constant exactly, not lowercased).

- [ ] **Step 3: Stage the file**

```bash
git add notebooks/silver/pos_cash_balance.sql
```

---

### Task 7: `credit_card_balance.sql`

**Files:**
- Create: `notebooks/silver/credit_card_balance.sql`

**Interfaces:**
- Consumes: `consumer_lending_risk_lakehouse.bronze.credit_card_balance`, `consumer_lending_risk_lakehouse.bronze.previous_application`, `consumer_lending_risk_lakehouse.bronze.application_train`, `consumer_lending_risk_lakehouse.bronze.application_test` (FK checks only)
- Produces: nothing consumed by other tasks

- [ ] **Step 1: Write the notebook**

```sql
-- Databricks notebook source
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
```

- [ ] **Step 2: Verify column references**

Cross-check against `docs/data_dictionary.md`'s `credit_card_balance` section: `SK_ID_PREV`, `SK_ID_CURR`, `MONTHS_BALANCE`, `AMT_DRAWINGS_ATM_CURRENT`, `AMT_DRAWINGS_OTHER_CURRENT`, `AMT_DRAWINGS_POS_CURRENT`, `AMT_PAYMENT_CURRENT`, `AMT_BALANCE`, `SK_DPD`, `NAME_CONTRACT_STATUS` — all present.

- [ ] **Step 3: Stage the file**

```bash
git add notebooks/silver/credit_card_balance.sql
```

---

### Task 8: `installments_payments.sql`

**Files:**
- Create: `notebooks/silver/installments_payments.sql`

**Interfaces:**
- Consumes: `consumer_lending_risk_lakehouse.bronze.installments_payments`, `consumer_lending_risk_lakehouse.bronze.previous_application`, `consumer_lending_risk_lakehouse.bronze.application_train`, `consumer_lending_risk_lakehouse.bronze.application_test` (FK checks only)
- Produces: nothing consumed by other tasks

- [ ] **Step 1: Write the notebook**

```sql
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
```

- [ ] **Step 2: Verify column references**

Cross-check against `docs/data_dictionary.md`'s `installments_payments` section: `SK_ID_PREV`, `SK_ID_CURR`, `AMT_PAYMENT`, `DAYS_ENTRY_PAYMENT`, `NUM_INSTALMENT_NUMBER`, `DAYS_INSTALMENT` — all present. Confirm the last query's `WHERE DAYS_ENTRY_PAYMENT IS NOT NULL` guard is present (avoids the delay computation choking on missed-payment nulls captured in the previous cell).

- [ ] **Step 3: Stage the file**

```bash
git add notebooks/silver/installments_payments.sql
```

---

### Task 9: Commit, push, and open the PR

**Files:** none (integration task only)

**Interfaces:**
- Consumes: all 8 staged files from Tasks 1–8
- Produces: a merged branch and an open PR, following this project's established pattern of one commit per notebook batch (see `c1c9016 feat: land ADF Parquet into Bronze Delta via 8 per-table notebooks` in git history)

- [ ] **Step 1: Confirm all 8 files are staged**

```bash
git status
```

Expected: 8 new files under `notebooks/silver/` listed as staged, nothing else.

- [ ] **Step 2: Commit**

```bash
git commit -m "$(cat <<'EOF'
docs: add Silver-layer profiling notebooks for the 8 Bronze tables

EOF
)"
```

- [ ] **Step 3: Push and open the PR**

```bash
git push -u origin <branch-name>
gh pr create --base develop --title "docs: add Silver-layer profiling notebooks" --body "$(cat <<'EOF'
## Summary
- Adds notebooks/silver/<table>.sql for all 8 Bronze tables: row count/key uniqueness, null rates on Silver-relevant columns, range/plausibility checks (including known Home Credit sentinel values), categorical distinct-value counts, and FK integrity checks
- Implements docs/superpowers/specs/2026-08-07-silver-profiling-notebooks-design.md
- Not executed in this session (no live Databricks connection) - user will run these in Databricks to validate against real data and report back anything to add

## Test plan
- [ ] Run each notebook in Databricks against the live Bronze tables and confirm no syntax errors
- [ ] Spot-check a couple of null-rate/range results against known Home Credit dataset characteristics
EOF
)"
```

Report the PR URL to the user once created.

---

## Self-Review Notes

- **Spec coverage:** every bullet in `docs/superpowers/specs/2026-08-07-silver-profiling-notebooks-design.md`'s "Per-table query plan" section maps to a cell in the corresponding task above — row count/uniqueness, null rate, range/plausibility (incl. sentinels), distinct values, and FK checks are all present for every one of the 8 tables.
- **Placeholder scan:** no TBD/TODO; every SQL cell is complete and runnable as written.
- **Consistency:** table names match `notebooks/bronze/*.py`'s `TABLE` constants exactly (including `POS_CASH_balance` casing); `SK_ID_BUREAU` used consistently (not `SK_BUREAU_ID`) across Tasks 3 and 4; FK-check pattern (`NOT EXISTS` against both `application_train` and `application_test`) is identical across Tasks 3, 5, 6, 7, 8.
