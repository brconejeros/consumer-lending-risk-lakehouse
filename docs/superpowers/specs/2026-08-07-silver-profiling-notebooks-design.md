# Silver-design profiling notebooks

## Purpose

Before writing `01_silver_transform.py`, we need to understand what's actually
in the Bronze tables: null rates, plausible ranges, known dataset quirks
(sentinel values, near-total-null columns), and cross-table referential
integrity. This spec covers a set of per-table Databricks SQL notebooks that
run exploratory/profiling queries against the Bronze layer to surface exactly
that, so Silver-layer decisions (which columns need null handling, which
FK gaps need addressing, which ranges need clamping) are grounded in the real
data rather than assumption.

These notebooks are profiling/reference material, not transformation logic —
they don't write anything, and they stay in the repo permanently as
documentation of what was found, living alongside the eventual Silver-layer
notebooks.

## Structure

New folder: `notebooks/silver/`, containing 8 files — one per source table,
mirroring the existing `notebooks/bronze/` naming (lowercase filenames, e.g.
`pos_cash_balance.sql`), while referencing tables by their real Unity Catalog
name (original CSV casing, e.g. `POS_CASH_balance`), consistent with this
project's naming convention (`CLAUDE.md` → "Conventions"):

```
notebooks/silver/
├── application_train.sql
├── application_test.sql
├── bureau.sql
├── bureau_balance.sql
├── previous_application.sql
├── pos_cash_balance.sql
├── credit_card_balance.sql
└── installments_payments.sql
```

## Format

Each file is a Databricks SQL notebook:

```sql
-- Databricks notebook source
<query 1>

-- COMMAND ----------

<query 2>

-- COMMAND ----------

<query 3>
```

`-- Databricks notebook source` is the real Databricks export marker (matches
`notebooks/bronze/*.py`'s `# Databricks notebook source` header for the
Python notebooks). `-- COMMAND ----------` separates cells so each check runs
and displays independently when opened in Databricks.

All queries reference Bronze tables directly:
`consumer_lending_risk_lakehouse.bronze.<TableName>`.

## Shared query categories

Every table's notebook covers, in order:

1. **Row count + key uniqueness** — total row count, plus a duplicate-key
   check on whatever that table's natural key is (a single ID for
   `application`/`bureau`/`previous_application`, or an ID + time/version
   column for the balance/installment tables).
2. **Null rate on Silver-relevant columns** — not every column, only the ones
   with known high missingness or that feed planned Gold aggregations.
3. **Range/plausibility checks** on business-critical numeric fields,
   including known Home Credit dataset quirks (e.g. the `365243` sentinel
   value used in `DAYS_EMPLOYED` and `DAYS_FIRST_DRAWING` in place of a
   real "days" value).
4. **Distinct-value counts** on key categorical fields, to inform type
   standardization decisions in Silver.
5. **FK integrity check** against the parent/child table, where applicable —
   directly implementing the checks CLAUDE.md's "Data quality" section
   already calls for (e.g. every `SK_ID_BUREAU` in `bureau_balance` must
   exist in `bureau`).

## Per-table query plan

### application_train.sql

- Row count; `SK_ID_CURR` uniqueness (duplicate check)
- `TARGET` class balance (count + percentage per value) — central to the
  business problem, worth surfacing even though this project doesn't train a
  model directly
- Null rate: `EXT_SOURCE_1`, `EXT_SOURCE_2`, `EXT_SOURCE_3`, `OCCUPATION_TYPE`,
  `AMT_ANNUITY`, `AMT_GOODS_PRICE`, `NAME_TYPE_SUITE`
- Range: `DAYS_BIRTH` (converted to age in years), `DAYS_EMPLOYED` (flag rows
  at the `365243` sentinel separately from real values), `AMT_INCOME_TOTAL`,
  `AMT_CREDIT`, `CNT_CHILDREN`
- Distinct values: `CODE_GENDER`, `NAME_CONTRACT_TYPE`, `NAME_INCOME_TYPE`,
  `NAME_EDUCATION_TYPE`, `NAME_FAMILY_STATUS`, `NAME_HOUSING_TYPE`

### application_test.sql

Same as `application_train.sql` minus the `TARGET` class-balance query
(column doesn't exist in this table).

### bureau.sql

- Row count; `SK_ID_BUREAU` uniqueness
- FK check: `SK_ID_CURR` values not present in
  `application_train`/`application_test`
- Null rate: `AMT_CREDIT_SUM_DEBT`, `DAYS_CREDIT_ENDDATE`,
  `AMT_CREDIT_MAX_OVERDUE`, `AMT_ANNUITY`
- Range: `DAYS_CREDIT`, `CREDIT_DAY_OVERDUE`, `AMT_CREDIT_SUM`
- Distinct values: `CREDIT_ACTIVE`, `CREDIT_TYPE`, `CREDIT_CURRENCY`

### bureau_balance.sql

- Row count; uniqueness of (`SK_ID_BUREAU`, `MONTHS_BALANCE`)
- FK check: `SK_ID_BUREAU` values not present in `bureau` (the check CLAUDE.md
  names explicitly)
- Range: `MONTHS_BALANCE`
- Distinct values: `STATUS`

### previous_application.sql

- Row count; `SK_ID_PREV` uniqueness
- FK check: `SK_ID_CURR` values not present in
  `application_train`/`application_test`
- Null rate: `RATE_INTEREST_PRIMARY`, `RATE_INTEREST_PRIVILEGED` (known
  near-total nulls in this dataset), `AMT_DOWN_PAYMENT`, `AMT_ANNUITY`,
  `CNT_PAYMENT`
- Range: `AMT_APPLICATION`, `AMT_CREDIT`, `DAYS_DECISION`; `DAYS_FIRST_DRAWING`
  sentinel check (`365243`), same treatment as `DAYS_EMPLOYED` above
- Distinct values: `NAME_CONTRACT_STATUS`, `NAME_CONTRACT_TYPE`,
  `CODE_REJECT_REASON`

### pos_cash_balance.sql

- Row count; uniqueness of (`SK_ID_PREV`, `MONTHS_BALANCE`)
- FK checks: `SK_ID_PREV` not present in `previous_application`; `SK_ID_CURR`
  not present in `application_train`/`application_test`
- Null rate: `CNT_INSTALMENT`, `CNT_INSTALMENT_FUTURE`
- Range: `SK_DPD`, `SK_DPD_DEF`, `MONTHS_BALANCE`
- Distinct values: `NAME_CONTRACT_STATUS`

### credit_card_balance.sql

- Row count; uniqueness of (`SK_ID_PREV`, `MONTHS_BALANCE`)
- FK checks: `SK_ID_PREV` not present in `previous_application`; `SK_ID_CURR`
  not present in `application_train`/`application_test`
- Null rate: `AMT_DRAWINGS_ATM_CURRENT`, `AMT_DRAWINGS_OTHER_CURRENT`,
  `AMT_DRAWINGS_POS_CURRENT`, `AMT_PAYMENT_CURRENT` (known high missingness)
- Range: `AMT_BALANCE`, `SK_DPD`
- Distinct values: `NAME_CONTRACT_STATUS`

### installments_payments.sql

- Row count
- FK checks: `SK_ID_PREV` not present in `previous_application`; `SK_ID_CURR`
  not present in `application_train`/`application_test`
- Null rate: `AMT_PAYMENT`, `DAYS_ENTRY_PAYMENT` (nulls here represent missed
  installments per the data dictionary — worth quantifying, not treating as a
  data-quality defect)
- Range/derived: `DAYS_INSTALMENT` vs `DAYS_ENTRY_PAYMENT` delta (a
  late-payment signal), `NUM_INSTALMENT_NUMBER`

## Out of scope

- Executing these notebooks — no active Databricks connection in this
  session (Postgres is stopped between sessions per CLAUDE.md, but Bronze
  tables should still be queryable independent of that; this just wasn't
  verified here). Running them is a follow-up once back in Databricks.
- Any transformation/write logic — that's `01_silver_transform.py`'s job,
  not this spec's.
- Exhaustive per-column checks — deliberately scoped to columns that inform
  an actual Silver decision, not every column in every table.
