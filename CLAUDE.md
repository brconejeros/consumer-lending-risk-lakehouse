# consumer-lending-risk-lakehouse

## Business problem

For a credit applicant without robust bank history, what is the probability of default?
Dataset: Home Credit Default Risk (Kaggle) — 8 related CSV tables, ~700MB, joined via
`SK_ID_CURR`, `SK_ID_BUREAU`, `SK_ID_PREV`.

This project mirrors real credit-bureau-style data (multiple tables, different grains,
foreign keys) — a public-dataset stand-in for CERC-style consumer credit data.

## Stack

- Databricks (Community Edition) + Azure ADLS Gen2 (or DBFS as local stand-in)
- Delta Lake + PySpark
- Star schema modeling
- Power BI or Databricks SQL Dashboard for the presentation layer

## Architecture: Bronze → Silver → Gold

**Bronze (landing zone)** — the 8 raw CSVs loaded as Delta tables, no transformation:
`application_train`, `application_test`, `bureau`, `bureau_balance`,
`previous_application`, `POS_CASH_balance`, `installments_payments`,
`credit_card_balance`.

**Silver (cleaned + conformed)** — null handling, type standardization (dates,
categoricals), de-duplication, referential integrity checks across tables (e.g. every
`SK_ID_BUREAU` in `bureau_balance` must exist in `bureau`; every `SK_ID_PREV` in the
satellite tables must exist in `previous_application`).

**Gold (star schema)** — grain is 1 row per `SK_ID_CURR`:
- `fact_application` — one row per applicant
- `dim_bureau` — aggregated bureau history per applicant
- `dim_previous_application` — aggregated prior Home Credit applications per applicant
- `dim_installments_agg` — aggregated installment payment behavior per applicant
- `dim_credit_card_agg` — aggregated credit card balance/behavior per applicant

Each dimension is pre-aggregated to `SK_ID_CURR` grain (count, sum, mean, max of
delinquency/payment fields) so `fact_application` joins to each dimension 1:1.

## Repo layout

- `/notebooks` — PySpark notebooks, run in order: `01_bronze_ingestion.py`,
  `02_silver_transform.py`, `03_gold_aggregation.py`, `04_quality_checks.py`
- `/src` — shared PySpark logic (schema definitions, aggregation functions, quality
  check helpers) factored out of the notebooks
- `/docs` — architecture diagram, ER diagram for the star schema, design notes
- `README.md` — problem statement, architecture summary, how to run

## Data quality

Validate with Delta Live Tables Expectations or Great Expectations:
- `SK_ID_CURR` uniqueness in `fact_application`
- plausible ranges for age/income fields
- non-null checks on critical fields
- foreign key integrity between Bronze tables before promoting to Silver

## Git commit conventions

- Do not add any AI/Claude attribution to commits — no `Co-Authored-By: Claude` (or
  similar) trailer, no mention of Claude/AI assistance in commit messages. Commits
  should read as authored solely by the repo owner.

## Conventions

- Notebooks are numbered and run top-to-bottom; the full pipeline (bronze → gold)
  should run from a single orchestrated entry point.
- Keep transformation logic testable: prefer functions in `/src` over inline notebook
  cells when logic is reused across notebooks.
- Table/column naming stays in the source dataset's original casing
  (e.g. `SK_ID_CURR`, `AMT_INCOME_TOTAL`) for traceability back to the raw CSVs.

## Completion criteria

- Pipeline runs end-to-end (bronze → gold) from a single command/orchestrated notebook.
- Star schema documented with an ER diagram.
- Power BI dashboard published with at least 3 visualizations answering the business
  problem (risk distribution by segment, default rate by income/age band, drill-down
  by individual application).
- README with problem statement, architecture, and how to run.

## Status

Project scaffold only — no data ingested yet, no notebooks written. Estimated 2-3
weeks at 5-8h/week.
