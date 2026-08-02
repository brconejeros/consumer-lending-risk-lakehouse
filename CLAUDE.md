# consumer-lending-risk-lakehouse

## Business problem

For a credit applicant without robust bank history, what is the probability of default?
Dataset: Home Credit Default Risk (Kaggle) — 8 related CSV tables, ~700MB, joined via
`SK_ID_CURR`, `SK_ID_BUREAU`, `SK_ID_PREV`.

This project mirrors real credit-bureau-style data (multiple tables, different grains,
foreign keys) — a public-dataset stand-in for CERC-style consumer credit data.

## Simulated source system

To make the ingestion story realistic (and demonstrate relational-database extraction,
a core skill for senior data engineering roles), the 8 CSVs are **not** read directly
by Databricks. They're first loaded into a local **PostgreSQL** instance (via Docker),
simulating the transactional origination system of a credit fintech — the kind of
system this data would actually come from. From there, extraction happens via
**Airbyte** (Postgres source connector), exactly as it would against a real production
database.

## Full pipeline flow

```
CSVs (Kaggle)  →  PostgreSQL (simulated source)  →  Airbyte  →  ADLS Gen2 (Bronze)  →  Databricks/PySpark  →  Delta Lake (Silver/Gold)
```

## Stack

- **PostgreSQL** (local, via Docker) — simulated transactional origination source
- **Airbyte** (Open Source, self-hosted via Docker Compose) — Postgres source →
  Azure Blob Storage/ADLS Gen2 destination connector, landing Parquet
- Azure Databricks (Unity Catalog-governed) + ADLS Gen2
- Delta Lake + PySpark
- Star schema modeling
- Power BI or Databricks SQL Dashboard for the presentation layer

## Architecture: Source → Ingestion → Bronze → Silver → Gold

**Source (simulated)** — the 8 CSVs loaded as relational tables in PostgreSQL
(database `credit_origination_db`): `application_train`, `application_test`,
`bureau`, `bureau_balance`, `previous_application`, `POS_CASH_balance`,
`installments_payments`, `credit_card_balance`. Represents the real transactional
system this data would normally come from.

**Ingestion (Airbyte)** — Airbyte connection: Postgres source → Azure Blob
Storage/ADLS Gen2 destination. Each sync extracts the 8 tables and lands them as
Parquet files in the Bronze container.

**Bronze (landing zone)** — the Parquet files Airbyte landed, loaded as Delta tables
with no additional transformation — a mirror of the Postgres tables.

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

- `/infra/postgres` — Docker Compose for local Postgres, plus the load script that
  creates `credit_origination_db` and loads the 8 CSVs as tables
- `/infra/airbyte` — Airbyte connection configs (source/destination definitions),
  Docker Compose for self-hosted Airbyte
- `/notebooks` — PySpark notebooks, run in order: `00_setup.sql`,
  `01_bronze_ingestion.py`, `02_silver_transform.py`, `03_gold_aggregation.py`,
  `04_quality_checks.py`
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

- PostgreSQL running locally with the 8 tables loaded, simulating the transactional
  source.
- Airbyte configured and syncing successfully from Postgres to ADLS Gen2.
- Pipeline runs end-to-end (bronze → gold) from a single command/orchestrated notebook.
- Star schema documented with an ER diagram.
- Power BI dashboard published with at least 3 visualizations answering the business
  problem (risk distribution by segment, default rate by income/age band, drill-down
  by individual application).
- README with problem statement, full architecture (including the ingestion layer),
  and how to run.

## Status

Unity Catalog is fully wired up (metastore, storage credential, external location,
catalog `consumer_lending_risk_lakehouse` with `bronze`/`silver`/`gold` schemas
created). The 8 CSVs were uploaded directly to a Volume
(`bronze.raw_files`) as a bypassed intermediate step — under the new architecture,
the official Bronze source will be Parquet files landed by Airbyte, not that direct
upload. Next: stand up Postgres via Docker, load the CSVs as tables, then set up
Airbyte. Estimated 2-3 weeks at 5-8h/week (may run longer given the added ingestion
layer).
