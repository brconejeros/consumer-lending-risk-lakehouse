# consumer-lending-risk-lakehouse

End-to-end lakehouse pipeline for consumer credit risk scoring — built on Azure
Databricks + Delta Lake with PySpark, modeling 8 relational tables into a star
schema to predict loan default probability for thin-file borrowers.

## Business problem

For a credit applicant without a robust bank history, what is the probability of
default? This project reproduces the kind of multi-table, multi-grain credit data
(applications, bureau history, prior loans, installments, credit card behavior)
seen in real consumer lending systems, using a public dataset as a stand-in.

## Dataset

[Home Credit Default Risk](https://www.kaggle.com/competitions/home-credit-default-risk)
(Kaggle) — 8 related CSV tables, ~700MB, joined via `SK_ID_CURR`, `SK_ID_BUREAU`,
and `SK_ID_PREV`:

- `application_train` / `application_test`
- `bureau`
- `bureau_balance`
- `previous_application`
- `POS_CASH_balance`
- `installments_payments`
- `credit_card_balance`

## Simulated source system

To make the ingestion story realistic — and demonstrate relational-database
extraction, a core skill for senior data engineering roles — the CSVs are not read
directly by Databricks. They're first loaded into **Azure Database for PostgreSQL –
Flexible Server** (`credit_origination_db`), simulating the transactional
origination system of a credit fintech. From there, **Azure Data Factory** (Copy
Activity, Postgres source) extracts the data, exactly as it would against a real
production database.

## Architecture

```
CSVs (Kaggle)  →  PostgreSQL (simulated source)  →  Azure Data Factory (Parquet landing in ADLS Gen2)  →  Databricks notebooks (Bronze Delta)  →  PySpark (Silver/Gold)
```

Medallion architecture (Bronze → Silver → Gold) on Azure Databricks, governed by
Unity Catalog, with data physically stored in ADLS Gen2:

```
Bronze (landing)          Silver (conformed)              Gold (star schema)
─────────────────         ───────────────────              ───────────────────
Parquet landed   ─────▶   Null handling,          ─────▶   fact_application
by ADF, loaded             type standardization,            (1 row / SK_ID_CURR)
into Delta by one          dedup, referential
notebook per table          integrity checks                dim_bureau
                            across all 8 tables               dim_previous_application
                                                               dim_installments_agg
                                                               dim_credit_card_agg
```

Each Gold dimension is pre-aggregated to `SK_ID_CURR` grain (counts, sums, means,
max delinquency, etc.), so `fact_application` joins to every dimension 1:1 — no
fan-out at query time.

## Stack

- **Azure Database for PostgreSQL – Flexible Server** — simulated transactional
  origination source
- **Azure Data Factory** — Postgres source → ADLS Gen2 Parquet landing, one
  pipeline covering all 8 tables. Fully managed, billed per pipeline run
- **Azure Databricks** (Unity Catalog-governed workspace)
- **ADLS Gen2** for physical storage, accessed via Unity Catalog external
  locations + managed-identity storage credentials (no keys/secrets in code) —
  one storage account for the ADF landing zone, a separate one for the
  metastore's own managed storage
- **Delta Lake + PySpark** for transformation
- **Databricks SQL Dashboard or Power BI** for the reporting layer

## Repo structure

```
infra/terraform/  → Terraform: Postgres Flexible Server, Data Factory, ADLS Gen2 landing storage
infra/postgres/   → CSV-load script (loads the 8 CSVs into Postgres as tables)
notebooks/        → numbered pipeline notebooks, run in order
  00_setup.sql          → Unity Catalog schema creation
  bronze/<table>.py     → one notebook per table, instantiates BronzeIngestionJob
  01_silver_transform.py, 02_gold_aggregation.py, 03_quality_checks.py
src/lakehouse/    → LakehouseLayerJob class hierarchy shared across Bronze/Silver/Gold
tests/        → unit tests for src/, run locally with pytest (no cluster needed)
docs/         → architecture diagram, ER diagram for the star schema
CLAUDE.md     → full project/architecture reference
```

## How to run

1. **Source**: `cd infra/terraform/platform && ./toggle.sh start`, then run
   `infra/postgres/load_csvs.py` to populate `credit_origination_db` with the 8
   CSVs as tables.
2. **Ingestion**: trigger the Data Factory pipeline
   (`copy_postgres_to_landing`) — lands all 8 tables as Parquet in the ADLS
   Gen2 landing storage account.
3. **Bronze**: run the `bronze_ingestion` Databricks Job — loads the
   landed Parquet into `consumer_lending_risk_lakehouse.bronze` as Delta
   tables, one notebook per table.
4. Open `/notebooks` in the Databricks workspace and run the rest in numeric
   order, attached to a running cluster/warehouse:
   - `01_silver_transform.py` — cleans types, nulls, dedups, validates FKs
     *(pending)*
   - `02_gold_aggregation.py` — builds the star schema *(pending)*
   - `03_quality_checks.py` — data quality expectations *(pending)*

## Status

- [x] Azure infra provisioned (Databricks workspace, Unity Catalog metastore,
      ADLS Gen2 storage, access connectors)
- [x] Unity Catalog schemas created (`bronze`, `silver`, `gold`)
- [x] PostgreSQL running on Azure with all 8 tables loaded
- [x] Azure Data Factory pipeline built and verified — lands all 8 tables as
      Parquet in ADLS Gen2
- [x] Bronze ingestion (Parquet → Delta) — all 8 tables loaded and verified
      against known row counts
- [ ] Silver transformation + referential integrity checks
- [ ] Gold star schema
- [ ] Data quality checks
- [ ] Dashboard (Power BI / Databricks SQL) with 3+ visualizations
- [ ] Architecture + ER diagrams in `/docs`

## Completion criteria

- PostgreSQL running on Azure with the 8 tables loaded, simulating the
  transactional source.
- Azure Data Factory landing all 8 tables as Parquet in ADLS Gen2, and
  Databricks notebooks loading them into Bronze as Delta tables — **done**.
- Pipeline runs end-to-end (bronze → gold) from a single orchestrated notebook.
- Star schema documented with an ER diagram.
- Dashboard published with at least 3 visualizations answering the business
  problem (risk distribution by segment, default rate by income/age band,
  drill-down by individual application).
- This README kept current with problem statement, full architecture (including
  the ingestion layer), and how to run.
