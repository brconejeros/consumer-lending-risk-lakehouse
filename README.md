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
directly by Databricks. They're first loaded into a local **PostgreSQL** database
(`credit_origination_db`, running in Docker), simulating the transactional
origination system of a credit fintech. From there, **Airbyte** (Postgres source
connector) extracts the data, exactly as it would against a real production
database.

## Architecture

```
CSVs (Kaggle)  →  PostgreSQL (simulated source)  →  Airbyte  →  ADLS Gen2 (Bronze)  →  Databricks/PySpark  →  Delta Lake (Silver/Gold)
```

Medallion architecture (Bronze → Silver → Gold) on Azure Databricks, governed by
Unity Catalog, with data physically stored in ADLS Gen2:

```
Bronze (landing)          Silver (conformed)              Gold (star schema)
─────────────────         ───────────────────              ───────────────────
Parquet files    ─────▶   Null handling,          ─────▶   fact_application
landed by Airbyte,         type standardization,            (1 row / SK_ID_CURR)
loaded as-is into          dedup, referential
Delta tables                integrity checks                dim_bureau
                            across all 8 tables               dim_previous_application
                                                               dim_installments_agg
                                                               dim_credit_card_agg
```

Each Gold dimension is pre-aggregated to `SK_ID_CURR` grain (counts, sums, means,
max delinquency, etc.), so `fact_application` joins to every dimension 1:1 — no
fan-out at query time.

## Stack

- **PostgreSQL** (local, via Docker) — simulated transactional origination source
- **Airbyte** (Open Source, self-hosted via Docker Compose) — Postgres source →
  Azure Blob Storage/ADLS Gen2 destination, landing Parquet
- **Azure Databricks** (Unity Catalog-governed workspace, VNet-injected)
- **ADLS Gen2** for physical storage, accessed via a Unity Catalog external
  location + managed-identity storage credential (no keys/secrets in code)
- **Delta Lake + PySpark** for transformation
- **Databricks SQL Dashboard or Power BI** for the reporting layer

## Repo structure

```
infra/postgres/  → Docker Compose for local Postgres + CSV-load script
infra/airbyte/   → Airbyte connection configs (source/destination), Docker Compose
notebooks/    → numbered pipeline notebooks, run in order
src/          → reusable PySpark logic (schemas, aggregations, quality checks)
tests/        → unit tests for src/
docs/         → architecture diagram, ER diagram for the star schema
CLAUDE.md     → full project/architecture reference
```

## How to run

1. **Source**: `cd infra/postgres && docker compose up -d` — starts PostgreSQL with
   `credit_origination_db`, then run the load script to populate it with the 8
   CSVs as tables *(pending)*.
2. **Ingestion**: Airbyte (self-hosted) syncs Postgres → ADLS Gen2 Bronze
   container as Parquet *(pending)*.
3. Data lands in the `consumer_lending_risk_lakehouse` Unity Catalog catalog,
   under the `bronze`, `silver`, and `gold` schemas.
4. Open `/notebooks` in the Databricks workspace (synced via Git folders from
   this repo) and run in numeric order, attached to a running cluster/warehouse:
   - `00_setup.sql` — creates the bronze/silver/gold schemas
   - `01_bronze_ingestion.py` — loads the Airbyte-landed Parquet files as Delta
     tables *(pending)*
   - `02_silver_transform.py` — cleans types, nulls, dedups, validates FKs
     *(pending)*
   - `03_gold_aggregation.py` — builds the star schema *(pending)*
   - `04_quality_checks.py` — data quality expectations *(pending)*

## Status

- [x] Azure infra provisioned (Databricks workspace, Unity Catalog metastore,
      ADLS Gen2 storage, access connector, VNet)
- [x] Unity Catalog schemas created (`bronze`, `silver`, `gold`)
- [x] PostgreSQL running locally in Docker (`credit_origination_db`)
- [ ] 8 CSVs loaded into Postgres as tables
- [ ] Airbyte set up and syncing Postgres → ADLS Gen2
- [ ] Bronze ingestion (Parquet → Delta)
- [ ] Silver transformation + referential integrity checks
- [ ] Gold star schema
- [ ] Data quality checks
- [ ] Dashboard (Power BI / Databricks SQL) with 3+ visualizations
- [ ] Architecture + ER diagrams in `/docs`

## Completion criteria

- PostgreSQL running locally with the 8 tables loaded, simulating the
  transactional source.
- Airbyte configured and syncing successfully from Postgres to ADLS Gen2.
- Pipeline runs end-to-end (bronze → gold) from a single orchestrated notebook.
- Star schema documented with an ER diagram.
- Dashboard published with at least 3 visualizations answering the business
  problem (risk distribution by segment, default rate by income/age band,
  drill-down by individual application).
- This README kept current with problem statement, full architecture (including
  the ingestion layer), and how to run.
