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

- **Terraform** (`azurerm` provider) — provisions the Postgres Flexible Server,
  Data Factory instance, and ADLS Gen2 landing storage as code, with remote
  state in an Azure Storage Account
- **Azure Database for PostgreSQL – Flexible Server** — simulated transactional
  origination source
- **Azure Data Factory** — Postgres source → ADLS Gen2 Parquet landing, one
  pipeline covering all 8 tables (parallel copy, up to 4 tables at once).
  Fully managed, billed per pipeline run
- **Azure Databricks** (Unity Catalog-governed workspace)
- **ADLS Gen2** for physical storage, accessed via Unity Catalog external
  locations + managed-identity storage credentials (no keys/secrets in code) —
  one storage account for the ADF landing zone, a separate one for the
  metastore's own managed storage
- **Delta Lake + PySpark** for transformation
- **Databricks Asset Bundle** — job-as-code orchestration: 14 independent
  Databricks Jobs (8 per-table Bronze→Silver pipelines, 5 Gold aggregations,
  1 quality-check job) chained by data-dependency (Table Update) triggers,
  not a fixed schedule
- **Great Expectations** — Gold-layer data quality checks (uniqueness,
  plausible-range validation), audited to a Delta table on every passing run
- **Databricks SQL (Lakeview) AI/BI Dashboard** for the reporting layer

## Results

Verified against the live `gold.fact_application` table (307,511 labeled
applications): **8.07% overall default rate**. Segment- and cohort-level
patterns hold up on inspection:

- **By income type** — the four largest segments (`Working`, `Commercial
  associate`, `State servant`, `Pensioner`, covering >99% of applicants) sit
  in a believable 5.4%–9.6% range. `Maternity leave`/`Unemployed` show
  higher rates but on tiny samples (n=5/n=22) — not statistically reliable
  on their own.
- **By income × age band** — default rate falls steadily with both higher
  income and older age, forming a clean gradient from ~13% (youngest,
  lowest-income) down to <2% (oldest, highest-income).
- **Coverage caveat** — only ~26% of applicants have any Home Credit credit
  card history, ~86% have bureau history, so the `dim_*` tables are
  intentionally not 1:1 with `fact_application` (see
  [`docs/er_diagram.md`](docs/er_diagram.md)).

These are cohort-level, historical default rates, not a per-applicant
predicted probability — see [CLAUDE.md](CLAUDE.md) "Future enhancements" for
what a real scoring model on top of this star schema would take.

## Repo structure

```
databricks.yml    → Databricks Asset Bundle: job-as-code for the 14 orchestration Jobs
resources/jobs/   → one YAML file per job (pipeline_<table>, gold_<output>, quality_checks, setup)
infra/terraform/  → Terraform: Postgres Flexible Server, Data Factory, ADLS Gen2 landing storage
infra/postgres/   → CSV-load script (loads the 8 CSVs into Postgres as tables)
notebooks/        → pipeline notebooks, run in order
  00_setup.sql            → Unity Catalog schema creation
  bronze/<table>.py       → one notebook per table, instantiates BronzeIngestionJob
  silver/<table>.py       → one notebook per table, instantiates SilverTransformJob
  silver/profiling/<table>.py → one notebook per table, exploratory (not a pipeline stage)
  gold/<output>.py       → one notebook per output, instantiates a GoldAggregationJob subclass
  03_quality_checks.py
src/lakehouse/    → LakehouseLayerJob class hierarchy shared across Bronze/Silver/Gold
tests/unit/        → local pyspark+delta-spark tests, no cluster needed
tests/integration/ → Databricks Connect tests against a real serverless cluster
docs/         → data_dictionary.md, er_diagram.md (Gold star schema ER diagram)
dashboards/   → consumer_lending_risk_dashboard.lvdash.json - the published
                Databricks SQL (Lakeview) AI/BI dashboard definition, importable
                via `databricks lakeview create/update --json @...`
CLAUDE.md     → full project/architecture reference
```

## How to run

**Prerequisites**: an Azure subscription, an Azure Databricks workspace with
Unity Catalog, and the `terraform`/`az`/`databricks` CLIs. Real infra, not a
local sandbox — see [CLAUDE.md](CLAUDE.md) "Working locally" for the full
tool/auth setup.

1. **Deploy the Databricks Jobs** (one-time, or after changing a notebook/
   job definition): `databricks bundle deploy --profile azure` from the
   repo root — deploys the 14 orchestration Jobs + `setup` defined in
   `databricks.yml`/`resources/jobs/*.yml`. First time only, run the
   `setup` job once (`databricks jobs run-now <job_id> --profile azure`)
   to create the Unity Catalog schemas.
2. **Start Postgres**: `cd infra/terraform/platform && ./toggle.sh start`,
   then run `infra/postgres/load_csvs.py` to populate
   `credit_origination_db` with the 8 CSVs as tables.
3. **Kick off the pipeline**: `./infra/terraform/data-factory/trigger_pipeline.sh`
   — triggers the Data Factory pipeline (`copy_postgres_to_landing`),
   landing all 8 tables as Parquet in the ADLS Gen2 landing storage
   account, then waits for it and explicitly triggers each table's
   `pipeline_<table>` Job (Bronze → Silver) itself. From there the rest
   cascades on its own: each `gold_<output>` Job fires via a Table Update
   trigger once the Silver tables it needs have committed; `quality_checks`
   fires once `gold.fact_application` is written. See CLAUDE.md
   "Architecture" → "Orchestration" for the full dependency graph and why
   the first hop is explicit rather than trigger-driven.
4. **Watch it run**: Databricks Jobs UI (each of the 14 jobs' run history),
   or Unity Catalog's table lineage graph in Catalog Explorer (open
   `gold.fact_application` → Lineage) for the whole chain in one view.

**Tests**: `uv run pytest tests/unit` (local `pyspark`+`delta-spark`, no
cluster needed). `tests/integration` needs a separate env against a real
serverless cluster — see [CLAUDE.md](CLAUDE.md) "Working locally".

## Status

- [x] Azure infra provisioned (Databricks workspace, Unity Catalog metastore,
      ADLS Gen2 storage, access connectors)
- [x] Unity Catalog schemas created (`bronze`, `silver`, `gold`, `quality`)
- [x] PostgreSQL running on Azure with all 8 tables loaded
- [x] Azure Data Factory pipeline built and verified — lands all 8 tables as
      Parquet in ADLS Gen2
- [x] Bronze ingestion (Parquet → Delta) — all 8 tables loaded and verified
      against known row counts
- [x] Silver transformation + referential integrity checks — all 8 tables
      loaded and verified against real Bronze data
- [x] Gold star schema — `fact_application` + 4 dimensions loaded and
      verified against real Silver data
- [x] Data quality checks (`03_quality_checks.py`, Great Expectations) —
      run live twice, all 3 expectations passing against real
      `fact_application` data
- [x] Orchestration — 14-job Databricks Asset Bundle (8 `pipeline_<table>`
      + 5 `gold_<output>` + `quality_checks`), run end-to-end live twice,
      confirmed correct (see CLAUDE.md "Status" for details, including the
      File Arrival trigger fix and ADF parallelization)
- [x] Gold star schema ER diagram (`docs/er_diagram.md`)
- [x] Databricks SQL (Lakeview) dashboard published with 5 visualizations —
      2 counters, 2 charts, 1 drill-down table (`dashboards/
      consumer_lending_risk_dashboard.lvdash.json`)

## Completion criteria

- PostgreSQL running on Azure with the 8 tables loaded, simulating the
  transactional source.
- Azure Data Factory landing all 8 tables as Parquet in ADLS Gen2, and
  Databricks notebooks loading them into Bronze as Delta tables — **done**.
- Pipeline runs end-to-end (bronze → gold → quality) via the 14-job
  Databricks Asset Bundle, chained by data-dependency triggers — **done**,
  verified live twice (see CLAUDE.md "Status").
- Star schema documented with an ER diagram — **done** (`docs/er_diagram.md`).
- Dashboard published with at least 3 visualizations answering the business
  problem (risk distribution by segment, default rate by income/age band,
  drill-down by individual application) — **done**, Databricks SQL (Lakeview)
  dashboard "Consumer Lending Risk Dashboard", published live against real
  `gold` data (see CLAUDE.md "Status" for build detail).
- This README kept current with problem statement, full architecture (including
  the ingestion layer), and how to run.

## License

[MIT](LICENSE)

## Development notes

Built with the assistance of [Claude Code](https://claude.com/claude-code),
Anthropic's AI coding assistant — used across infrastructure, pipeline, and
dashboard development, with every architectural decision, debugging step, and
verification run directed and reviewed against real Azure/Databricks
infrastructure rather than taken on faith. `CLAUDE.md` in this repo is the
project's working reference for that process.
