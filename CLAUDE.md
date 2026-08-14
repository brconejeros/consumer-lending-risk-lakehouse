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
by Databricks. They're first loaded into **Azure Database for PostgreSQL – Flexible
Server**, simulating the transactional origination system of a credit fintech — the
kind of system this data would actually come from. From there, extraction happens via
**Azure Data Factory** (Copy Activity, Postgres source), exactly as it would against a
real production database.

Postgres originally ran locally via Docker; it moved to Azure because:
- **Local resource constraints** — the dev machine has limited disk/memory.
- **More realistic architecture** — a real fintech's origination database runs in the
  cloud, not on an engineer's laptop.
- **Cost** — Postgres Flexible Server (Burstable B1ms, 32 GB storage + 32 GB backup)
  fits inside the Azure free account's 12-month free tier, provided it's within 12
  months of the free account's creation date and usage stays under the monthly hour
  caps. ADF itself is billed per pipeline run/activity, not per hour, and this
  project's batch-load usage costs cents per run.

Ingestion was originally built on self-hosted Airbyte (via `abctl` on a dedicated Azure
VM) instead of ADF — abandoned after persistent sync failures on tables larger than
~50K rows that resisted every attempted fix. See "Status" for a short summary and git
history on the `bugfix/*`/`feature/adf-*` branches for the full story.

## Full pipeline flow

```
CSVs (Kaggle)  →  PostgreSQL (simulated source)  →  Azure Data Factory (Parquet landing in ADLS Gen2)  →  Databricks notebooks (Bronze Delta tables)  →  PySpark (Silver/Gold)
```

ADF's Copy Activity reads each of the 8 Postgres tables and lands them as Parquet in a
dedicated ADLS Gen2 storage account (`landing` filesystem, one folder per table). A
Databricks notebook per table then reads that Parquet and writes it into Unity
Catalog's `bronze` schema as a Delta table (full overwrite each run - see "Architecture"
and "Repo layout").

## Stack

- **Terraform** (`azurerm` provider) — provisions the Postgres Flexible Server, the
  Data Factory instance, and the ADLS Gen2 landing storage account as code
- **Azure Database for PostgreSQL – Flexible Server** (Burstable B1ms, free-tier
  eligible) — simulated transactional origination source
- **Azure Data Factory** — one pipeline (`ForEach` + parameterized Copy Activity,
  sequential) reads each of the 8 Postgres tables and lands them as Parquet in ADLS
  Gen2. Fully managed/serverless - billed per pipeline run, not per VM-hour
- Azure Databricks (Unity Catalog-governed) + ADLS Gen2 (both for the landing zone and
  the metastore's own managed storage, kept as separate storage accounts)
- Delta Lake + PySpark
- Star schema modeling
- Power BI or Databricks SQL Dashboard for the presentation layer

## Infrastructure as Code (Terraform)

Azure resources (Postgres Flexible Server, Data Factory, ADLS Gen2 landing storage) are
provisioned with Terraform (`azurerm` provider) instead of clicked together manually —
reproducible, versioned in git, and easy to rebuild from scratch.

- **State backend**: remote state in an Azure Storage Account (Blob container),
  not a local `.tfstate` file — keeps state off the disk-constrained dev machine
  and fits the "always free" Blob Storage tier (5 GB LRS).
- **Free-tier guardrails**: the Postgres SKU (Burstable B1ms) is pinned as a
  variable default to stay free-tier.
- **Two-stage apply**:
  1. `infra/terraform/platform` — the Postgres Flexible Server. Firewall rules:
     the dev machine's admin IP (for `load_csvs.py`/psql), plus Azure's documented
     "allow access from Azure services" sentinel rule (`0.0.0.0`/`0.0.0.0`), since
     ADF's default Azure Integration Runtime has no fixed outbound IP to scope a
     rule to.
  2. `infra/terraform/data-factory` — the Data Factory instance, its Postgres and
     ADLS Gen2 linked services, the landing storage account, the Copy pipeline, and
     a Databricks access connector (for the notebook side's Unity Catalog external
     location - see "Architecture"). Reads `postgres_fqdn` from stage 1's remote
     state. Simpler dependency than the old Airbyte setup: no bootstrap
     chicken-and-egg, just a plain `terraform_remote_state` read.
- **ADF-specific gotchas worth remembering**:
  - Use `azurerm_data_factory_linked_custom_service` (type `PostgreSqlV2`) for the
    Postgres linked service, not the typed `azurerm_data_factory_linked_service_postgresql`
    resource — that resource has a confirmed open provider bug
    (hashicorp/terraform-provider-azurerm#22890) that silently ignores SSL
    parameters and always connects with `EncryptionMethod=0`, which a Flexible
    Server (SSL-enforced by default) rejects.
  - Hyphens in ADF object names (datasets/linked services/pipelines) break the
    platform's own internal runtime-variable composition for parameterized Copy
    activities (`InvalidTemplate ... character 'p' at position 45 is not
    expected`) — use underscores instead.
  - `azurerm_data_factory_linked_custom_service` can silently drop fields (e.g.
    `authenticationType`) from `type_properties_json` on `apply`, with `terraform
    plan` showing no drift afterward — if a required property error persists
    despite it being present in config, check the live resource via `az rest`
    against the ADF REST API directly; a direct `PUT` may be needed to actually
    persist it.
  - `azurerm_data_factory_dataset_parquet` sends `compressionCodec` as an empty
    string if `compression_codec` isn't set explicitly, which the Parquet sink
    rejects outright rather than treating as "no compression" — set it explicitly
    (e.g. `"snappy"`).
- **Secrets**: Postgres admin password etc. passed via `TF_VAR_*` env vars or a
  gitignored `terraform.tfvars` — never committed, same pattern as the existing
  `.env` files.
- **On/off**: `terraform apply`/`destroy` control whether resources *exist*. For
  day-to-day toggling, `infra/terraform/platform/toggle.sh` wraps `az postgres
  flexible-server stop/start` — there's no VM to toggle anymore, since ADF is
  fully managed and Data Factory itself isn't billed per-hour.

## Architecture: Source → Ingestion → Bronze → Silver → Gold

**Source (simulated)** — the 8 CSVs loaded as relational tables in Azure Database
for PostgreSQL – Flexible Server (database `credit_origination_db`):
`application_train`, `application_test`, `bureau`, `bureau_balance`,
`previous_application`, `POS_CASH_balance`, `installments_payments`,
`credit_card_balance`. Represents the real transactional system this data would
normally come from.

**Ingestion (Azure Data Factory)** — one pipeline, one `ForEach` activity
(sequential) wrapping a parameterized Copy Activity: reads each of the 8 Postgres
tables and writes them as Parquet into a dedicated ADLS Gen2 storage account's
`landing` filesystem, one folder per table (`landing/<table>/part-0000.parquet`).

**Bronze (landing zone)** — a Databricks notebook per table
(`notebooks/bronze/<table>.py`) reads that table's Parquet folder and writes it
into Unity Catalog's `bronze` schema as a Delta table via `saveAsTable(...,
mode="overwrite")` — a full overwrite each run, not incremental/merge, since
there's no CDC/incremental state to track (see "Future enhancements"). A
Databricks Job (`bronze_ingestion`) runs `00_setup.sql` first, then all 8
per-table notebooks in parallel (independent of each other, only depending on
setup).

The read/write logic itself lives in one class, `BronzeIngestionJob`
(`src/lakehouse/bronze.py`), not copy-pasted across the 8 notebooks — see
"OOP ingestion framework (`src/lakehouse`)" below for why that's not the same
thing as the shared-loop abstraction this project deliberately avoided.

**Silver (cleaned + conformed)** — null handling, type standardization (dates,
categoricals), de-duplication, referential integrity checks across tables (e.g. every
`SK_ID_BUREAU` in `bureau_balance` must exist in `bureau`; every `SK_ID_PREV` in the
satellite tables must exist in `previous_application`).

**Gold (star schema)** — grain is 1 row per `SK_ID_CURR`:
- `fact_application` — one row per applicant, unioning `tb_application_train`
  and `tb_application_test` (`SampleTypeCd` = `TRAIN`/`TEST`, `Target`
  nullable since test rows have none) so every applicant - labeled or not -
  joins to every dimension the same way
- `dim_bureau` — aggregated bureau history per applicant
- `dim_previous_application` — aggregated prior Home Credit applications per applicant
- `dim_installments_agg` — aggregated installment payment behavior per applicant
- `dim_credit_card_agg` — aggregated credit card balance/behavior per applicant

Each dimension is pre-aggregated to `SK_ID_CURR` grain (count, sum, mean, max of
delinquency/payment fields) so `fact_application` joins to each dimension 1:1.
Count/sum aggregates are `0` (not `NULL`) for an applicant with no rows in a
given satellite table; avg/min/max stay `NULL` in that case, since there's
nothing to average.

## OOP ingestion framework (`src/lakehouse`)

The medallion layers share one template-method class hierarchy instead of
each notebook hand-rolling its own read/transform/write sequence — the
project's OOP showcase, and the reusable shape Silver/Gold will plug into
next:

- `LakehouseLayerJob` (`src/lakehouse/base.py`) — abstract base class (`abc.ABC`).
  Defines `run()` as the fixed orchestration: `extract() -> transform() ->
  validate() -> load()`. `extract`/`load` are `@abstractmethod`; `transform`/
  `validate` default to no-ops so a layer that doesn't need them yet (Bronze,
  today) can skip them without boilerplate overrides.
- `BronzeIngestionJob(LakehouseLayerJob)` (`src/lakehouse/bronze.py`) —
  implements `extract` (read the ADF landing-zone Parquet) and `load` (Delta
  `saveAsTable(..., mode="overwrite")` into `bronze`), paired with a
  `BronzeTableConfig` frozen dataclass that derives `landing_path` and
  `target_table` from just a table name.
- This resolves the tension with "Deliberately 8 separate files" below: the
  **notebooks stay 8 separate files** (so the Databricks Job still gets 8
  independent, parallel task boundaries) — they just each instantiate the
  same `BronzeIngestionJob` with their own `BronzeTableConfig(table=...)`
  instead of repeating the read/write cell. What's shared is the *class*, not
  a loop driving all 8 tables from one script.
- Silver/Gold will get their own `LakehouseLayerJob` subclasses
  (`SilverTransformJob`, `GoldAggregationJob`, ...) overriding `transform`/
  `validate` for null handling, dedup, FK checks, and star-schema
  aggregation — same base class, same common libraries (`pyspark`, stdlib
  `dataclasses`/`abc`/`logging`), no new dependency per layer.
- Covered by `tests/unit/test_base.py` (local `pyspark` + `delta-spark`, no
  live cluster needed — see "Working locally" for the version pin this
  requires) and `tests/integration/test_bronze.py` (Databricks Connect
  against the real workspace — see "Repo layout" for why Bronze moved off
  local unit tests).

## Silver naming convention

Applies from Silver onward only — Bronze keeps the source dataset's raw
casing (see "Conventions" below), since Bronze's whole point is
traceability back to the raw CSVs/Postgres tables.

- **Columns** — PascalCase with a trailing type suffix reflecting what the
  value represents, not just its literal name:

  | Suffix | Meaning | Example |
  |---|---|---|
  | `Id` | surrogate key | `SK_ID_CURR` → `CurrId` |
  | `Amt` | monetary amount | `AMT_INCOME_TOTAL` → `IncomeTotalAmt` |
  | `Cd` | coded/categorical value | `CODE_GENDER` → `GenderCd` |
  | `Cnt` | count | `CNT_CHILDREN` → `ChildrenCnt` |
  | `Flg` | 0/1 boolean | `FLAG_OWN_CAR` → `OwnCarFlg` |
  | `Days` | relative day-count delta | `DAYS_BIRTH` → `BirthDays` |
  | `Avg`/`Mode`/`Medi` | normalized housing stat | `APARTMENTS_AVG` → `ApartmentsAvg` |

  `src/lakehouse/naming.py`'s `to_silver_column_name()` applies the
  mechanical prefix-strip rules above automatically. Anything that doesn't
  fit a rule (e.g. `EXT_SOURCE_n` normalized scores, which should get a
  `Score` suffix) goes into that table's `SilverTableConfig.column_overrides`
  once the table is actually wired up, rather than being guessed at
  automatically.
- **`Desc` columns** — opt-in, not mechanical. Where a `Cd` column's codes
  have a genuinely documented, non-obvious meaning (e.g. `bureau_balance`'s
  `StatusCd`: `0`-`5`/`C`/`X` DPD buckets), `SilverTableConfig.code_descriptions`
  adds a `<Column>Desc` column decoding it into text, alongside the coded
  column rather than replacing it. Most coded/categorical columns are
  already human-readable text (`CreditActiveCd`, `ContractStatusCd`, ...)
  and don't need this — only add it where the raw codes are genuinely
  opaque without a lookup.
- **Tables** — `tb_<snake_case_name>`, all lowercase, via
  `to_silver_table_name()` in the same module (e.g. `POS_CASH_balance` →
  `tb_pos_cash_balance`).

## Repo layout

- `/infra/terraform` — Terraform config provisioning the Postgres Flexible
  Server and the Data Factory/landing storage; see "Infrastructure as Code" for
  the module breakdown
- `/infra/postgres` — the load script that creates `credit_origination_db` and
  loads the 8 CSVs as tables (the server itself is Terraform-provisioned)
- `/notebooks` — PySpark notebooks:
  - `00_setup.sql` — Unity Catalog schema creation
  - `bronze/<table>.py` × 8 — one notebook per table; each is a thin wrapper
    that instantiates `BronzeIngestionJob` with that table's
    `BronzeTableConfig` and calls `.run()` (see "OOP ingestion framework").
    Deliberately 8 separate files rather than one script looping over all 8
    tables — explicit, separate task boundaries per table in the Databricks
    Job. The read/write logic itself is no longer duplicated across them;
    only the per-table config line differs
  - `silver/<table>.py` × 8 — same pattern as `bronze/`: a thin wrapper
    instantiating `SilverTableConfig` with that table's `column_overrides`/
    `dedup_keys`/`fk_checks` and calling `SilverTransformJob(...).run()`.
    `fk_checks` reads Bronze rather than Silver (see "Data quality"), so
    every table's job is independent - no run-order dependency between
    these 8 files, same as Bronze's parallel task boundaries
  - `gold/<output>.py` × 5 (`fact_application.py`, `dim_bureau.py`,
    `dim_previous_application.py`, `dim_installments_agg.py`,
    `dim_credit_card_agg.py`) — same thin-wrapper pattern as `bronze/`/
    `silver/`: each instantiates a `GoldTableConfig` and calls its job
    class's `.run()`. All 5 outputs are independent (none reads another
    Gold table, only Silver), so this follows the established
    one-file-per-output convention rather than one monolithic script -
    an earlier draft of this doc assumed Gold would "stay single
    notebooks" before the design confirmed the outputs don't depend on
    each other
  - `03_quality_checks.py` — not yet built
  - `silver/profiling/<table>.py` × 8 — same one-file-per-table pattern as
    `bronze/`/`silver/`, not a pipeline stage. Each is exploratory: what
    that Bronze table is, its grain, business relevance, key predictive
    columns, and data-quality quirks, informing that table's
    `silver/<table>.py` `column_overrides`/`fk_checks` choices. Shared
    helpers (`null_rate`, `fk_orphan_count`) live in
    `src/lakehouse/profiling.py` rather than being duplicated across the 8
    files
- `/src/lakehouse` — the `LakehouseLayerJob` class hierarchy shared across
  medallion layers — see "OOP ingestion framework". `base.py`/`bronze.py`
  (Bronze), `naming.py` (Silver-onward column/table naming rules — pure
  Python, no Spark import, see "Silver naming convention"), `silver.py`
  (`SilverTransformJob`/`SilverTableConfig`/`FkCheck`, wired up per-table in
  `notebooks/silver/<table>.py`), `profiling.py` (`null_rate`/
  `fk_orphan_count`, shared by `notebooks/silver/profiling/<table>.py`),
  `gold.py` (`GoldAggregationJob` shared base + `GoldTableConfig`, with
  one small subclass per Gold output - `FactApplicationJob`/
  `DimBureauJob`/`DimPreviousApplicationJob`/`DimInstallmentsAggJob`/
  `DimCreditCardAggJob` - wired up per-output in `notebooks/gold/
  <output>.py`), `session.py` (Databricks Connect serverless session
  factory, used only by `tests/integration`)
- `/tests/unit` — tests against a local `pyspark` + `delta-spark` session
  (no Databricks cluster required), run via `uv run pytest tests/unit`.
  Covers `base.py`/`naming.py`/`silver.py`/`profiling.py`/`gold.py`; Bronze
  has no unit tests (see `/tests/integration` below)
- `/tests/integration` — tests against a real **serverless** Databricks
  cluster over Databricks Connect (`src/lakehouse/session.py`); run via the
  separate `.venv-dbconnect` env, not the default one — see "Working
  locally". `test_bronze.py` is Bronze's only test coverage, deliberately
  integration-only rather than split like Silver's local-unit +
  read-only-integration pattern: the team has budget to test against real
  infrastructure "for everything," and Bronze's `load()` is
  `mode="overwrite"`, so exercising it for real (including a full `run()`
  against the real `application_test` table) is just re-running the same
  idempotent operation the `bronze_ingestion` Databricks Job already
  performs — not a risky action. The idempotency/schema-change cases that
  used to point `landing_path_override` at a local `tmp_path` now write
  synthetic Parquet to a Unity Catalog Volume scratch path instead (the
  remote serverless cluster can't see the local filesystem, and this
  workspace has the public DBFS root disabled - `dbfs:/tmp/...` fails with
  `DbfsDisabledException`, confirmed by hitting it - so a UC Volume is the
  correct governed equivalent, not DBFS) and target a scratch table name,
  cleaned up via the Databricks SDK's `WorkspaceClient().files` (recursive
  delete, since the API's own `delete_directory` refuses non-empty
  directories) after each test
- `/docs` — architecture diagram, ER diagram for the star schema, design notes
- `README.md` — problem statement, architecture summary, how to run

## Data quality

Validate with Delta Live Tables Expectations or Great Expectations:
- `SK_ID_CURR` uniqueness in `fact_application`
- plausible ranges for age/income fields
- non-null checks on critical fields — in Silver today, scoped to each
  table's grain columns (`SilverTableConfig.dedup_keys` doubles as the
  null-check list, since a row with a null grain key isn't a meaningful row
  either — see `src/lakehouse/silver.py`). `SilverTableConfig.sentinel_nulls`
  additionally nulls out known non-null "this value doesn't apply" sentinels
  per column (e.g. `application_{train,test}`'s `DAYS_EMPLOYED` uses `365243`
  for "not currently employed", affecting ~18%/~19% of rows respectively —
  found via `notebooks/silver/profiling/application_train.py`) without
  dropping the row. General per-column imputation strategy beyond known
  sentinels is still Gold-layer feature engineering, not implemented here
- foreign key integrity between Bronze tables before promoting to Silver —
  `FkCheck.ref_table` in `SilverTableConfig` points at the *Bronze* parent
  table specifically (not a Silver one), so every table's
  `SilverTransformJob` can run independently, in any order. **Not a hard
  failure**: verified against the real dataset that a non-trivial fraction
  of rows in `bureau_balance` (~11%), `POS_CASH_balance` (~3%),
  `credit_card_balance` (~28%), and `installments_payments` (~9%) reference
  a parent key that genuinely doesn't exist in Bronze — a real
  characteristic of the Home Credit dataset, not a bug. `transform()` drops
  those rows and logs a warning instead of raising, so a table's Silver
  load isn't blocked by data the pipeline can't fix upstream

## Git commit conventions

- Do not add any AI/Claude attribution to commits — no `Co-Authored-By: Claude` (or
  similar) trailer, no mention of Claude/AI assistance in commit messages. Commits
  should read as authored solely by the repo owner.
- This also applies to the git author/committer identity itself, not just the
  message text — every commit (and any PR opened from one) must show as
  authored by **Bruno Conejeros
  `<53882404+brconejeros@users.noreply.github.com>`** (the GitHub noreply
  address already used across this repo's history), never the tool's default
  `Claude <noreply@anthropic.com>` identity. Set it per-commit, e.g.
  `git -c user.name="Bruno Conejeros" -c user.email="53882404+brconejeros@users.noreply.github.com" commit -m "..."` —
  do not touch global git config to do this.
- The same rule applies to infrastructure: no "claude" or other AI references in
  Azure resource names, resource group names, tags, or SSH key comments.
- Commit messages use Conventional Commits prefixes: `feat:`, `fix:`, `chore:`,
  `docs:`.
- Never put an actual secret value (password, key, token, connection string)
  in CLAUDE.md or any other tracked file - only point at where it lives (a
  gitignored path, e.g. the list in "Working locally"). Before writing to a
  tracked file or staging changes, check whether anything in it should be a
  secret instead and route it into a `.env`/`.tfvars` file if so.

## Branching conventions

- Always branch from an up-to-date `develop` — never commit directly to
  `develop` or `main`.
- Before creating a new branch, check for existing branches other than
  `develop`/`main` (`git branch -a`) — reuse or clean up in-progress work
  instead of duplicating it.
- Branch name prefixes:
  - `feature/` — new functionality (e.g. `feature/unity-catalog-setup`)
  - `bugfix/` — bug fixes
  - `hotfix/` — urgent fixes branched from `main` directly, bypassing `develop`
    (the only branches, besides `develop`, that `enforce-branch-flow.yml`
    allows to PR into `main`)
  - `chore/` — non-feature maintenance: dependency bumps, tooling, CI config
  - `docs/` — README/CLAUDE.md-only changes
- Merge feature/bugfix/chore/docs branches into `develop` via PR; `develop`
  (or a `hotfix/*` branch) is later PR'd into `main`.
- As soon as a branch is merged, delete it — both locally and on the remote
  (`git branch -d <branch>` and `git push origin --delete <branch>`) — so
  only `develop`/`main` and active work remain in `git branch -a`.

## Conventions

- Notebooks are numbered and run top-to-bottom; the full pipeline (bronze → gold)
  should run from a single orchestrated entry point (the Databricks Job).
- Keep transformation logic testable: prefer classes/functions in `/src` over
  inline notebook cells when logic is reused across notebooks. This now
  includes the 8 per-table Bronze and Silver notebooks too — they call the
  shared `BronzeIngestionJob`/`SilverTransformJob` classes rather than
  duplicating read/write/transform code, but stay as 8 separate notebook
  *files* each rather than being collapsed into one script that loops over
  all 8 tables (see "OOP ingestion framework" and "Repo layout").
- Table/column naming stays in the source dataset's original casing
  (e.g. `SK_ID_CURR`, `AMT_INCOME_TOTAL`) for traceability back to the raw CSVs.

## Working locally

Operational details for resuming work in a fresh session - not architecture,
just "how do I actually run the next command."

- **Tools live in `~/.local/bin`, not on PATH by default** - on the Linux/WSL
  dev environment this was originally written from, `uv`, `terraform`, `gh`,
  `az`, `databricks` were all installed there (no sudo on that machine).
  Every fresh shell needs `export PATH="$HOME/.local/bin:$PATH"` first. **A
  Windows machine is a different story** - confirmed on one that only had
  `claude.exe`/`gh.exe`/Python launchers there, with `terraform`/`az`/
  `databricks` missing entirely. Check with `which <tool>` rather than
  assuming either environment; install what's missing (e.g. on Windows,
  `winget install Databricks.DatabricksCLI` for the Databricks CLI).
- **Real secrets already exist on disk, gitignored** - don't ask "what's the
  password," read the file:
  - `infra/terraform/.env` - `ARM_CLIENT_ID`/`ARM_CLIENT_SECRET`/
    `ARM_TENANT_ID`/`ARM_SUBSCRIPTION_ID` for the Terraform service principal,
    sourced before every `terraform` command in both stages.
  - `infra/terraform/platform/terraform.tfvars` - Postgres admin password,
    admin IP.
  - `infra/terraform/data-factory/terraform.tfvars` - landing storage account
    name, Postgres admin password (must match platform's).
  - `~/.databrickscfg` - may already have unrelated profiles from other
    projects/workspaces (confirmed once: a `DEFAULT`/named profile pointing
    at a completely different GCP-hosted workspace) - **don't assume an
    existing profile is this project's.** This project's real Azure
    Databricks workspace is `https://adb-7405619456327656.16.azuredatabricks.net`;
    the profile authenticated against it is named `azure`, set up via
    `databricks auth login --host https://adb-7405619456327656.16.azuredatabricks.net
    --profile azure` (browser OAuth, not a PAT). If that profile is missing
    on a fresh machine, that command recreates it - it just needs the
    `databricks` CLI installed first (see above) and the user to complete
    the browser sign-in themselves.
- **`az`/`gh` auth were set up interactively** (browser device-code flows) -
  if a fresh session hits auth errors from either, that's expected; these
  can't be restarted programmatically. Ask the user to re-run `az login` /
  `gh auth login`.
- **A freshly-installed CLI may not resolve by bare name even after
  install** - on Windows, a package manager (e.g. `winget`) can register a
  PATH entry in the registry without the currently-running terminal process
  picking it up - confirmed: opening a *new tab/window* in the same
  already-running terminal app still used the stale PATH, because the host
  app cached its environment at its own launch time, not the tab's. Closing
  and fully restarting the terminal *application* (not just its windows)
  fixed it; the installed binary's full path always works as a fallback
  in the meantime (e.g. via `winget list --id <pkg>` to locate it).
- **`pyspark` is pinned to `==3.5.3`, not just `<3.6`** - newer 3.5.x patch
  releases (verified broken: 3.5.9) fail every `delta-spark==3.2.1`
  `saveAsTable(mode="overwrite")` locally with `AnalysisException: Table ...
  does not support truncate in batch mode`, even for a brand-new table,
  regardless of 2-part vs 3-part table name or `spark.sql.catalogImplementation`.
  Doesn't affect the real pipeline (Databricks Runtime's own Delta/Unity
  Catalog integration doesn't hit this), only local `pytest` runs against
  `tests/unit/conftest.py`'s local Spark+Delta session - if `uv add`/`uv sync`
  ever bumps `pyspark` past `3.5.3`, re-pin it rather than debugging the
  symptom.
- **Databricks Connect integration tests need a separate venv** -
  `databricks-connect` and plain `pyspark` fight at Spark-context init time
  if both are importable in the same environment, so `tests/integration`
  never shares an env with `tests/unit`. Run
  `./scripts/setup_dbconnect_env.sh` once (creates `.venv-dbconnect`,
  installs `databricks-connect`/`pytest`/`python-dotenv` - doesn't touch
  `pyproject.toml`/`uv.lock`), then
  `.venv-dbconnect/bin/pytest tests/integration` (`.venv-dbconnect/Scripts/pytest.exe`
  on Windows) for the serverless-cluster tests, vs. `uv run pytest
  tests/unit` for the local ones. Auth: `tests/integration/conftest.py`
  reads the profile name from the `DATABRICKS_CONFIG_PROFILE` env var
  (falls back to the SDK's own default-profile resolution if unset) rather
  than a hardcoded name - set it to `azure` (see the `~/.databrickscfg`
  bullet above for what that profile is and how to recreate it if it's
  missing).
- **`databricks-connect` is pinned to `==18.3` on Python 3.12** (both set in
  `scripts/setup_dbconnect_env.sh`), not latest/whatever Python `uv venv`
  defaults to - verified broken: `19.0.0` on Python 3.13 fails every
  serverless session with `INVALID_PARAMETER_VALUE.INVALID_CLIENT_IMAGE_VERSION`,
  because it requests a client image version newer than this Azure
  workspace's serverless compute supports. Databricks' own compatibility
  table (linked in the script) lists `18.0`-`18.3` as the current top
  serverless-compatible bracket, requiring Python 3.12 - if `tests/
  integration` ever starts failing the same way again, re-check that table
  and re-pin both together rather than just bumping the package.

**Resuming work, in order:**
1. `cd infra/terraform/platform && ./toggle.sh start` - takes a few minutes
   for Postgres to actually come up.
2. `terraform plan` in both `platform` and `data-factory` to check for drift
   before assuming everything's still intact.
3. Trigger the ADF pipeline (`az rest ... /pipelines/copy_postgres_to_landing/createRun`)
   and the Databricks Job (`databricks jobs run-now <job_id>`) as needed - both
   are manually triggered, no schedule, since Postgres is stopped between
   sessions.

## Completion criteria

- PostgreSQL running on Azure (Flexible Server, free-tier) with the 8 tables loaded,
  simulating the transactional source.
- Azure Data Factory pipeline landing all 8 tables as Parquet in ADLS Gen2, and
  Databricks notebooks loading them into Unity Catalog's `bronze` schema as Delta
  tables - **done**, verified end-to-end (see "Status").
- Pipeline runs end-to-end (bronze → gold) from a single command/orchestrated notebook.
- Star schema documented with an ER diagram.
- Power BI dashboard published with at least 3 visualizations answering the business
  problem (risk distribution by segment, default rate by income/age band, drill-down
  by individual application).
- README with problem statement, full architecture (including the ingestion layer),
  and how to run.

## Future enhancements

Not part of the current build - revisit once the pipeline works end-to-end:

- **CDC from Postgres to Databricks** — true Change Data Capture (logical
  replication slot + publication on Postgres) instead of ADF's current full
  Copy-Activity pull each run. More realistic for a live origination system,
  and a stronger signal of the CDC skill for senior data engineering roles.
  ADF supports CDC via a dedicated Mapping Data Flow/CDC capability; would
  also need the Bronze notebooks to switch from full-overwrite to
  incremental-append/merge.
- **Simulate ongoing originations** — a small script that periodically
  inserts/updates rows in Postgres (new applications, updated
  `previous_application` rows, etc.). The CSVs are a one-time historical
  load, so CDC has nothing to capture without this - without ongoing writes,
  CDC is functionally identical to a one-time snapshot.

## Status

**Airbyte was tried first and abandoned.** Self-hosted Airbyte (via `abctl` on a
dedicated Azure VM) got fully working end-to-end - webapp, Postgres source,
Databricks Lakehouse destination connector all functional, after resolving several
real infrastructure bugs (a `kind`/Kubernetes pod-networking NAT issue, an `abctl`
Helm chart pinning issue, a missing stream-selection config). It was ultimately
abandoned because sync jobs reliably OOM-killed on any table larger than ~50K rows,
and three separate attempts to raise the effective container memory limit
(a Terraform actor-level override, the platform's Kubernetes ConfigMap, restarting
the components that read it) all failed to change what the actual sync pod launched
with — the real lever was never found. Only `application_test` (the smallest table)
ever synced successfully. Full troubleshooting detail lives in the git history of
the now-merged `bugfix/kind-pod-cidr-route`, `bugfix/airbyte-connection-stream-config`,
and `feature/adf-terraform-module`/`feature/bronze-ingestion-notebook` branches, not
repeated here.

**Current architecture (Azure Data Factory) is built and verified working
end-to-end.** `infra/terraform/platform` provisions Postgres; `infra/terraform/data-factory`
provisions the Data Factory instance, the ADLS Gen2 landing storage account
(`streditorigination01`), the Copy pipeline, and a Databricks access connector.
A manually-created Unity Catalog storage credential + external location
(matching this project's existing manual-Databricks-setup pattern - no
`databricks` Terraform provider in this repo) lets the Bronze notebooks read the
landing zone. The ingestion service principal (`sp-airbyte-bronze-ingestion` -
attempts to rename it away from that stale name failed silently via the
Databricks API, likely because it's Azure AD-backed and its display name is
controlled there) was re-granted for the new role: dropped `CREATE_VOLUME`
(Airbyte-only), added `READ_FILES` on the new external location.

Verified end-to-end: the ADF pipeline landed all 8 tables as Parquet in ~9
minutes (including the ~13.6M-row `installments_payments` table Airbyte never
got remotely close to), and the `bronze_ingestion` Databricks Job (9 tasks - one
per table, plus `00_setup` - all succeeding in parallel) loaded all 8 into
`consumer_lending_risk_lakehouse.bronze` with row counts matching the known
Home Credit dataset sizes exactly (e.g. `bureau_balance` = 27,299,925,
`installments_payments` = 13,605,401).

The Databricks Job currently runs as the creating user rather than the service
principal - setting `run_as` to a service principal needs the account-level
"Account Access Control Proxy" API, which needs account-admin auth not
configured in this session; not worth the setup for a portfolio project where
the SP's actual security-relevant role (governing data access via Unity Catalog
grants) is unaffected either way.

Both Postgres and the Data Factory/storage are left as Terraform manages them;
Postgres is stopped between sessions (`toggle.sh stop`) to avoid cost - there's
no VM to stop anymore, since ADF is fully managed.

The Bronze notebooks were refactored onto the `LakehouseLayerJob`/
`BronzeIngestionJob` class hierarchy in `src/lakehouse` (see "OOP ingestion
framework") - same read-Parquet/write-Delta behavior, now behind a tested,
reusable class instead of duplicated inline code, and the template method
(`extract`/`transform`/`validate`/`load`) Silver and Gold subclass too.

The Silver layer's framework was built next: `src/lakehouse/naming.py`
(PascalCase+suffix column naming, `tb_` table naming - pure Python, no
Spark, see "Silver naming convention"), `src/lakehouse/silver.py`
(`SilverTransformJob`/`SilverTableConfig`/`FkCheck` - table-agnostic
extract/transform/validate/load, FK checks via left-anti join rather than
a subquery, which a prior reverted Silver attempt hit a Photon bug on), and
`src/lakehouse/session.py` (a Databricks Connect serverless session
factory). Tests split into `tests/unit` (local `pyspark`+`delta-spark`,
unchanged behavior) and `tests/integration` (Databricks Connect against a
real serverless cluster, via the separate `.venv-dbconnect` env from
`scripts/setup_dbconnect_env.sh` - see "Working locally").

All 8 tables are now wired into `SilverTransformJob`, one per-table
notebook each under `notebooks/silver/` (mirroring `notebooks/bronze/`'s
pattern - 8 separate files/Databricks-Job tasks rather than one script
looping over all 8), informed by the 8 per-table `notebooks/
silver/profiling/<table>.py` notebooks' findings (see "Silver naming
convention" for the two real naming-rule misses they surfaced:
`AMT_REQ_CREDIT_BUREAU_*` are enquiry counts despite the `AMT_` prefix, and
`NFLAG_*` doesn't match the `FLAG_` prefix rule). `column_overrides` are
targeted, not exhaustive - only added where the mechanical rule in
`naming.py` is wrong or simply doesn't fire for an obviously coded/flagged
column; plain unprefixed descriptive columns fall back to plain PascalCase
rather than getting a suffix for its own sake.

`tests/integration` now runs clean against this project's actual Azure
Databricks workspace over Databricks Connect serverless compute (see
"Working locally" for the `~/.databrickscfg` profile/version-pin setup this
took) - confirms the real serverless session connects and that
`SilverTransformJob`'s rename logic produces correct Silver column names
against the real `application_train` Bronze table.

A read-only dry run of all 8 `SilverTableConfig`s against live Bronze data
(extract + transform, no `load()`) surfaced a real design gap: `FkCheck`
originally raised and blocked the whole table on any orphaned row, but the
real dataset has a genuine, non-trivial orphan rate (`bureau_balance` ~11%,
`credit_card_balance` ~28%, `POS_CASH_balance` ~3%,
`installments_payments` ~9%) - not a bug, just how the source data is.
Fixed by moving the FK check into `transform()` as a filter-and-warn step
(see "Data quality") instead of a hard failure in `validate()`
(`SilverTransformJob` no longer overrides `validate()` at all). Verified
against live data again after the fix: all 8 tables' `extract()`/
`transform()` now complete cleanly with the expected rows dropped and
logged.

Running all 8 `notebooks/silver/profiling/<table>.py` notebooks for real
(not just the FK spot checks) surfaced one more real issue:
`application_{train,test}`'s `DAYS_EMPLOYED` sentinel value (`365243`,
~18%/~19% of rows) was passed through untouched. Fixed via a new
`SilverTableConfig.sentinel_nulls` field - `transform()` nulls out
configured sentinel values per column without dropping the row. Verified
against live data: 0 remaining sentinel rows post-transform, row counts
unchanged.

All 8 `notebooks/silver/*.py` notebooks have now been run for real against
the live workspace - `consumer_lending_risk_lakehouse.silver` has all 8
`tb_*` tables, verified by querying them directly afterward (row counts
match the dry run exactly: `tb_bureau_balance` 24,179,741 rows after
dropping the 3,120,184 orphans, `tb_application_train` 307,511 with 122
correctly-renamed columns, etc.). Silver is done end-to-end.

A pass checking whether any Silver `Cd` column's codes had a documented
but non-obvious meaning worth decoding found exactly one real case:
`bureau_balance.StatusCd` (`0`-`5`/`C`/`X` DPD buckets, per
`docs/data_dictionary.md` - itself fixed to spell out all 8 codes instead
of truncating with "..."). Added via a new `SilverTableConfig.
code_descriptions` field (see "Silver naming convention") - `StatusDesc`
sits alongside `StatusCd`, not replacing it. Every other coded/categorical
Silver column checked was already human-readable text
(`CreditActiveCd`/`ContractStatusCd`/etc.) or had no documented per-value
meaning to decode (`CreditCurrencyCd`, `RejectReasonCd`) - a separate pass
also checked all Silver string columns for typos/casing/whitespace
inconsistencies directly against live data and found none.

Bronze's test coverage moved from `tests/unit/test_bronze.py` (local
`pyspark`+`delta-spark`, synthetic data) to `tests/integration/test_bronze.py`
(Databricks Connect, real workspace) - now that Databricks Connect access to
the real serverless cluster exists and the team has budget to test against
real infrastructure, there's no reason to keep validating Bronze against a
synthetic local session instead of the real ADLS landing zone and real
Unity Catalog `bronze` schema it actually reads/writes. Verified against
live data: `extract()` against the real `application_test` landing Parquet
and a full `run()` into `consumer_lending_risk_lakehouse.bronze.application_test`
both return/land exactly 48,744 rows, matching the known real count.
Idempotency and schema-change cases run against a scratch Unity Catalog
Volume path/table (never the real 8) rather than DBFS - this workspace has
the public DBFS root disabled, confirmed by actually hitting
`DbfsDisabledException` on a first attempt - cleaned up via the Databricks
SDK's Files API after each test.

The Gold star schema is built and live: `src/lakehouse/gold.py`
(`GoldAggregationJob` shared base + 5 small subclasses, one per output -
see "Repo layout"), wired up one per notebook under `notebooks/gold/
<output>.py`. `dim_bureau` and `dim_previous_application` each do a
2-step rollup (`bureau_balance` → per-`BureauId` signal → per-`CurrId`;
`POS_CASH_balance` → per-`PrevId` signal → per-`CurrId`) since their
history tables sit a grain below the parent they roll into.
`fact_application` unions `tb_application_train`/`tb_application_test`
with a nullable `Target` and `SampleTypeCd`, per "Architecture".

Verified against live data before and after writing: dry-run
`extract()`/`transform()` row counts matched hand predictions exactly
(`fact_application` = 307,511 + 48,744 = 356,255), and a spot-check of one
real applicant's `dim_bureau` row (8 raw `bureau` rows, 3 active/5 closed,
`CreditSumAmt` summing to 1,285,239.06) matched the aggregate exactly. All
5 notebooks then run for real: `consumer_lending_risk_lakehouse.gold` now
has `fact_application` (356,255 rows), `dim_bureau` (305,811),
`dim_previous_application` (338,857), `dim_installments_agg` (336,935),
and `dim_credit_card_agg` (92,447) - row counts match the dry run exactly.

The feature set per dimension is a reasonable first pass (counts, sums,
means, max delinquency per "Architecture"), not exhaustive feature
engineering - real modeling work will likely want more later.

Next: `03_quality_checks.py` (data quality expectations), then the ER
diagram and Power BI dashboard per "Completion criteria". Estimated 2-3
weeks at 5-8h/week (already running longer given the ingestion-layer
detour and rebuild).
