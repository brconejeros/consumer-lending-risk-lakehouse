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
**Airbyte** (Postgres source connector, self-hosted on a small Azure VM), exactly as it
would against a real production database.

Postgres and Airbyte originally ran locally via Docker; both moved to Azure because:
- **Local resource constraints** — the dev machine has limited disk/memory, and
  running Postgres plus the full Airbyte stack (webapp, server, worker, temporal, db)
  alongside Databricks work was too heavy.
- **More realistic architecture** — a real fintech's origination database and ELT
  tooling run in the cloud, not on an engineer's laptop; hosting them on Azure
  strengthens the "production-like source system" story rather than weakening it.
- **Cost** — both fit inside the Azure free account's 12-month free tier: Postgres
  Flexible Server (Burstable B1ms, 32 GB storage + 32 GB backup) and one burstable VM
  (B1s / B2pts v2 / B2ats v2, 750 hours/month) for Airbyte. Staying within these SKUs
  and a single instance of each keeps the ingestion layer at $0, provided it's within
  12 months of the free account's creation date and usage stays under the monthly
  hour caps.

## Full pipeline flow

```
CSVs (Kaggle)  →  PostgreSQL (simulated source)  →  Airbyte  →  Databricks Unity Catalog (Bronze Delta tables)  →  PySpark (Silver/Gold)
```

Airbyte writes directly into Bronze as Delta tables via its Databricks Lakehouse
destination connector - no intermediate Parquet/Blob landing step, no separate
`01_bronze_ingestion.py` load (see "Status" for why the original ADLS Gen2/Blob
Storage plan changed).

## Stack

- **Terraform** (`azurerm` provider, plus the Airbyte provider for connector
  config) — provisions the Postgres Flexible Server, networking, and the
  Airbyte VM as code
- **Azure Database for PostgreSQL – Flexible Server** (Burstable B1ms, free-tier
  eligible) — simulated transactional origination source
- **Airbyte** (Open Source, self-hosted via `abctl` on a burstable Azure VM —
  B1s / B2pts v2 / B2ats v2, free-tier eligible) — Postgres source → Databricks
  Lakehouse destination connector, writing directly into Unity Catalog's
  `bronze` schema as Delta tables (no intermediate Blob/Parquet landing).
  `abctl` deploys Airbyte via a local `kind` Kubernetes cluster running on
  Docker; Docker Compose deployment was deprecated by Airbyte in August 2024
- Azure Databricks (Unity Catalog-governed) + ADLS Gen2
- Delta Lake + PySpark
- Star schema modeling
- Power BI or Databricks SQL Dashboard for the presentation layer

## Infrastructure as Code (Terraform)

Azure resources (networking, the Postgres Flexible Server, and the Airbyte VM)
are provisioned with Terraform (`azurerm` provider) instead of clicked together
manually — reproducible, versioned in git, and easy to rebuild from scratch.

- **State backend**: remote state in an Azure Storage Account (Blob container),
  not a local `.tfstate` file — keeps state off the disk-constrained dev machine
  and fits the "always free" Blob Storage tier (5 GB LRS).
- **Free-tier guardrails**: the Postgres SKU (Burstable B1ms) is pinned as a
  variable default to stay free-tier. The VM SKU is **not** free-tier - all
  three free-tier burstable sizes (B1s/B2pts_v2/B2ats_v2) cap at 1GB RAM,
  which isn't enough for `abctl`'s Kubernetes control plane (see "Status" for
  what this actually took to discover). Running `Standard_D2as_v7` (2 vCPU/
  8GB) instead, at real cost.
- **Postgres and the VM live in different regions** (`location` vs.
  `vm_location` variables) - they only talk over Postgres's public endpoint,
  no VNet peering, so there's no need to co-locate them. This split exists
  because this subscription (a Free Trial) has its own per-region,
  per-SKU allow-list - a size/region combo that fails isn't necessarily a
  transient capacity issue. Check what's actually allowed before assuming a
  size will work:
  `az rest --method get --url "https://management.azure.com/subscriptions/<id>/providers/Microsoft.Compute/skus?api-version=2021-07-01&\$filter=location eq '<region>'"`
  and look for VM SKUs with an empty `restrictions` array (`az vm list-skus`
  works too but is far slower in practice).
- **Two-stage apply**:
  1. `infra/terraform/platform` — VNet/subnet/NSG, the Postgres Flexible Server,
     and the Airbyte VM. Cloud-init on the VM installs Docker (and the Compose
     plugin, unused by Airbyte itself but handy for ad-hoc container work);
     `infra/airbyte/install.sh` then installs `abctl` and runs
     `abctl local install --low-resource-mode --chart-version 1.8.5
     --insecure-cookies`, which stands up Airbyte via a `kind` Kubernetes
     cluster on port 8000. The chart version and cookie flag aren't
     optional - see "Status" for why.
  2. `infra/terraform/airbyte-config` — once Airbyte is reachable at the VM's
     IP (stage 1 output), uses the official Airbyte Terraform provider to
     declare the Postgres source connector, Databricks Lakehouse destination
     connector, and the connection between them as code instead of manual UI
     clicks. Kept as a separate stage/state because it depends on stage 1
     already being applied and Airbyte being up. The provider's
     `client_id`/`client_secret`/`token_url` auth doesn't work against this
     self-hosted instance (a 401 - the OAuth2 flow it sends doesn't match
     what this instance's custom token endpoint needs); use `bearer_auth`
     instead with a token from `get_token.sh`, run fresh (~15 min expiry)
     immediately before every plan/apply.
- **Secrets**: Postgres admin password, VM SSH key, etc. passed via `TF_VAR_*`
  env vars or a gitignored `terraform.tfvars` — never committed, same pattern
  as the existing `.env` files.
- **On/off**: `terraform apply`/`destroy` control whether resources *exist*,
  but destroying and recreating the VM re-triggers the whole Airbyte install.
  For day-to-day toggling without losing that setup, a small script
  (`infra/terraform/toggle.sh`) wraps `az postgres flexible-server stop/start`
  and `az vm deallocate/start` against the resources Terraform created.

## Architecture: Source → Ingestion → Bronze → Silver → Gold

**Source (simulated)** — the 8 CSVs loaded as relational tables in Azure Database
for PostgreSQL – Flexible Server (database `credit_origination_db`):
`application_train`, `application_test`, `bureau`, `bureau_balance`,
`previous_application`, `POS_CASH_balance`, `installments_payments`,
`credit_card_balance`. Represents the real transactional system this data would
normally come from.

**Ingestion (Airbyte)** — Airbyte (self-hosted on a small Azure VM) connection:
Postgres source → Databricks Lakehouse destination. Each sync extracts the 8
tables and writes them directly as Delta tables in the `bronze` schema - no
intermediate file landing step.

**Bronze (landing zone)** — the 8 tables as Airbyte wrote them, with no
additional transformation — a mirror of the Postgres tables, materialized
directly as Delta tables by the destination connector.

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

- `/infra/terraform` — Terraform config provisioning the Postgres Flexible
  Server, networking, the Airbyte VM, and Airbyte's connector config; see
  "Infrastructure as Code" for the module breakdown
- `/infra/postgres` — the load script that creates `credit_origination_db` and
  loads the 8 CSVs as tables (the server itself is Terraform-provisioned)
- `/infra/airbyte` — `install.sh` installing `abctl` and running
  `abctl local install` for self-hosted Airbyte (the VM itself is
  Terraform-provisioned)
- `/notebooks` — PySpark notebooks, run in order: `00_setup.sql`,
  `01_silver_transform.py`, `02_gold_aggregation.py`, `03_quality_checks.py`.
  No separate Bronze ingestion notebook - Airbyte's Databricks destination
  connector writes Bronze directly as Delta tables
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
  should run from a single orchestrated entry point.
- Keep transformation logic testable: prefer functions in `/src` over inline notebook
  cells when logic is reused across notebooks.
- Table/column naming stays in the source dataset's original casing
  (e.g. `SK_ID_CURR`, `AMT_INCOME_TOTAL`) for traceability back to the raw CSVs.

## Working locally

Operational details for resuming work in a fresh session - not architecture,
just "how do I actually run the next command."

- **Tools live in `~/.local/bin`, not on PATH by default** - `uv`, `terraform`,
  `gh`, `az`, `databricks` were all installed there (no sudo on this machine).
  Every fresh shell needs `export PATH="$HOME/.local/bin:$PATH"` first.
- **VM SSH key**: `~/.ssh/consumer-lending-airbyte-vm` (outside the repo, not
  gitignored because it doesn't need to be - it's just not in the repo at
  all). `ssh -i ~/.ssh/consumer-lending-airbyte-vm azureuser@<vm-ip>`.
- **The VM's public IP is dynamic** - it's changed three times already across
  region/size moves. Get the current one from
  `terraform output airbyte_vm_public_ip` in `infra/terraform/platform`;
  don't assume a previously-known IP is still current.
- **Real secrets already exist on disk, gitignored** - don't ask "what's the
  password," read the file:
  - `infra/terraform/.env` - `ARM_CLIENT_ID`/`ARM_CLIENT_SECRET`/
    `ARM_TENANT_ID`/`ARM_SUBSCRIPTION_ID` for the Terraform service principal,
    sourced before every `terraform` command in both stages.
  - `infra/terraform/platform/terraform.tfvars` - Postgres admin password,
    admin IP.
  - `infra/terraform/airbyte-config/terraform.tfvars` - Databricks
    client_id/secret. Airbyte client_id/secret are also here but only used by
    `get_token.sh`, not the provider directly (see "Infrastructure as Code").
  - `~/.databrickscfg` - a Databricks PAT for the `databricks` CLI (SQL
    grants, warehouse start/stop). Lives on the dev machine, not the VM.
- **`get_token.sh` needs three env vars exported first, then eval'd**:
  ```
  export AIRBYTE_CLIENT_ID="..."     # from terraform.tfvars
  export AIRBYTE_CLIENT_SECRET="..." # from terraform.tfvars
  export AIRBYTE_VM_IP="..."         # from terraform output
  eval "$(./get_token.sh)"           # sets TF_VAR_airbyte_bearer_token
  ```
- **`az`/`gh`/`databricks` auth were all set up interactively** (browser
  device-code flows) - if a fresh session hits auth errors from any of them,
  that's expected; these can't be restarted programmatically. Ask the user to
  re-run `az login` / `gh auth login`, or regenerate the Databricks PAT.

**Resuming work, in order:**
1. `cd infra/terraform/platform && ./toggle.sh start` - takes a few minutes;
   both Postgres and the VM need to actually come up.
2. If the VM was *recreated* (not just restarted) since the last session, its
   IP changed - run a plain `terraform apply` in `platform` to reconcile the
   Postgres firewall rule to the new IP.
3. Get a fresh Airbyte bearer token (`get_token.sh`, above) before touching
   `infra/terraform/airbyte-config` - the previous one expired in ~15 minutes.
4. `terraform plan` in `airbyte-config` to check for drift before assuming
   the source/destination/connection are still intact.

## Completion criteria

- PostgreSQL running on Azure (Flexible Server, free-tier) with the 8 tables loaded,
  simulating the transactional source.
- Airbyte configured and syncing successfully from Postgres to Databricks
  Unity Catalog's `bronze` schema.
- Pipeline runs end-to-end (bronze → gold) from a single command/orchestrated notebook.
- Star schema documented with an ER diagram.
- Power BI dashboard published with at least 3 visualizations answering the business
  problem (risk distribution by segment, default rate by income/age band, drill-down
  by individual application).
- README with problem statement, full architecture (including the ingestion layer),
  and how to run.

## Future enhancements

Not part of the current build - revisit once the pipeline works end-to-end:

- **CDC from Postgres to Databricks** — replace Airbyte's Xmin-based
  incremental replication with true Change Data Capture (logical replication
  slot + publication on Postgres, Debezium under the hood via Airbyte). More
  realistic for a live origination system, and a stronger signal of the CDC
  skill for senior data engineering roles.
- **Simulate ongoing originations** — a small script that periodically
  inserts/updates rows in Postgres (new applications, updated
  `previous_application` rows, etc.). The CSVs are a one-time historical
  load, so CDC has nothing to capture without this - without ongoing writes,
  CDC is functionally identical to a one-time snapshot.

## Status

Unity Catalog is fully wired up (metastore, storage credential, external location,
catalog `consumer_lending_risk_lakehouse` with `bronze`/`silver`/`gold` schemas
created). The 8 CSVs were uploaded directly to a Volume
(`bronze.raw_files`) as a bypassed intermediate step — under the new architecture,
Airbyte's Databricks Lakehouse destination connector writes Bronze directly as
Delta tables, not that direct upload.

Postgres was stood up locally via Docker first and loaded with all 8 tables as a
first pass (`infra/postgres/load_csvs.py`); the local container, image, and volume
have since been removed. The project has since moved that source to Azure Database
for PostgreSQL – Flexible Server (see "Simulated source system" for why), with
Terraform provisioning it and the Airbyte VM (see "Infrastructure as Code").

The `infra/terraform/platform` module is applied and live: the Postgres Flexible
Server (`credit-origination-pg-01`, `centralus`) has all 8 tables loaded, and
Airbyte is installed and running on the VM (`Standard_D2as_v7`, `eastus2`) via
`abctl`, pinned to chart `1.8.5` with `--insecure-cookies` (see below for why) -
the web UI fully works: login succeeds and every workspace page (Sources,
Destinations, Connections, Settings) renders correctly. Credentials are
retrievable via `abctl local credentials`.

`infra/terraform/airbyte-config` is also applied and live: the Postgres source,
the Databricks Lakehouse destination, and the connection between them
(`Credit Origination -> Bronze`) all exist in Airbyte, and both connectors'
`check_connection` calls succeed. Originally planned as an Azure Blob
Storage/ADLS Gen2 destination landing Parquet - changed after discovering
Airbyte's Azure Blob Storage connector only supports CSV/JSONL (no Parquet at
all), and switching to the Databricks Lakehouse connector instead (writing
Delta tables directly into Unity Catalog, no intermediate landing step) turned
out simpler anyway.

Both Postgres and the VM are currently **stopped** (`toggle.sh stop` / `az vm
deallocate`) to avoid unnecessary cost - leave them off until asked to turn
them back on.

Getting both stages working took several rounds of discovering things the
docs/quota checks didn't reveal upfront:

- **Docker Compose deployment for Airbyte OSS was deprecated in August 2024.**
  Only `abctl` (Kubernetes via `kind`, on Docker) is supported now - CLAUDE.md
  originally committed to Docker Compose before this was discovered.
- **All three free-tier VM sizes (B1s/B2pts_v2/B2ats_v2) cap at 1GB RAM** -
  nowhere near enough for `abctl`'s Kubernetes control plane, even in
  `--low-resource-mode`. The first real install attempt (on `B2pts_v2`, ARM)
  failed after ~11 minutes with the control plane's healthz checks timing out.
  This is a hard ceiling on the whole free-tier VM family, not a fluke.
- **This subscription (Free Trial) has its own per-region, per-SKU allow-list**
  that has nothing to do with quota - `Standard_B2s`, then `Standard_D2s_v3`,
  both failed with `SkuNotAvailable`/capacity restrictions in `centralus`, and
  `D2s_v3` failed in `eastus2` too, despite untouched quota for all of them.
  Querying the Resource SKUs API directly (see "Infrastructure as Code") for
  SKUs with an empty `restrictions` array found `Standard_D2as_v7`, which
  worked immediately in `eastus2`.
- Postgres and the VM ended up in different regions as a result (`location`
  vs. `vm_location`) - fine, since they only talk over Postgres's public
  endpoint.
- **Airbyte's Azure Blob Storage destination connector only supports CSV or
  JSONL** - no Parquet option exists, and no separate ADLS Gen2-specific
  connector exists either (unlike "S3 Data Lake"/"GCS Data Lake" for other
  clouds). Switched to the Databricks Lakehouse destination connector instead
  (writes Delta tables directly into Unity Catalog - simpler than landing
  files at all).
- **Databricks Lakehouse destination setup**: reused the existing "Serverless
  Starter Warehouse" (`9b1762275ab88f7f`) rather than creating a new one.
  Created a dedicated service principal (`sp-airbyte-bronze-ingestion`,
  application ID `fe2874b6-e710-4833-9576-bf8f0d973161`) via the Databricks
  CLI rather than using a personal token, and granted it `USE CATALOG` on
  `consumer_lending_risk_lakehouse` plus `USE SCHEMA`, `CREATE TABLE`,
  `MODIFY`, `SELECT`, and `CREATE VOLUME` on the `bronze` schema (the last one
  found only after the destination's `check_connection` failed with a clear
  `PERMISSION_DENIED` on `CREATE VOLUME` - the connector uses a Unity Catalog
  volume internally for staging).
- **The Airbyte Terraform provider's OAuth2 auth
  (`client_id`/`client_secret`/`token_url`) 401s against this self-hosted
  instance** - it sends the token request without the content-type header
  this instance's custom `/api/v1/applications/token` endpoint needs (a known
  upstream issue, not specific to this setup). `bearer_auth` with a token
  fetched directly works fine, but that token is short-lived (~15 min) - see
  `get_token.sh` and `versions.tf`.
- **The hardest bug**: after fixing auth, `airbyte_source`/`airbyte_connection`
  creation failed with `SQL state 08001` ("connection attempt failed") -
  immediately, in ~3ms, too fast to be a genuine network timeout. Node-level
  connectivity tests (DNS, raw TCP, even a full SSL handshake, all run via
  `docker exec` into the `airbyte-abctl-control-plane` container) succeeded
  perfectly, which was misleading - `kind` pods use a *separate* pod-network
  CNI (CIDR `10.244.0.0/24`) layered on top of the node's own Docker network,
  and that CIDR had **no MASQUERADE rule** at all in the host's iptables,
  discovered by testing from inside an actual scheduled Kubernetes pod
  (`kubectl run` + `nc`) rather than the node container - DNS resolved fine
  and TCP handshakes even completed from the node's perspective, but pod
  traffic timed out trying to actually leave the VM. `kindnetd`'s initial
  setup adds this rule once, but it's pure in-memory iptables state - it does
  not survive a VM stop/start, and nothing re-adds it automatically on
  restart. Fixed with a script + systemd oneshot service
  (`fix-kind-pod-nat.sh` / `fix-kind-pod-nat.service`, baked into cloud-init
  and also installed directly on the live VM) that re-adds the rule on every
  boot, idempotently.
- **The Airbyte webapp itself crashed on every workspace page** (Sources,
  Destinations, Connections, even Settings) with `React error #185` ("Maximum
  update depth exceeded") - reproducible even in incognito, so not a browser
  cache issue. Tried and ruled out in order: the `initial_setup_complete` /
  `display_setup_wizard` workspace flags (had to flip these directly in
  Postgres too - `abctl`'s bootstrap doesn't set them, and the normal setup
  wizard couldn't complete because of this same crash, a chicken-and-egg
  problem), a missing `workspace_admin` permission row for the default user,
  and the `AIRBYTE_URL` ConfigMap value being stuck at `localhost:8000`
  instead of the VM's actual address. None of it fixed the crash. Turned out
  to be a confirmed **upstream bug** (`airbytehq/airbyte#76834`, filed against
  `2.1.0`, closed only via a private internal ticket) - and the real root
  cause: **`abctl`'s "latest" currently resolves to an alpha-tagged Helm chart**
  (`1.9.x`+, whose `appVersion` strings all contain `-alpha-`, even though the
  app itself reports a clean version like `2.1.1`). The actual last stable
  chart is `1.8.5` (also its own `appVersion` - the two diverged after that).
  Fixed by uninstalling, deleting the stale `~/.airbyte/abctl/data/` volume
  directories (`abctl local uninstall` doesn't clean these up, and they carry
  over an incompatible Postgres data directory across versions), and
  reinstalling with `--chart-version 1.8.5`. That alone surfaced a second,
  separate issue - login succeeded but no session cookie was set ("you appear
  to have deployed over HTTP") - fixed with `--insecure-cookies` (also
  required a full reinstall to apply; no `helm` CLI is available on the VM to
  patch it in place). A fresh install means a new workspace ID and new
  `abctl local credentials` - the Postgres source, Databricks destination,
  and connection all had to be recreated via `terraform state rm` +
  `apply` once the new credentials were in `terraform.tfvars`.

Next: run an actual sync on the `Credit Origination -> Bronze` connection and
verify the 8 tables land correctly as Delta tables in
`consumer_lending_risk_lakehouse.bronze`, then write `01_silver_transform.py`.
Estimated 2-3 weeks at 5-8h/week (already running longer given the added
ingestion layer and the troubleshooting above).
