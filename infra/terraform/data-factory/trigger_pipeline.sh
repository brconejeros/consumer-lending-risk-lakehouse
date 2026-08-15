#!/usr/bin/env bash
# Kicks off the Postgres -> ADLS landing Copy pipeline (see pipeline.tf).
# Replaces typing the `az rest ... createRun` call from memory each
# session (see CLAUDE.md "Working locally") - this is the one deliberate
# manual step in the pipeline; everything downstream (the 14 Databricks
# jobs in databricks.yml) fires itself once the Parquet lands, via File
# Arrival/Table Update triggers - see CLAUDE.md "Databricks orchestration".
set -euo pipefail

RESOURCE_GROUP="ingestion"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_FACTORY_NAME=$(terraform -chdir="$SCRIPT_DIR" output -raw data_factory_name)
PIPELINE_NAME=$(terraform -chdir="$SCRIPT_DIR" output -raw pipeline_name)
SUBSCRIPTION_ID=$(az account show --query id -o tsv)

az rest \
  --method POST \
  --uri "https://management.azure.com/subscriptions/${SUBSCRIPTION_ID}/resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.DataFactory/factories/${DATA_FACTORY_NAME}/pipelines/${PIPELINE_NAME}/createRun?api-version=2018-06-01"
