#!/usr/bin/env bash
# Kicks off the Postgres -> ADLS landing Copy pipeline (see pipeline.tf),
# waits for it to finish, then explicitly triggers the 8 pipeline_<table>
# Databricks jobs.
#
# That explicit trigger step exists because their configured File Arrival
# triggers don't actually fire: ADF's Copy Activity always writes the same
# filename per table (e.g. landing/bureau/part-0000.parquet), and
# Databricks' own docs are explicit that "Overwriting an existing file
# with a file of the same name does not trigger a run" - confirmed live
# 2026-08-15 (see CLAUDE.md "Databricks orchestration"). The 5 `gold_*`
# jobs and `quality_checks` don't have this problem - their Table Update
# triggers watch Delta table commits, not raw filenames, so they still
# cascade on their own once the 8 pipeline jobs land in Silver.
set -euo pipefail

RESOURCE_GROUP="ingestion"
DATABRICKS_PROFILE="azure"
POLL_INTERVAL_SECONDS=20
MAX_POLL_ATTEMPTS=90 # 30 minutes

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_FACTORY_NAME=$(terraform -chdir="$SCRIPT_DIR" output -raw data_factory_name)
PIPELINE_NAME=$(terraform -chdir="$SCRIPT_DIR" output -raw pipeline_name)
SUBSCRIPTION_ID=$(az account show --query id -o tsv)

RUN_ID=$(az rest \
  --method POST \
  --uri "https://management.azure.com/subscriptions/${SUBSCRIPTION_ID}/resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.DataFactory/factories/${DATA_FACTORY_NAME}/pipelines/${PIPELINE_NAME}/createRun?api-version=2018-06-01" \
  --query runId -o tsv)

echo "ADF pipeline run ${RUN_ID} started, waiting for it to finish..."

RUN_URI="https://management.azure.com/subscriptions/${SUBSCRIPTION_ID}/resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.DataFactory/factories/${DATA_FACTORY_NAME}/pipelineruns/${RUN_ID}?api-version=2018-06-01"

STATUS="InProgress"
ATTEMPT=0
while [ "$STATUS" = "InProgress" ] || [ "$STATUS" = "Queued" ]; do
  ATTEMPT=$((ATTEMPT + 1))
  if [ "$ATTEMPT" -gt "$MAX_POLL_ATTEMPTS" ]; then
    echo "Timed out waiting for ADF pipeline run ${RUN_ID} to finish (still ${STATUS} after 30 minutes)." >&2
    exit 1
  fi
  sleep "$POLL_INTERVAL_SECONDS"
  STATUS=$(az rest --method GET --uri "$RUN_URI" --query status -o tsv)
  echo "  status=${STATUS}"
done

if [ "$STATUS" != "Succeeded" ]; then
  echo "ADF pipeline run ${RUN_ID} did not succeed (status=${STATUS}) - not triggering the Databricks jobs." >&2
  exit 1
fi

echo "ADF succeeded - triggering the 8 pipeline_<table> Databricks jobs (see header comment for why this is explicit rather than trigger-driven)."

databricks jobs list --profile "$DATABRICKS_PROFILE" -o json | python3 -c "
import json, sys
for j in json.load(sys.stdin):
    name = j['settings']['name']
    if name.startswith('pipeline_'):
        print(j['job_id'], name)
" | while read -r job_id job_name; do
  echo "  triggering ${job_name} (${job_id})"
  databricks jobs run-now "$job_id" --no-wait --profile "$DATABRICKS_PROFILE" > /dev/null
done

echo "All 8 pipeline_<table> jobs triggered. Gold and Quality jobs will cascade on their own via Table Update triggers."
