#!/usr/bin/env bash
# Starts/stops the Postgres Flexible Server without touching Terraform state
# - use this for day-to-day on/off, `terraform destroy` for actually tearing
# the resource down. Idempotent: safe to run even if it's already in the
# target state. No VM to manage anymore - ADF is fully managed/serverless
# (see infra/terraform/data-factory), billed per pipeline run, not per hour.
set -euo pipefail

ACTION="${1:-}"
RESOURCE_GROUP="ingestion"
PG_SERVER="credit-origination-pg-01"

case "$ACTION" in
  start)
    pg_state=$(az postgres flexible-server show --name "$PG_SERVER" --resource-group "$RESOURCE_GROUP" --query state -o tsv)
    if [ "$pg_state" != "Ready" ]; then
      az postgres flexible-server start --name "$PG_SERVER" --resource-group "$RESOURCE_GROUP"
    fi
    ;;
  stop)
    pg_state=$(az postgres flexible-server show --name "$PG_SERVER" --resource-group "$RESOURCE_GROUP" --query state -o tsv)
    if [ "$pg_state" = "Ready" ]; then
      az postgres flexible-server stop --name "$PG_SERVER" --resource-group "$RESOURCE_GROUP"
    fi
    ;;
  *)
    echo "Usage: $0 {start|stop}" >&2
    exit 1
    ;;
esac
