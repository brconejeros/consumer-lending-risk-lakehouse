#!/usr/bin/env bash
# Starts/stops the Postgres Flexible Server and Airbyte VM without touching
# Terraform state - use this for day-to-day on/off, `terraform destroy` for
# actually tearing the resources down.
set -euo pipefail

ACTION="${1:-}"
RESOURCE_GROUP="ingestion"
PG_SERVER="credit-origination-pg-01"
VM_NAME="airbyte-vm"

case "$ACTION" in
  start)
    az postgres flexible-server start --name "$PG_SERVER" --resource-group "$RESOURCE_GROUP"
    az vm start --name "$VM_NAME" --resource-group "$RESOURCE_GROUP"
    ;;
  stop)
    az vm deallocate --name "$VM_NAME" --resource-group "$RESOURCE_GROUP"
    az postgres flexible-server stop --name "$PG_SERVER" --resource-group "$RESOURCE_GROUP"
    ;;
  *)
    echo "Usage: $0 {start|stop}" >&2
    exit 1
    ;;
esac
