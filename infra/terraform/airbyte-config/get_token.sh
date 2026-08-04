#!/usr/bin/env bash
# Mints a fresh Airbyte bearer token (~15 min lifetime) and prints an export
# statement for TF_VAR_airbyte_bearer_token. The airbyte provider can't use
# client_id/client_secret/token_url directly against this self-hosted
# instance (see versions.tf for why), so run this immediately before every
# plan/apply:
#
#   eval "$(./get_token.sh)"
#   terraform plan
set -euo pipefail

CLIENT_ID="${AIRBYTE_CLIENT_ID:?set AIRBYTE_CLIENT_ID}"
CLIENT_SECRET="${AIRBYTE_CLIENT_SECRET:?set AIRBYTE_CLIENT_SECRET}"
VM_IP="${AIRBYTE_VM_IP:?set AIRBYTE_VM_IP}"

TOKEN=$(curl -sS --max-time 10 -X POST "http://${VM_IP}:8000/api/v1/applications/token" \
  -H "Content-Type: application/json" \
  -d "{\"client_id\":\"${CLIENT_ID}\",\"client_secret\":\"${CLIENT_SECRET}\"}" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

echo "export TF_VAR_airbyte_bearer_token='${TOKEN}'"
