#!/usr/bin/env bash
# Installs and starts self-hosted Airbyte OSS on the Airbyte VM via abctl.
#
# Docker Compose deployment for Airbyte OSS was deprecated in August 2024;
# abctl is the current supported method - it runs Airbyte via a `kind`
# Kubernetes cluster on top of Docker (which cloud-init already installed).
#
# Run this on the Airbyte VM itself (see infra/terraform/platform), not the
# dev machine.
#
# Pinned to chart 1.8.5 deliberately - abctl's default ("latest") currently
# resolves to an alpha-tagged chart line (1.9.x+, app version reported as
# e.g. "2.1.1" with no "alpha" in that string, but the chart itself is
# alpha), which has a confirmed webapp bug (React error #185 - infinite
# render loop on every workspace page, matches airbytehq/airbyte#76834).
# 1.8.5 is the actual last stable chart release. See CLAUDE.md Status for
# the full story.
#
# --insecure-cookies is required too: without it, login succeeds but the
# session cookie never gets set ("the server failed to set a cookie... you
# appear to have deployed over HTTP") since we're on http://<ip>:8000, not
# HTTPS.
set -euo pipefail

curl -LsfS https://get.airbyte.com | bash -
abctl local install --low-resource-mode --no-browser --chart-version 1.8.5 --insecure-cookies

# abctl doesn't have a flag to set this for a bare IP host (--host rejects
# IP addresses) - patch it directly, then restart the server pod to pick it
# up. Without this, instance_configuration.airbyteUrl stays
# "airbyte-url-not-configured", which can cause its own frontend issues.
VM_IP=$(curl -s https://ifconfig.me)
docker exec airbyte-abctl-control-plane kubectl patch configmap -n airbyte-abctl airbyte-abctl-airbyte-env \
  --type merge -p "{\"data\":{\"AIRBYTE_URL\":\"http://${VM_IP}:8000\"}}"
docker exec airbyte-abctl-control-plane kubectl rollout restart -n airbyte-abctl deploy/airbyte-abctl-server
docker exec airbyte-abctl-control-plane kubectl rollout status -n airbyte-abctl deploy/airbyte-abctl-server --timeout=120s
