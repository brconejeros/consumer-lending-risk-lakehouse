#!/usr/bin/env bash
# Installs and starts self-hosted Airbyte OSS on the Airbyte VM via abctl.
#
# Docker Compose deployment for Airbyte OSS was deprecated in August 2024;
# abctl is the current supported method - it runs Airbyte via a `kind`
# Kubernetes cluster on top of Docker (which cloud-init already installed).
#
# Run this on the Airbyte VM itself (see infra/terraform/platform), not the
# dev machine.
set -euo pipefail

curl -LsfS https://get.airbyte.com | bash -
abctl local install --low-resource-mode --no-browser
