terraform {
  required_version = ">= 1.15"

  required_providers {
    airbyte = {
      source  = "airbytehq/airbyte"
      version = "~> 1.2"
    }
  }

  # Same bootstrapped storage account as infra/terraform/platform, different
  # key - a separate state file, since this stage depends on platform already
  # being applied and Airbyte being reachable. terraform_remote_state reads
  # this backend directly; no azurerm provider needed for that.
  backend "azurerm" {
    resource_group_name  = "ingestion"
    storage_account_name = "tfstateingestdfed4d"
    container_name       = "tfstate"
    key                  = "airbyte-config.tfstate"
    use_azuread_auth     = true
  }
}

provider "airbyte" {
  server_url = "http://${data.terraform_remote_state.platform.outputs.airbyte_vm_public_ip}:8000/api/public/v1"

  # Not client_id/client_secret/token_url: this provider's OAuth2 flow sends
  # the token request without the content-type header this self-hosted
  # instance's custom /applications/token endpoint needs, so it 401s (a
  # known upstream issue, not specific to our setup). bearer_auth with a
  # token fetched directly works fine - but that token is short-lived
  # (~15 min), so fetch a fresh one immediately before every plan/apply:
  #   curl -X POST http://<vm-ip>:8000/api/v1/applications/token \
  #     -H "Content-Type: application/json" \
  #     -d '{"client_id":"...","client_secret":"..."}'
  bearer_auth = var.airbyte_bearer_token
}
