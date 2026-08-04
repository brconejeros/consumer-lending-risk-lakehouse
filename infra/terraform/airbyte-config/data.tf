# Pulls the Airbyte VM's IP and Postgres's FQDN from the platform stage's
# state - same backend, different key, kept as a separate apply because this
# stage needs Airbyte already up and reachable.
data "terraform_remote_state" "platform" {
  backend = "azurerm"
  config = {
    resource_group_name  = "ingestion"
    storage_account_name = "tfstateingestdfed4d"
    container_name       = "tfstate"
    key                  = "platform.tfstate"
    use_azuread_auth     = true
  }
}
