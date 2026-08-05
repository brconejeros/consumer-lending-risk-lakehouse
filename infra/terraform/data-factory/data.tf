# Created out-of-band via az cli during bootstrap (holds the tfstate storage
# account too) - referenced here, not managed, so `terraform destroy` never
# touches it.
data "azurerm_resource_group" "ingestion" {
  name = var.resource_group_name
}

# Pulls Postgres's FQDN from the platform stage's state - same backend,
# different key.
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
