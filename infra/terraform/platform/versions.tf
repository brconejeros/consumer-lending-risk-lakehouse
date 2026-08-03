terraform {
  required_version = ">= 1.15"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }

  # Bootstrapped out-of-band via az cli (see CLAUDE.md, "Infrastructure as
  # Code") since Terraform can't create the backend it's about to use.
  backend "azurerm" {
    resource_group_name  = "ingestion"
    storage_account_name = "tfstateingestdfed4d"
    container_name       = "tfstate"
    key                  = "platform.tfstate"
    use_azuread_auth     = true
  }
}

provider "azurerm" {
  features {}

  # The service principal Terraform runs as is scoped to just the `ingestion`
  # resource group (least privilege), so it can't register providers at the
  # subscription level. Providers are registered once out-of-band via az cli
  # under a broader identity instead (see CLAUDE.md).
  resource_provider_registrations = "none"
}
