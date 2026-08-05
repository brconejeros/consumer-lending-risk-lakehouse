resource "azurerm_data_factory" "main" {
  name                = "adf-credit-origination"
  location            = var.location
  resource_group_name = data.azurerm_resource_group.ingestion.name

  # System-assigned identity used for MSI auth to the landing storage account
  # (see role_assignments.tf) - no storage account keys anywhere in this
  # module.
  identity {
    type = "SystemAssigned"
  }
}
