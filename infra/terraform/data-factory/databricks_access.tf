# Lets the Bronze ingestion notebooks read the landing container via a
# Unity Catalog external location (CREATE STORAGE CREDENTIAL + CREATE
# EXTERNAL LOCATION referencing this connector's principal_id, done
# manually to match how the project's existing external location was set
# up - no databricks Terraform provider in this repo).
resource "azurerm_databricks_access_connector" "landing" {
  name                = "dac-credit-landing"
  resource_group_name = data.azurerm_resource_group.ingestion.name
  location            = var.location

  identity {
    type = "SystemAssigned"
  }
}

# Contributor, not Reader - Unity Catalog's own CREATE EXTERNAL LOCATION
# validation does a write+delete round-trip test file against the
# connector's identity, independent of the read-only grant given to the
# service principal at the Unity Catalog layer (see CLAUDE.md/plan: the SP
# only gets READ FILES on the external location itself).
resource "azurerm_role_assignment" "databricks_to_landing" {
  scope                = azurerm_storage_account.landing.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.landing.identity[0].principal_id
}
