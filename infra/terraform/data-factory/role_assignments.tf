# Lets ADF's system-assigned identity write Parquet into the landing
# filesystem via MSI - no storage account keys anywhere in this module.
resource "azurerm_role_assignment" "adf_to_landing" {
  scope                = azurerm_storage_account.landing.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.main.identity[0].principal_id
}
