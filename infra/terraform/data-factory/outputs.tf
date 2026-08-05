output "data_factory_name" {
  description = "Name of the Azure Data Factory instance"
  value       = azurerm_data_factory.main.name
}

output "landing_storage_account_name" {
  description = "Name of the ADLS Gen2 landing storage account"
  value       = azurerm_storage_account.landing.name
}

output "landing_dfs_endpoint" {
  description = "Primary DFS (ADLS Gen2) endpoint of the landing storage account - used to build abfss:// paths"
  value       = azurerm_storage_account.landing.primary_dfs_endpoint
}

output "pipeline_name" {
  description = "Name of the Postgres -> landing Parquet pipeline (for az datafactory pipeline create-run)"
  value       = azurerm_data_factory_pipeline.bronze_landing.name
}

output "databricks_access_connector_id" {
  description = "Resource ID of the access connector - needed for CREATE STORAGE CREDENTIAL in Unity Catalog"
  value       = azurerm_databricks_access_connector.landing.id
}
