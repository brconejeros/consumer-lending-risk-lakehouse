# Writes directly into Unity Catalog as Delta tables - no intermediate
# object-storage landing step needed (Databricks stages internally), so this
# replaces the originally-planned Azure Blob Storage destination. See
# CLAUDE.md "Simulated source system" / Status for why.
data "airbyte_connector_configuration" "databricks" {
  connector_name    = "destination-databricks"
  connector_version = "4.0.1"

  configuration = {
    hostname     = var.databricks_hostname
    http_path    = var.databricks_http_path
    port         = "443"
    database     = var.databricks_catalog
    schema       = var.databricks_schema
    accept_terms = true
    authentication = {
      auth_type = "OAUTH"
      client_id = var.databricks_client_id
      secret    = var.databricks_client_secret
    }
  }
}

resource "airbyte_destination" "databricks" {
  name          = "Bronze Unity Catalog"
  workspace_id  = var.airbyte_workspace_id
  definition_id = data.airbyte_connector_configuration.databricks.definition_id
  configuration = data.airbyte_connector_configuration.databricks.configuration_json
}
