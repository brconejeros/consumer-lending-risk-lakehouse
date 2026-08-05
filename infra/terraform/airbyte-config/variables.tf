variable "airbyte_client_id" {
  description = "OAuth2 client ID for Airbyte's API, from `abctl local credentials` on the Airbyte VM. Not used by the provider directly (see airbyte_bearer_token) - used by get_token.sh to mint a fresh bearer token"
  type        = string
  sensitive   = true
}

variable "airbyte_client_secret" {
  description = "OAuth2 client secret for Airbyte's API, from `abctl local credentials` on the Airbyte VM. Not used by the provider directly (see airbyte_bearer_token) - used by get_token.sh to mint a fresh bearer token"
  type        = string
  sensitive   = true
}

variable "airbyte_bearer_token" {
  description = "Short-lived (~15 min) bearer token for the airbyte provider - fetch a fresh one with ./get_token.sh immediately before every plan/apply, don't put this in terraform.tfvars"
  type        = string
  sensitive   = true
}

variable "airbyte_workspace_id" {
  description = "abctl creates exactly one default workspace per install - its ID, found via GET /api/public/v1/workspaces"
  type        = string
  default     = "38ad6937-82f1-466d-833b-bc25412e2ff6"
}

variable "postgres_admin_username" {
  description = "Must match infra/terraform/platform's postgres_admin_username"
  type        = string
  default     = "pgadmin"
}

variable "postgres_admin_password" {
  description = "Must match infra/terraform/platform's postgres_admin_password"
  type        = string
  sensitive   = true
}

variable "postgres_database" {
  description = "Must match infra/terraform/platform's credit_origination_db database"
  type        = string
  default     = "credit_origination_db"
}

variable "databricks_hostname" {
  description = "Databricks workspace hostname (no https://, no trailing slash)"
  type        = string
  default     = "adb-7405619456327656.16.azuredatabricks.net"
}

variable "databricks_http_path" {
  description = "HTTP path of the SQL warehouse Airbyte writes through"
  type        = string
  default     = "/sql/1.0/warehouses/9b1762275ab88f7f"
}

variable "databricks_catalog" {
  description = "Unity Catalog name"
  type        = string
  default     = "consumer_lending_risk_lakehouse"
}

variable "databricks_schema" {
  description = "Unity Catalog schema Airbyte writes into"
  type        = string
  default     = "bronze"
}

variable "databricks_client_id" {
  description = "Application ID of the dedicated sp-airbyte-bronze-ingestion service principal (see CLAUDE.md Status for how it was created and granted)"
  type        = string
  sensitive   = true
}

variable "databricks_client_secret" {
  description = "OAuth secret for sp-airbyte-bronze-ingestion"
  type        = string
  sensitive   = true
}
