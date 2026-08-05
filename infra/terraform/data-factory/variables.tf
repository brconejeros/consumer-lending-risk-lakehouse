variable "location" {
  description = "Azure region for the Data Factory and landing storage account. Matches Postgres's region (centralus) for simplicity - unlike the old Airbyte VM, ADF has no per-region VM SKU restrictions forcing a region split"
  type        = string
  default     = "centralus"
}

variable "resource_group_name" {
  description = "Resource group holding the ingestion infra (created out-of-band via az cli, referenced as a data source)"
  type        = string
  default     = "ingestion"
}

variable "landing_storage_account_name" {
  description = "Globally-unique name for the ADLS Gen2 landing storage account (lowercase alphanumeric, <= 24 chars)"
  type        = string
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
