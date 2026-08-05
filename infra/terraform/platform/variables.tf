variable "location" {
  description = "Azure region for the Postgres Flexible Server"
  type        = string
  default     = "centralus"
}

variable "resource_group_name" {
  description = "Resource group holding the ingestion infra (created out-of-band via az cli, referenced as a data source)"
  type        = string
  default     = "ingestion"
}

variable "postgres_admin_username" {
  description = "Admin login for the Postgres Flexible Server"
  type        = string
  default     = "pgadmin"
}

variable "postgres_admin_password" {
  description = "Admin password for the Postgres Flexible Server"
  type        = string
  sensitive   = true
}

variable "postgres_sku_name" {
  description = "Free-tier-eligible Burstable SKU for the Postgres Flexible Server"
  type        = string
  default     = "B_Standard_B1ms"
}

variable "postgres_storage_mb" {
  description = "Storage size in MB (free tier covers up to 32 GB)"
  type        = number
  default     = 32768
}

variable "postgres_version" {
  description = "Postgres major version"
  type        = string
  default     = "16"
}

variable "admin_ip_address" {
  description = "Your current public IP (no CIDR suffix) - scopes the Postgres firewall rule to just this address"
  type        = string
}
