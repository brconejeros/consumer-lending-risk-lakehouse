variable "location" {
  description = "Azure region for the ingestion resources (independent of the resource group's own location)"
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

variable "vm_size" {
  description = "Free-tier-eligible burstable VM size for the Airbyte host"
  type        = string
  default     = "Standard_B2pts_v2"
}

variable "vm_image_sku" {
  description = "Ubuntu 24.04 LTS image SKU matching vm_size's architecture (server-arm64 for B2pts_v2, server for x64 sizes like B1s/B2ats_v2)"
  type        = string
  default     = "server-arm64"
}

variable "vm_admin_username" {
  description = "Admin username for the Airbyte VM"
  type        = string
  default     = "azureuser"
}

variable "ssh_public_key_path" {
  description = "Path to the SSH public key used for VM admin login"
  type        = string
  default     = "~/.ssh/consumer-lending-airbyte-vm.pub"
}

variable "admin_ip_address" {
  description = "Your current public IP (no CIDR suffix) - scopes the NSG rules and the Postgres firewall rule to just this address"
  type        = string
}
