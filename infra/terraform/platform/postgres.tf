# Created out-of-band via az cli during bootstrap (holds the tfstate storage
# account too) - referenced here, not managed, so `terraform destroy` never
# touches it.
data "azurerm_resource_group" "ingestion" {
  name = var.resource_group_name
}

resource "azurerm_postgresql_flexible_server" "origination" {
  name                = "credit-origination-pg-01"
  resource_group_name = data.azurerm_resource_group.ingestion.name
  location            = var.location

  version                = var.postgres_version
  administrator_login    = var.postgres_admin_username
  administrator_password = var.postgres_admin_password
  storage_mb             = var.postgres_storage_mb
  sku_name               = var.postgres_sku_name
  backup_retention_days  = 7
  zone                   = "1"

  public_network_access_enabled = true

  lifecycle {
    # Azure auto-assigns the zone on create; it can't be changed in-place
    # outside of an HA failover, so don't let drift here force a replace.
    ignore_changes = [zone]
  }
}

resource "azurerm_postgresql_flexible_server_database" "credit_origination" {
  name      = "credit_origination_db"
  server_id = azurerm_postgresql_flexible_server.origination.id
  charset   = "UTF8"
  collation = "en_US.utf8"
}

# So load_csvs.py and psql can reach it directly from the dev machine.
resource "azurerm_postgresql_flexible_server_firewall_rule" "allow_admin_ip" {
  name             = "allow-admin-ip"
  server_id        = azurerm_postgresql_flexible_server.origination.id
  start_ip_address = var.admin_ip_address
  end_ip_address   = var.admin_ip_address
}

# Azure's documented sentinel rule (0.0.0.0/0.0.0.0) for "allow access from
# Azure services" - needed since ADF's default Azure Integration Runtime has
# no fixed outbound IP, unlike the old Airbyte VM's static public IP.
resource "azurerm_postgresql_flexible_server_firewall_rule" "allow_azure_services" {
  name             = "allow-azure-services"
  server_id        = azurerm_postgresql_flexible_server.origination.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}
