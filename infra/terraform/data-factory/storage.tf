# Brand-new, Terraform-managed landing zone for ADF's Parquet output -
# deliberately decoupled from Unity Catalog's own metastore storage, which
# was set up manually/out-of-band (no databricks provider exists in this
# repo). Standard/LRS/Hot: at this project's ~700MB one-time-ish batch
# scale, tiering optimization is noise, and Hot avoids Cool's early-deletion
# penalty during dev iteration/re-runs.
resource "azurerm_storage_account" "landing" {
  name                     = var.landing_storage_account_name
  resource_group_name      = data.azurerm_resource_group.ingestion.name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"

  # Required for actual ADLS Gen2 (hierarchical namespace) semantics - without
  # this it's just flat Blob storage with a Gen2-shaped API on top.
  is_hns_enabled = true

  # ADF's default Azure Integration Runtime isn't VNet-injected and has no
  # fixed outbound IP, same as Postgres's own public-endpoint-only setup.
  public_network_access_enabled = true
}

resource "azurerm_storage_data_lake_gen2_filesystem" "landing" {
  name               = "landing"
  storage_account_id = azurerm_storage_account.landing.id
}
