# PostgreSqlV2 via a custom linked service, not the typed
# azurerm_data_factory_linked_service_postgresql resource - that resource has
# a confirmed open provider bug (hashicorp/terraform-provider-azurerm#22890)
# where it silently ignores SSL parameters and always connects with
# EncryptionMethod=0, which won't work against a Flexible Server that
# enforces SSL by default.
resource "azurerm_data_factory_linked_custom_service" "postgres" {
  name            = "ls_postgres_origination"
  data_factory_id = azurerm_data_factory.main.id
  type            = "PostgreSqlV2"

  type_properties_json = jsonencode({
    server   = data.terraform_remote_state.platform.outputs.postgres_fqdn
    port     = 5432
    database = var.postgres_database
    username = var.postgres_admin_username
    sslMode  = 1 # TODO verify this enum value against the PostgreSqlV2 linked
    # service reference on first apply (disable/allow/prefer/require/...)
    authenticationType = "Basic"
    password = {
      type  = "SecureString"
      value = var.postgres_admin_password
    }
  })
}

resource "azurerm_data_factory_linked_service_data_lake_storage_gen2" "landing" {
  name                 = "ls_adls_landing"
  data_factory_id      = azurerm_data_factory.main.id
  url                  = azurerm_storage_account.landing.primary_dfs_endpoint
  use_managed_identity = true
}
