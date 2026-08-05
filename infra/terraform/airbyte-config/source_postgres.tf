data "airbyte_connector_configuration" "postgres" {
  connector_name    = "source-postgres"
  connector_version = "3.8.4"

  configuration = {
    host               = data.terraform_remote_state.platform.outputs.postgres_fqdn
    port               = 5432
    database           = var.postgres_database
    schemas            = ["public"]
    ssl_mode           = { mode = "require" }
    replication_method = { method = "Xmin" }
    tunnel_method      = { tunnel_method = "NO_TUNNEL" }
  }

  configuration_secrets = {
    username = var.postgres_admin_username
    password = var.postgres_admin_password
  }
}

resource "airbyte_source" "postgres" {
  name          = "Credit Origination Postgres"
  workspace_id  = var.airbyte_workspace_id
  definition_id = data.airbyte_connector_configuration.postgres.definition_id
  configuration = data.airbyte_connector_configuration.postgres.configuration_json
}
